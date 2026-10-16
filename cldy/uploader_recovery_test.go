package cldy_test

// The startup-recovery specs on the production scratch/<clusterID>/<sample> layout (F-01, F-05,
// F-36). They build the uploader without storage services, so they make no network calls.

import (
	"os"
	"time"

	"github.com/ibm/finops-agent/cldy"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

var _ = Describe("Uploader startup recovery (production layout)", func() {
	const clusterID = "123456-1234-1234-123456789012"
	var tempDir string
	var scratch *prodScratch
	BeforeEach(func() {
		var err error
		tempDir, err = os.MkdirTemp("", "")
		Expect(err).ToNot(HaveOccurred())
		scratch = newProdScratch(GinkgoT(), tempDir, clusterID)
	})
	AfterEach(func() {
		Expect(os.RemoveAll(tempDir)).To(Succeed())
	})

	It("should recover complete sample", func() {
		config := defaultConfig(tempDir)
		config.RecoveryPeriod = 100000 * time.Hour
		// write data before creating uploader simulating recovery state
		scratch.AddCompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0)
		actualUploader := cldy.NewUploaderForTest(config, nil, nil)
		var uploader cldy.Uploader = actualUploader
		uploader.SetClusterID(clusterID)
		Expect(actualUploader.RecoveredSamples).To(Equal(1), "F-01: complete sample not recovered on the production layout")
		Expect(actualUploader.RecoveredUploads).To(Equal(1), "F-01: recovered sample not queued for upload")
		checkScratchEmpty(scratch)

		// write another sample and ensure recovery does not break happy path
		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
	It("should drop, not upload, a sample outside the recovery range", func() {
		config := defaultConfig(tempDir)
		// 1 hour (will not recover as agent-measurement timestamp is old)
		config.RecoveryPeriod = 1 * time.Hour

		scratch.AddCompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0)
		actualUploader := cldy.NewUploaderForTest(config, nil, nil)
		var uploader cldy.Uploader = actualUploader
		uploader.SetClusterID(clusterID)
		Expect(actualUploader.RecoveredSamples).To(Equal(0))
		Expect(actualUploader.RecoveredUploads).To(Equal(0))
		Expect(actualUploader.EventsForTest().Dropped).To(Equal(map[string]int{cldy.DropReasonRecoveryExpired: 1}),
			"F-36: an expired sample is a counted drop")
		checkScratchEmpty(scratch)

		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
	It("should quarantine, not recover, an incomplete sample", func() {
		config := defaultConfig(tempDir)
		// 100 years (should recover all samples if complete)
		config.RecoveryPeriod = 1000000 * time.Hour

		scratch.AddIncompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0, "deployments.jsonl")
		actualUploader := cldy.NewUploaderForTest(config, nil, nil)
		var uploader cldy.Uploader = actualUploader
		uploader.SetClusterID(clusterID)
		Expect(actualUploader.RecoveredSamples).To(Equal(0))
		Expect(actualUploader.RecoveredUploads).To(Equal(0))
		Expect(actualUploader.EventsForTest().Dropped).To(Equal(map[string]int{cldy.DropReasonInvalidSample: 1}),
			"a sample without a valid manifest is quarantined and counted")
		Expect(scratch.Quarantined(GinkgoT())).To(HaveLen(1))
		checkScratchEmpty(scratch)

		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
	It("should recover multiple complete samples and ignore 1 incomplete sample", func() {
		config := defaultConfig(tempDir)
		// 100 years (should recover all samples)
		config.RecoveryPeriod = 1000000 * time.Hour

		scratch.AddCompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0)
		// still valid test data, just with only 1 node file
		scratch.AddFinalisedSampleWithout(GinkgoT(), time.Unix(1743499000, 0), 1,
			"stats-summary-nodename2.json", "stats-summary-nodename3.json", "stats-summary-nodename4.json")
		// invalid data set
		scratch.AddIncompleteSample(GinkgoT(), time.Unix(1743499600, 0), 2, "deployments.jsonl")
		actualUploader := cldy.NewUploaderForTest(config, nil, nil)
		var uploader cldy.Uploader = actualUploader
		uploader.SetClusterID(clusterID)
		Expect(actualUploader.RecoveredSamples).To(Equal(2), "F-01: complete samples not recovered on the production layout")
		Expect(actualUploader.RecoveredUploads).To(Equal(2), "F-01: recovered samples not queued for upload")
		checkScratchEmpty(scratch)

		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
})

func checkCollectionAndConstruction(scratch *prodScratch, uploader cldy.Uploader, actualUploader *cldy.CldyUploader) {
	uploader.AddSample(scratch.AddCompleteSample(GinkgoT(), time.Now(), 10))

	path, err := actualUploader.ConstructPayload(time.Now())
	Expect(err).ToNot(HaveOccurred())
	fileInfo, err := os.Stat(path)
	Expect(err).ToNot(HaveOccurred())
	Expect(fileInfo.Size()).To(BeNumerically(">", 0))
}

// checkScratchEmpty asserts that recovery consumed (packaged, dropped or quarantined) every
// sample file.
func checkScratchEmpty(scratch *prodScratch) {
	Expect(scratch.ScratchFileCount()).To(Equal(0))
}
