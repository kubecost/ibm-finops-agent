//go:build reliability_repro

package cldy_test

// The startup-recovery specs, converted from the flat scratch/<sample> layout (which production
// never writes) to the production scratch/<clusterID>/<sample> layout. On that layout they fail
// because of F-01 (and F-05/F-36 behind it), so they stay behind the reliability_repro tag until
// chunk 01 fixes recovery.

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
		stopCh := make(chan struct{})
		defer close(stopCh)
		uploader := cldy.NewCldyUploader(config, stopCh)
		uploader.SetClusterID(clusterID)
		actualUploader := uploader.(*cldy.CldyUploader)
		Expect(actualUploader.RecoveredSamples).To(Equal(1), "F-01: complete sample not recovered on the production layout")
		Expect(actualUploader.RecoveredUploads).To(Equal(1), "F-01: recovered sample not queued for upload")
		checkScratchEmpty(scratch)

		// write another sample and ensure recovery does not break happy path
		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
	It("should recover sample but not upload when outside recovery range", func() {
		config := defaultConfig(tempDir)
		// 1 hour (will not recover as agent-measurement timestamp is old)
		config.RecoveryPeriod = 1 * time.Hour

		scratch.AddCompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0)
		stopCh := make(chan struct{})
		defer close(stopCh)
		uploader := cldy.NewCldyUploader(config, stopCh)
		uploader.SetClusterID(clusterID)
		actualUploader := uploader.(*cldy.CldyUploader)
		Expect(actualUploader.RecoveredSamples).To(Equal(1), "F-01: complete sample not recovered on the production layout")
		Expect(actualUploader.RecoveredUploads).To(Equal(0))
		checkScratchEmpty(scratch)

		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
	It("should not recover incomplete sample", func() {
		config := defaultConfig(tempDir)
		// 100 years (should recover all samples if complete)
		config.RecoveryPeriod = 1000000 * time.Hour

		scratch.AddIncompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0, "deployments.jsonl")
		stopCh := make(chan struct{})
		defer close(stopCh)
		uploader := cldy.NewCldyUploader(config, stopCh)
		uploader.SetClusterID(clusterID)
		actualUploader := uploader.(*cldy.CldyUploader)
		Expect(actualUploader.RecoveredSamples).To(Equal(0))
		Expect(actualUploader.RecoveredUploads).To(Equal(0))
		checkScratchEmpty(scratch)

		checkCollectionAndConstruction(scratch, uploader, actualUploader)
	})
	It("should recover multiple complete samples and ignore 1 incomplete sample", func() {
		config := defaultConfig(tempDir)
		// 100 years (should recover all samples)
		config.RecoveryPeriod = 1000000 * time.Hour

		scratch.AddCompleteSample(GinkgoT(), time.Unix(1743465782, 0), 0)
		// still valid test data, just with only 1 node file
		scratch.AddIncompleteSample(GinkgoT(), time.Unix(1743499000, 0), 1,
			"stats-summary-nodename2.json", "stats-summary-nodename3.json", "stats-summary-nodename4.json")
		// invalid data set
		scratch.AddIncompleteSample(GinkgoT(), time.Unix(1743499600, 0), 2, "deployments.jsonl")
		stopCh := make(chan struct{})
		defer close(stopCh)
		uploader := cldy.NewCldyUploader(config, stopCh)
		uploader.SetClusterID(clusterID)
		actualUploader := uploader.(*cldy.CldyUploader)
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

// checkScratchEmpty asserts that recovery consumed (packaged or discarded) every sample file.
func checkScratchEmpty(scratch *prodScratch) {
	Expect(scratch.ScratchFileCount()).To(Equal(0))
}
