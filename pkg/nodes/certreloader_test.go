package nodes

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

// writeKeyPair generates a self-signed cert/key pair with the given common name and writes
// it to the provided cert/key paths.
func writeKeyPair(certPath, keyPath, commonName string) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	Expect(err).ToNot(HaveOccurred())

	template := x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: commonName},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
	}

	der, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	Expect(err).ToNot(HaveOccurred())

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	Expect(os.WriteFile(certPath, certPEM, 0o600)).To(Succeed())

	keyDER, err := x509.MarshalECPrivateKey(key)
	Expect(err).ToNot(HaveOccurred())
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
	Expect(os.WriteFile(keyPath, keyPEM, 0o600)).To(Succeed())
}

func leafCommonName(certPath, keyPath string) string {
	r := newClientCertReloader(certPath, keyPath)
	cert, err := r.GetClientCertificate(nil)
	Expect(err).ToNot(HaveOccurred())
	Expect(cert.Leaf).ToNot(BeNil())
	return cert.Leaf.Subject.CommonName
}

var _ = Describe("clientCertReloader", func() {
	var (
		dir      string
		certPath string
		keyPath  string
	)

	BeforeEach(func() {
		dir = GinkgoT().TempDir()
		certPath = filepath.Join(dir, "tls.crt")
		keyPath = filepath.Join(dir, "tls.key")
	})

	It("loads the certificate on first use", func() {
		writeKeyPair(certPath, keyPath, "first")
		Expect(leafCommonName(certPath, keyPath)).To(Equal("first"))
	})

	It("picks up a rotated certificate when the key file changes", func() {
		writeKeyPair(certPath, keyPath, "first")
		r := newClientCertReloader(certPath, keyPath)

		first, err := r.GetClientCertificate(nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(first.Leaf.Subject.CommonName).To(Equal("first"))

		// Rotate the pair on disk. Ensure the mod time advances so the reloader detects it.
		writeKeyPair(certPath, keyPath, "second")
		future := time.Now().Add(time.Second)
		Expect(os.Chtimes(keyPath, future, future)).To(Succeed())

		second, err := r.GetClientCertificate(nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(second.Leaf.Subject.CommonName).To(Equal("second"))
	})

	It("serves the last-good certificate when the key file disappears", func() {
		writeKeyPair(certPath, keyPath, "first")
		r := newClientCertReloader(certPath, keyPath)

		_, err := r.GetClientCertificate(nil)
		Expect(err).ToNot(HaveOccurred())

		Expect(os.Remove(keyPath)).To(Succeed())

		cert, err := r.GetClientCertificate(nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(cert.Leaf.Subject.CommonName).To(Equal("first"))
	})

	It("serves the last-good certificate when a rotated file is corrupt", func() {
		writeKeyPair(certPath, keyPath, "first")
		r := newClientCertReloader(certPath, keyPath)

		_, err := r.GetClientCertificate(nil)
		Expect(err).ToNot(HaveOccurred())

		// Simulate a partial write mid-rotation: key file present and newer, but unparseable.
		Expect(os.WriteFile(keyPath, []byte("not a valid key"), 0o600)).To(Succeed())
		future := time.Now().Add(time.Second)
		Expect(os.Chtimes(keyPath, future, future)).To(Succeed())

		cert, err := r.GetClientCertificate(nil)
		Expect(err).ToNot(HaveOccurred())
		Expect(cert.Leaf.Subject.CommonName).To(Equal("first"))
	})

	It("returns an error when the first load is corrupt", func() {
		Expect(os.WriteFile(certPath, []byte("not a cert"), 0o600)).To(Succeed())
		Expect(os.WriteFile(keyPath, []byte("not a key"), 0o600)).To(Succeed())

		r := newClientCertReloader(certPath, keyPath)
		_, err := r.GetClientCertificate(nil)
		Expect(err).To(HaveOccurred())
	})

	It("returns an error when no certificate has ever loaded", func() {
		r := newClientCertReloader(certPath, keyPath)
		_, err := r.GetClientCertificate(nil)
		Expect(err).To(HaveOccurred())
	})
})
