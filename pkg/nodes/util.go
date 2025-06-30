package nodes

import (
	//nolint: staticcheck
	. "github.com/onsi/gomega"
)

func safeClose(closer func() error, err *error) {
	if closeErr := closer(); closeErr != nil && *err == nil {
		*err = closeErr
	}
}

func safeCloseTest(closer func() error) {
	Expect(closer()).To(Not(HaveOccurred()))
}