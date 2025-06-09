package cldy

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSafePath(t *testing.T) {

	tests := map[string]struct {
		elements []string
		want     string
	}{
		"empty": {
			elements: []string{
				"",
			},
			want: ".",
		},
		"single element clean": {
			elements: []string{
				"test/scratch/fileName.txt",
			},
			want: "test/scratch/fileName.txt",
		},
		"single element clean trailing slash": {
			elements: []string{
				"test/scratch/dirName/",
			},
			want: "test/scratch/dirName/",
		},
		"single element dirty": {
			elements: []string{
				"test/scratch/../fileName.txt",
			},
			want: "test/scratch/fileName.txt",
		},
		"single element dirty trailing slash": {
			elements: []string{
				"test/scratch/../dirName/",
			},
			want: "test/scratch/dirName/",
		},
		"multi-element clean": {
			elements: []string{
				"test",
				"scratch",
				"fileName.txt",
			},
			want: "test/scratch/fileName.txt",
		},
		"multi-element clean trailing slash": {
			elements: []string{
				"test/scratch/",
				"dirName/",
			},
			want: "test/scratch/dirName/",
		},
		"multi-element dirty": {
			elements: []string{
				"test/scratch/",
				"../fileName.txt",
			},
			want: "test/scratch/fileName.txt",
		},
		"multi-element dirty trailing slash": {
			elements: []string{
				"test/scratch/",
				"../dirName/",
			},
			want: "test/scratch/dirName/",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := SafePath(tt.elements...); got != tt.want {
				t.Errorf("SafePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

var _ = Describe("Util", func() {
	Context("IsAvailableDiskSpace", func() {
		It("should return false on disk exceedance", func() {
			Expect(IsAvailableDiskSpace(0xFFFFFFFFFFFFFFFF)).To(BeFalse())
		})
		It("should return true when there is space", func() {
			Expect(IsAvailableDiskSpace(1)).To(BeTrue())
		})
	})
})
