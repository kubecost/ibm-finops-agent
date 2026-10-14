package cldy_test

import (
	"os"
	"testing"

	"github.com/ibm/finops-agent/cldy"
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

		// The following escaped the previous implementation, which deleted every
		// literal ".." substring before cleaning. "...." collapses to ".." once the
		// inner ".." is removed, which then resolves as a traversal.
		"traversal via doubled dots": {
			elements: []string{
				"test/scratch",
				"....//....//etc/passwd",
			},
			want: "test/scratch/..../..../etc/passwd",
		},
		"traversal via dot-slash padding": {
			elements: []string{
				"test/scratch",
				"..././..././etc/shadow",
			},
			want: "test/scratch/.../.../etc/shadow",
		},
		"traversal escaping the base is clamped": {
			elements: []string{
				"test/scratch",
				"../../../../etc/passwd",
			},
			want: "test/scratch/etc/passwd",
		},

		// The previous implementation also mangled legitimate names containing
		// consecutive dots, silently writing samples to the wrong directory.
		"legitimate name with consecutive dots is preserved": {
			elements: []string{
				"test/scratch",
				"v1..2-cluster",
			},
			want: "test/scratch/v1..2-cluster",
		},
		"legitimate filename with consecutive dots is preserved": {
			elements: []string{
				"test/scratch",
				"my..cluster.tgz",
			},
			want: "test/scratch/my..cluster.tgz",
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			if got := cldy.SafePath(tt.elements...); got != tt.want {
				t.Errorf("SafePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

var _ = Describe("Util", func() {
	Context("IsAvailableDiskSpace", func() {
		It("should return false on disk exceedance", func() {
			Expect(cldy.IsAvailableDiskSpace(0xFFFFFFFFFFFFFFFF, "/")).To(BeFalse())
		})
		It("should return true when there is space", func() {
			Expect(cldy.IsAvailableDiskSpace(1, "/")).To(BeTrue())
		})
	})
})

func safeClose(closer func() error) {
	Expect(closer()).To(Not(HaveOccurred()))
}

func safeRemove(dir string) {
	Expect(os.RemoveAll(dir)).To(Not(HaveOccurred()))
}
