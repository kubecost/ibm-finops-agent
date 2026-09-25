package cldy_test

import (
	"os"
	"testing"

	"github.com/ibm/finops-agent/cldy"
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
			if got := cldy.SafePath(tt.elements...); got != tt.want {
				t.Errorf("SafePath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func safeClose(closer func() error) {
	Expect(closer()).To(Not(HaveOccurred()))
}

func safeRemove(dir string) {
	Expect(os.RemoveAll(dir)).To(Not(HaveOccurred()))
}
