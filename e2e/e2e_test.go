package e2e_test

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
)


var _ = Describe("E2E", func() {
	t := GinkgoT()
	kv := os.Getenv("KUBERNETES_VERSION")
	knownFiles["stats-summary-e2e-" + kv + "-control-plane.json"] = false

	var wd string

	BeforeEach(func() {
		wd = os.Getenv("WORKING_DIR")
		if wd == "" {
			t.Skip("Skipping outside of e2e tests")
		}
	})
	Context("Test sample", func() {
		It("has the correct list of files", func() {
			f, err := os.Open(wd + "/file_list.txt")
			if err != nil {
				t.Fatalf("failed to open file: %s", err)
			}
			defer func() {
				err = f.Close()
				if err != nil {
					t.Fatalf("failed to close file: %s", err)
				}
			}()

			scanner := bufio.NewScanner(f)
			for scanner.Scan() {
				fileName := scanner.Text()
				if _, ok := knownFiles[fileName]; ok {
					knownFiles[fileName] = true
				} else {
					t.Fatalf("Could not find value in sample: %s", fileName)
				}
			}

			if err := scanner.Err(); err != nil {
				t.Fatal(err)
			}

			for file, val := range knownFiles {
				if !val {
					t.Fatalf("Could not find file %s in sample", file)
				}
			}
		})

		It("has the correct node data", func() {
			f, err := os.Open(wd + "/nodes.jsonl")
			if err != nil {
				t.Fatalf("failed to open file: %s", err)
			}
			defer func() {
				err = f.Close()
				if err != nil {
					t.Fatalf("failed to close file: %s", err)
				}
			}()

			decoder := json.NewDecoder(f)
			for {
				var node corev1.Node
				err := decoder.Decode(&node)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Error decoding JSON: %s", err)
				}

				// Has name of node
				Expect(node.Name).To(ContainSubstring("e2e-" + kv + "-control-plane"))
				// Has fields removed according to ParseMetricData
				Expect(node.Finalizers).To(BeEmpty())
			}
		})

		It("has the correct namespace data", func() {
			f, err := os.Open(wd + "/namespaces.jsonl")
			if err != nil {
				t.Fatalf("failed to open file: %s", err)
			}
			defer func() {
				err = f.Close()
				if err != nil {
					t.Fatalf("failed to close file: %s", err)
				}
			}()

			var namespaceNames []string

			decoder := json.NewDecoder(f)
			for {
				var namespace corev1.Namespace
				err := decoder.Decode(&namespace)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Error decoding JSON: %s", err)
				}

				namespaceNames = append(namespaceNames, namespace.Name)
				// Has fields removed according to ParseMetricData
				Expect(namespace.Finalizers).To(BeEmpty())
			}

			// Has namespace for stress pod and agent deployment
			Expect(namespaceNames).To(ContainElement("stress"))
			Expect(namespaceNames).To(ContainElement("ibm-finops-agent"))
		})

		It("has the correct pod data", func() {
			f, err := os.Open(wd + "/pods.jsonl")
			if err != nil {
				t.Fatalf("failed to open file: %s", err)
			}
			defer func() {
				err = f.Close()
				if err != nil {
					t.Fatalf("failed to close file: %s", err)
				}
			}()

			var podNames []string

			decoder := json.NewDecoder(f)
			for {
				var pod corev1.Pod
				err := decoder.Decode(&pod)
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatalf("Error decoding JSON: %s", err)
				}

				podNames = append(podNames, pod.Name)
				// Has fields removed according to ParseMetricData
				Expect(pod.Finalizers).To(BeEmpty())

				fmt.Printf("%s", pod.Name)
			}

			// // Has namespace for stress pod and agent deployment
			Expect(podNames).To(ContainElement(ContainSubstring("stress")))
			Expect(podNames).To(ContainElement(ContainSubstring("unified-agent")))
		})
	})
})

var knownFiles = map[string]bool{
	"agent-measurement.json": false,
	"daemonsets.jsonl": false,
	"deployments.jsonl": false,
	"jobs.jsonl": false,
	"namespaces.jsonl": false,
	"nodes.jsonl": false,
	"persistentvolumeclaims.jsonl": false,
	"persistentvolumes.jsonl": false,
	"pods.jsonl": false,
	"replicasets.jsonl": false,
	"replicationcontrollers.jsonl": false,
	"services.jsonl": false,
	"statefulsets.jsonl": false,
}