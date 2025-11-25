package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/opencost/opencost/core/pkg/model/kubemodel"
)

func kubeModelBinaryToJson(bytes []byte) ([]byte, error) {
	kms := new(kubemodel.KubeModelSet)
	err := kms.UnmarshalBinary(bytes)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling binary to KubeModelSet: %w", err)
	}

	setJson, err := json.Marshal(kms)
	if err != nil {
		return nil, fmt.Errorf("error marshalling KubeModelSet to JSON: %w", err)
	}

	return setJson, nil
}

func decodeKubeModel(srcPath, outPath string) {
	filepath.WalkDir(srcPath, func(path string, d os.DirEntry, err error) error {
		kind := "file"
		if d.IsDir() {
			kind = "dir"
		}

		fmt.Printf("[kubemodel] walking %s (%s)\n", path, kind)

		if err != nil {
			fmt.Printf("[kubemodel] error: %s", err)
			return nil
		}

		if d.IsDir() {
			os.MkdirAll(filepath.Join(outPath, path), 0755)
			return nil
		}

		fileName, _ := filepath.Rel(srcPath, path)

		bytes, err := os.ReadFile(path)
		if err != nil {
			fmt.Printf("[kubemodel] error reading file %s: %v\n", path, err)
			return nil
		}

		jsonData, err := kubeModelBinaryToJson(bytes)
		if err != nil {
			fmt.Printf("[kubemodel] error converting binary to JSON for file %s: %v\n", path, err)
			return nil
		}

		outFilePath := filepath.Join(outPath, srcPath, fileName+".json")
		err = os.WriteFile(outFilePath, jsonData, 0644)
		if err != nil {
			fmt.Printf("[kubemodel] error writing JSON file %s: %v\n", outFilePath, err)
			return nil
		}

		return nil
	})
}

func main() {
	decodeKubeModel("data/kubemodel", "data/json")
}
