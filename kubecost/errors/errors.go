package errors

import "fmt"

func MissingExportBucketConfigFileErr() error {
	return fmt.Errorf("configuration missing valid ExportBucketConfigFile")
}

func BucketConfigFileLoadErr(err error) error {
	return fmt.Errorf("failed to load bucket configuration file: %w", err)
}

func BucketStorageCreationErr(err error) error {
	return fmt.Errorf("failed to create bucket storage: %w", err)
}

func BucketWriteErr(err error) error {
	return fmt.Errorf("failed to write data to bucket storage: %w", err)
}

func BucketReadErr(err error) error {
	return fmt.Errorf("failed to read data from bucket storage: %w", err)
}

func BucketDeleteErr(err error) error {
	return fmt.Errorf("failed to delete data from bucket storage: %w", err)
}
