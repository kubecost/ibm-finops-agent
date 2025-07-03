package nodes

func safeClose(closer func() error, err *error) {
	if closeErr := closer(); closeErr != nil && *err == nil {
		*err = closeErr
	}
}