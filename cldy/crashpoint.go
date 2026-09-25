package cldy

// Named crash points in the payload lifecycle. Tests use them to model a process kill at that
// point: see testHook.
const (
	// crashAfterCreate is reached after the temporary payload file (upload/.<name>.partial) is
	// created and before anything is written.
	crashAfterCreate = "after-create"
	// crashMidTar is reached after each tar header is written, before that entry's content.
	crashMidTar = "mid-tar"
	// crashBeforeRename is reached once the temporary payload is complete and fsynced, before it
	// is renamed to its final name.
	crashBeforeRename = "before-rename"
	// crashAfterRename is reached after the payload is renamed to its final name, before the
	// samples it holds are removed.
	crashAfterRename = "after-rename"
	// crashAfterUpload is reached after every storage service accepted a payload, before the
	// payload file is deleted.
	crashAfterUpload = "after-upload"

	// crashSampleFileWritten is reached after each file of a sample is written into its staging
	// directory, the manifest included.
	crashSampleFileWritten = "sample-file-written"
	// crashSampleBeforeRename is reached once a staging directory holds its fsynced manifest,
	// before it is renamed to its finalised name.
	crashSampleBeforeRename = "sample-before-rename"
	// crashSampleAfterRename is reached after the rename, before the parent directory is fsynced.
	// An error returned here is ignored: the sample is already finalised.
	crashSampleAfterRename = "sample-after-rename"
)

// testHook is nil in production. Tests set it to observe or interrupt the named crash points.
// A non-nil error aborts the operation at that point and is returned to its caller. To model a
// kill -9 exactly, a test captures whatever on-disk state it needs inside the hook, because
// deferred cleanup still runs after the hook returns.
var testHook func(point string) error

func crashPoint(point string) error {
	if testHook == nil {
		return nil
	}
	return testHook(point)
}
