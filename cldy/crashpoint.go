package cldy

// Named crash points in the payload lifecycle. Tests use them to model a process kill at that
// point: see testHook.
const (
	// crashAfterCreate is reached after the payload file is created and before anything is written.
	crashAfterCreate = "after-create"
	// crashMidTar is reached after each tar header is written, before that entry's content.
	crashMidTar = "mid-tar"
	// crashBeforeRename is reached once the payload is complete, before the samples are removed.
	// There is no rename today; this is where a temp-file rename would go.
	crashBeforeRename = "before-rename"
	// crashAfterUpload is reached after every storage service accepted a payload, before the
	// payload file is deleted.
	crashAfterUpload = "after-upload"
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
