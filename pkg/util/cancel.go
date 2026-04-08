package util

import (
	"context"
	"sync"
)

// CancelToken is a thread-safe wrapper around a stateful cancellable context. This is helpful when context for
// a specific execution may run in the background and a cancellation func's lifetime must extend beyond the parent
// function scope.
//
// This type should be created once on a parent struct, and then `NewContext` should be used each time a new
// context is required.
type CancelToken struct {
	lock       sync.Mutex
	ctx        context.Context
	cancelFunc context.CancelFunc
}

// NewCancelToken creates a new instance of a cancellation token. This token can create a `context.Context`
// which interfaces more appropriately with conventional go APIs and can be cancelled by calling the returned
// instance's `Cancel()`
func NewCancelToken() *CancelToken {
	return new(CancelToken)
}

// NewContext will update the internal state of the token based on the provided parent context, and return
// a new context that can be used. If a previous context exists and was _not_ cancelled, then this function
// will cancel the previous context before creating the new context.
func (ct *CancelToken) NewContext(parentCtx context.Context) context.Context {
	ct.lock.Lock()
	defer ct.lock.Unlock()

	// in the event we already have a context, cancel it
	if ct.cancelFunc != nil {
		ct.cancelFunc()
	}

	// set internal context and cancelation func
	ct.ctx, ct.cancelFunc = context.WithCancel(parentCtx)
	return ct.ctx
}

// Cancel will execute the cancellation on the current context. This function can safely
// be called multiple times for a single context without issues, but only the first execution
// will actually cancel the context.
func (ct *CancelToken) Cancel() {
	ct.lock.Lock()
	defer ct.lock.Unlock()

	if ct.cancelFunc != nil {
		ct.cancelFunc()
	}

	ct.cancelFunc = nil
}
