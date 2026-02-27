package util

import (
	"context"
	"testing"
)

func TestNewCancelToken(t *testing.T) {
	ct := NewCancelToken()
	if ct == nil {
		t.Fatal("NewCancelToken() returned nil")
	}
}

func TestCancelToken_NewContext(t *testing.T) {
	parent := context.Background()
	ct := NewCancelToken()

	ctx := ct.NewContext(parent)
	if ctx == nil {
		t.Fatal("NewContext() returned nil")
	}

	select {
	case <-ctx.Done():
		t.Error("new context should not be done")
	default:
		// expected
	}
}

func TestCancelToken_NewContext_Cancel(t *testing.T) {
	parent := context.Background()
	ct := NewCancelToken()

	ctx := ct.NewContext(parent)
	ct.Cancel()

	select {
	case <-ctx.Done():
		// expected - context should be cancelled
	default:
		t.Error("context should be done after Cancel()")
	}
}

func TestCancelToken_NewContext_ReplacesPrevious(t *testing.T) {
	parent := context.Background()
	ct := NewCancelToken()

	ctx1 := ct.NewContext(parent)
	ctx2 := ct.NewContext(parent)

	// First context should be cancelled when second is created
	select {
	case <-ctx1.Done():
		// expected
	default:
		t.Error("first context should be cancelled when NewContext is called again")
	}

	// Second context should still be active
	select {
	case <-ctx2.Done():
		t.Error("second context should not be done yet")
	default:
		// expected
	}

	ct.Cancel()

	// Now second context should also be cancelled
	select {
	case <-ctx2.Done():
		// expected
	default:
		t.Error("second context should be done after Cancel()")
	}
}

func TestCancelToken_Cancel_Idempotent(t *testing.T) {
	parent := context.Background()
	ct := NewCancelToken()
	ctx := ct.NewContext(parent)

	ct.Cancel()
	ct.Cancel()
	ct.Cancel()

	select {
	case <-ctx.Done():
		// expected
	default:
		t.Error("context should be done after Cancel()")
	}
}

func TestCancelToken_NewContext_WithCancelledParent(t *testing.T) {
	parent, cancelParent := context.WithCancel(context.Background())
	cancelParent()

	ct := NewCancelToken()
	ctx := ct.NewContext(parent)

	select {
	case <-ctx.Done():
		// expected - child inherits parent cancellation
	default:
		t.Error("context should be done when parent is cancelled")
	}
}
