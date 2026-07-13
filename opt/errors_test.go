package opt

import (
	"errors"
	"testing"
)

func TestErrClosed(t *testing.T) {
	if ErrClosed.Error() != "chanx: channel closed" {
		t.Errorf("unexpected error message: %v", ErrClosed.Error())
	}
	if !errors.Is(ErrClosed, ErrClosed) {
		t.Error("errors.Is failed for ErrClosed")
	}
}
