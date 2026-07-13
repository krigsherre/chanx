package io

import (
	"github.com/krigsherre/chanx/opt"
)

// Receive receives a value from a channel.
// Use opt.WithContext for cancellation, or opt.NonBlocking to return immediately if empty.
// Returns (value, ok bool, err error).
// If the channel is closed, it returns (zero, false, opt.ErrClosed).
func Receive[T any](ch <-chan T, opts ...opt.Option) (T, bool, error) {
	o := opt.ApplyOpts(opts)
	var zero T

	if o.NonBlocking {
		select {
		case val, ok := <-ch:
			if !ok {
				return zero, false, opt.ErrClosed // consistent with blocking behavior when closed
			}
			return val, true, nil
		default:
			return zero, false, nil
		}
	}

	select {
	case <-o.Ctx.Done():
		return zero, false, o.Ctx.Err()
	case val, ok := <-ch:
		if !ok {
			return zero, false, opt.ErrClosed
		}
		return val, true, nil
	}
}

// RecvOr is a non-blocking receive. Returns the value if available, otherwise returns fallback.
func RecvOr[T any](ch <-chan T, fallback T) T {
	select {
	case val, ok := <-ch:
		if !ok {
			return fallback
		}
		return val
	default:
		return fallback
	}
}
