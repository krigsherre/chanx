package io

import (
	"github.com/krigsherre/chanx/opt"
)

// Send sends a value on a channel.
// Use opt.WithContext for cancellation, or opt.NonBlocking to return immediately if full.
// Returns (sent bool, err error).
// If ch is nil, it panics.
func Send[T any](ch chan<- T, val T, opts ...opt.Option) (bool, error) {
	if ch == nil {
		panic("chanx: send on nil channel")
	}
	o := opt.ApplyOpts(opts)

	if o.NonBlocking {
		// Non-blocking can also panic if closed, so we catch it
		var ok bool
		func() {
			defer func() {
				if r := recover(); r != nil {
					ok = false
				}
			}()
			select {
			case ch <- val:
				ok = true
			default:
				ok = false
			}
		}()
		return ok, nil
	}

	select {
	case <-o.Ctx.Done():
		return false, o.Ctx.Err()
	case ch <- val:
		return true, nil
	}
}
