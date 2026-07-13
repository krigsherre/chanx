package stream

import (
	"github.com/krigsherre/chanx/opt"
)

// OrDone aborts reading when done closes.
func OrDone[T any](done <-chan struct{}, ch <-chan T) Stream[T] {
	out := make(chan T)
	go func() {
		defer close(out)
		for {
			select {
			case <-done:
				return
			case v, ok := <-ch:
				if !ok {
					return
				}
				select {
				case out <- v:
				case <-done:
					return
				}
			}
		}
	}()
	return out
}

// ForEach iterates synchronously over a channel.
func ForEach[T any](ch <-chan T, fn func(T) error, opts ...opt.Option) error {
	o := opt.ApplyOpts(opts)
	for {
		select {
		case <-o.Ctx.Done():
			return o.Ctx.Err()
		case v, ok := <-ch:
			if !ok {
				return nil
			}
			if err := fn(v); err != nil {
				return err
			}
		}
	}
}

// Drain blocks and collects all items into a slice.
func Drain[T any](ch <-chan T) []T {
	var res []T
	for v := range ch {
		res = append(res, v)
	}
	return res
}

// First reads the first element and returns.
func First[T any](ch <-chan T) (T, error) {
	var zero T
	v, ok := <-ch
	if !ok {
		return zero, opt.ErrClosed
	}
	return v, nil
}

// Last blocks until closed and returns the final element.
func Last[T any](ch <-chan T) (T, error) {
	var last T
	var found bool
	for v := range ch {
		last = v
		found = true
	}
	if !found {
		return last, opt.ErrClosed
	}
	return last, nil
}

// Take limits the stream to n items.
func Take[T any](ch <-chan T, n int) Stream[T] {
	if n < 0 {
		n = 0
	}
	out := make(chan T, 16)
	go func() {
		defer close(out)
		count := 0
		for v := range ch {
			if count >= n {
				break
			}
			out <- v
			count++
		}
	}()
	return out
}

// Skip drops the first n items.
func Skip[T any](ch <-chan T, n int) Stream[T] {
	if n < 0 {
		n = 0
	}
	out := make(chan T, 16)
	go func() {
		defer close(out)
		count := 0
		for v := range ch {
			if count < n {
				count++
				continue
			}
			out <- v
		}
	}()
	return out
}
