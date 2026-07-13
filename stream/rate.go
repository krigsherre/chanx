package stream

import (
	"github.com/krigsherre/chanx/opt"
)

import "time"

// Buffer returns a stream backed by a buffer.
func Buffer[T any](ch <-chan T, size int) Stream[T] {
	if size <= 0 {
		panic("chanx: Buffer size must be > 0")
	}
	out := make(chan T, size)
	go func() {
		defer close(out)
		for v := range ch {
			out <- v
		}
	}()
	return out
}

// Batch groups items. If opt.WithTimeout is provided, it triggers partial batches on intervals.
func Batch[T any](ch <-chan T, size int, opts ...opt.Option) Stream[[]T] {
	if size <= 0 {
		panic("chanx: Batch size must be > 0")
	}
	o := opt.ApplyOpts(opts)
	out := make(chan []T, 8)

	go func() {
		defer close(out)
		var batch []T

		if o.Timeout <= 0 {
			for v := range ch {
				batch = append(batch, v)
				if len(batch) == size {
					out <- batch
					batch = nil
				}
			}
			if len(batch) > 0 {
				out <- batch
			}
			return
		}

		ticker := time.NewTicker(o.Timeout)
		defer ticker.Stop()

		for {
			select {
			case <-o.Ctx.Done():
				if len(batch) > 0 {
					out <- batch
				}
				return
			case <-ticker.C:
				if len(batch) > 0 {
					out <- batch
					batch = nil
				}
			case v, ok := <-ch:
				if !ok {
					if len(batch) > 0 {
						out <- batch
					}
					return
				}
				batch = append(batch, v)
				if len(batch) == size {
					out <- batch
					batch = nil
					ticker.Reset(o.Timeout)
				}
			}
		}
	}()

	return out
}

// Throttle limits emission to one per interval.
func Throttle[T any](ch <-chan T, interval time.Duration) Stream[T] {
	out := make(chan T, 16)
	go func() {
		defer close(out)
		for v := range ch {
			out <- v
			time.Sleep(interval)
		}
	}()
	return out
}

// Debounce delays emission until interval silence.
func Debounce[T any](ch <-chan T, interval time.Duration, opts ...opt.Option) Stream[T] {
	o := opt.ApplyOpts(opts)
	out := make(chan T, 16)
	go func() {
		defer close(out)
		var timer *time.Timer
		var lastVal T
		var hasVal bool

		for {
			var timerCh <-chan time.Time
			if timer != nil {
				timerCh = timer.C
			}

			select {
			case <-o.Ctx.Done():
				if hasVal {
					out <- lastVal
				}
				return
			case <-timerCh:
				out <- lastVal
				hasVal = false
				timer.Stop()
				timer = nil
			case v, ok := <-ch:
				if !ok {
					if hasVal {
						out <- lastVal
					}
					return
				}
				lastVal = v
				hasVal = true
				if timer == nil {
					timer = time.NewTimer(interval)
				} else {
					timer.Reset(interval)
				}
			}
		}
	}()
	return out
}
