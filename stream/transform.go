package stream

// Map returns a Stream that receives fn(value) for each value from ch.
func Map[T, U any](ch <-chan T, fn func(T) U) Stream[U] {
	out := make(chan U, 16)
	go func() {
		defer close(out)
		for v := range ch {
			out <- fn(v)
		}
	}()
	return out
}

// Filter returns a Stream containing only elements matching fn.
func Filter[T any](ch <-chan T, fn func(T) bool) Stream[T] {
	out := make(chan T, 16)
	go func() {
		defer close(out)
		for v := range ch {
			if fn(v) {
				out <- v
			}
		}
	}()
	return out
}

// FlatMap maps over elements and merges the resulting streams.
func FlatMap[T, U any](ch <-chan T, fn func(T) <-chan U) Stream[U] {
	out := make(chan U, 16)
	go func() {
		defer close(out)
		for v := range ch {
			inner := fn(v)
			for iv := range inner {
				out <- iv
			}
		}
	}()
	return out
}

// Reduce reduces a stream to a single value. It blocks until completion.
func Reduce[T, U any](ch <-chan T, init U, fn func(acc U, val T) U) U {
	acc := init
	for v := range ch {
		acc = fn(acc, v)
	}
	return acc
}
