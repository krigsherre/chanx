package stream

import (
	"time"

	"github.com/krigsherre/chanx/opt"
)

type Stream[T any] <-chan T

func (s Stream[T]) Filter(fn func(T) bool) Stream[T] {
	return Filter(s, fn)
}

func (s Stream[T]) Take(n int) Stream[T] {
	return Take(s, n)
}

func (s Stream[T]) Skip(n int) Stream[T] {
	return Skip(s, n)
}

func (s Stream[T]) Buffer(size int) Stream[T] {
	return Buffer(s, size)
}

func (s Stream[T]) Throttle(interval time.Duration) Stream[T] {
	return Throttle(s, interval)
}

func (s Stream[T]) Debounce(interval time.Duration, opts ...opt.Option) Stream[T] {
	return Debounce(s, interval, opts...)
}

func (s Stream[T]) Drain() []T {
	return Drain(s)
}

func (s Stream[T]) ForEach(fn func(T) error, opts ...opt.Option) error {
	return ForEach(s, fn, opts...)
}

func (s Stream[T]) OrDone(done <-chan struct{}) Stream[T] {
	return OrDone(done, s)
}

func (s Stream[T]) First() (T, error) {
	return First(s)
}

func (s Stream[T]) Last() (T, error) {
	return Last(s)
}
