package stream

func Of[T any](values ...T) Stream[T] {
	ch := make(chan T, len(values))
	for _, v := range values {
		ch <- v
	}
	close(ch)
	return ch
}

func FromSlice[T any](s []T) Stream[T] {
	ch := make(chan T, len(s))
	for _, v := range s {
		ch <- v
	}
	close(ch)
	return ch
}

func Generate[T any](fn func(yield func(T) bool)) Stream[T] {
	ch := make(chan T, 16)
	go func() {
		defer close(ch)
		yield := func(v T) (ok bool) {
			defer func() {
				if r := recover(); r != nil {
					ok = false
				}
			}()
			ch <- v
			return true
		}
		fn(yield)
	}()
	return ch
}

func Range(start, end, step int) Stream[int] {
	if step <= 0 {
		panic("chanx: step must be positive")
	}
	ch := make(chan int)
	if start >= end {
		close(ch)
		return ch
	}
	go func() {
		defer close(ch)
		for i := start; i < end; i += step {
			ch <- i
		}
	}()
	return ch
}

func Repeat[T any](values ...T) Stream[T] {
	ch := make(chan T, 16)
	if len(values) == 0 {
		close(ch)
		return ch
	}
	go func() {
		defer close(ch)
		for {
			for _, v := range values {
				ch <- v
			}
		}
	}()
	return ch
}
