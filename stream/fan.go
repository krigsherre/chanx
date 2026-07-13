package stream

import "sync"

// FanOut distributes items round-robin to n output streams.
func FanOut[T any](ch <-chan T, n int) []Stream[T] {
	if n <= 0 {
		panic("chanx: FanOut n must be > 0")
	}
	outs := make([]Stream[T], n)
	chans := make([]chan T, n)
	for i := 0; i < n; i++ {
		chans[i] = make(chan T, 16)
		outs[i] = chans[i]
	}

	go func() {
		for i := 0; i < n; i++ {
			defer close(chans[i])
		}
		i := 0
		for v := range ch {
			chans[i] <- v
			i = (i + 1) % n
		}
	}()

	return outs
}

// FanIn merges multiple streams into one.
func FanIn[T any](chs ...<-chan T) Stream[T] {
	out := make(chan T, 16)
	var wg sync.WaitGroup
	wg.Add(len(chs))

	for _, c := range chs {
		go func(in <-chan T) {
			defer wg.Done()
			for v := range in {
				out <- v
			}
		}(c)
	}

	go func() {
		wg.Wait()
		close(out)
	}()

	return out
}

// Merge is an alias for FanIn.
func Merge[T any](chs ...<-chan T) Stream[T] {
	return FanIn(chs...)
}

// Tee duplicates a stream into two streams.
func Tee[T any](ch <-chan T) (Stream[T], Stream[T]) {
	res := TeeN(ch, 2)
	return res[0], res[1]
}

// TeeN duplicates a stream into n streams.
func TeeN[T any](ch <-chan T, n int) []Stream[T] {
	if n <= 0 {
		panic("chanx: TeeN n must be > 0")
	}
	if n == 1 {
		return []Stream[T]{ch}
	}
	outs := make([]Stream[T], n)
	chans := make([]chan T, n)
	for i := 0; i < n; i++ {
		chans[i] = make(chan T, 16)
		outs[i] = chans[i]
	}

	go func() {
		for i := 0; i < n; i++ {
			defer close(chans[i])
		}
		for v := range ch {
			for i := 0; i < n; i++ {
				chans[i] <- v
			}
		}
	}()

	return outs
}

// Bridge flattens a stream of streams.
func Bridge[T any](ch Stream[Stream[T]]) Stream[T] {
	out := make(chan T, 16)
	go func() {
		defer close(out)
		for inner := range ch {
			for v := range inner {
				out <- v
			}
		}
	}()
	return out
}
