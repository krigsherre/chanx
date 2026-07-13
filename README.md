# chanx

**Every Go channel operation, one function call. Generics. Zero dependencies.**

[![Go Version](https://img.shields.io/badge/go-1.21%2B-blue.svg)](https://golang.org/doc/devel/release.html)

```bash
go get github.com/krigsherre/chanx
```

## Why chanx?

Raw Go channel operations are incredibly powerful, but implementing common patterns often requires repetitive, multi-line boilerplate. `chanx` abstracts these patterns into simple, highly intuitive interfaces via domain sub-packages (`opt`, `stream`, `io`), maintaining type safety through generics.

### How it works

```mermaid
flowchart LR
    A[stream.Of] -->|Stream| B[Filter]
    B -->|Stream| C[Map]
    C -->|Stream| D[Batch]
    D -->|[]T| E[Drain]
    
    style A fill:#4CAF50,stroke:#fff,stroke-width:2px,color:#fff
    style B fill:#2196F3,stroke:#fff,stroke-width:2px,color:#fff
    style C fill:#2196F3,stroke:#fff,stroke-width:2px,color:#fff
    style D fill:#9C27B0,stroke:#fff,stroke-width:2px,color:#fff
    style E fill:#F44336,stroke:#fff,stroke-width:2px,color:#fff
```

### Before & After

**Fluent Data Pipelines**
```go
// Before (raw Go)
// (Requires dozens of lines of goroutines and channel management)

// After (chanx Stream API)
results := stream.Of(1, 2, 3, 4, 5, 6).
    Filter(isEven).
    Skip(1).
    Take(2).
    Drain() // Output: [4, 6]
```

**Context-Aware Send**
```go
// Before (raw Go)
select {
case <-ctx.Done():
    return ctx.Err()
case ch <- val:
}

// After (chanx Options API)
_, err := io.Send(ch, val, opt.WithContext(ctx))
```

## Quick Start

```go
package main

import (
	"fmt"
	
	"github.com/krigsherre/chanx/opt"
	"github.com/krigsherre/chanx/stream"
)

func main() {
	// Fluent Stream Processing
	pipeline := stream.Of(1, 2, 3, 4, 5).
		Filter(func(i int) bool { return i > 2 }).
		Buffer(10)

	// Since Map changes types, it wraps the stream
	doubled := stream.Map(pipeline, func(i int) int { return i * 2 })

	// Easily collect results
	results := doubled.Drain()
	fmt.Println(results) // [6, 8, 10]
}
```

## API Reference

The library is split into three main sub-packages:

### `github.com/krigsherre/chanx/opt`
All core channel I/O operations accept optional configurations:
* `opt.WithContext(ctx)`: Attach cancellation to an operation.
* `opt.NonBlocking()`: Operation returns immediately if unable to proceed.
* `opt.WithTimeout(d)`: Define a timeout interval (e.g. for `Batch`).

### `github.com/krigsherre/chanx/stream`
The `Stream[T]` type is a drop-in replacement for `<-chan T` that supports method chaining.
* `Filter(fn func(T) bool)`: Keeps items matching a predicate.
* `Take(n int)`: Takes only the first `n` items.
* `Skip(n int)`: Skips the first `n` items.
* `Buffer(size int)`: Buffers the stream.
* `Throttle(interval time.Duration)`: Limits emission to one item per `interval`.
* `Debounce(interval time.Duration, opts ...Option)`: Emits an item only after `interval` silence.
* `OrDone(done <-chan struct{})`: Aborts the stream when `done` closes.
* `Drain() []T`: Collects all items into a slice.
* `ForEach(fn func(T) error, opts ...Option) error`: Synchronously iterates over the stream.

**Creation:**
* `stream.Of(values ...T) Stream[T]`: Creates a stream from specific values.
* `stream.FromSlice(s []T) Stream[T]`: Creates a stream from a slice.
* `stream.Generate(fn func(yield func(T) bool)) Stream[T]`: Yields values via a generator function.
* `stream.Range(start, end, step int) Stream[int]`: Emits a range of integers.
* `stream.Repeat(values ...T) Stream[T]`: Emits values in a continuous loop.

**Transformations & Fan Patterns:**
* `stream.Map(ch <-chan T, fn func(T) U) Stream[U]`: Transforms items one-by-one.
* `stream.Batch(ch <-chan T, size int, opts ...Option) Stream[[]T]`: Groups items into batches.
* `stream.FanOut(ch <-chan T, n int) []Stream[T]`: Distributes values round-robin to `n` channels.
* `stream.Merge(chs ...<-chan T) Stream[T]`: Merges multiple channels into one.

### `github.com/krigsherre/chanx/io`
* `io.Send(ch chan<- T, val T, opts ...Option) (bool, error)`: Unified send operation.
* `io.Receive(ch <-chan T, opts ...Option) (T, bool, error)`: Unified receive operation.
* `io.RecvOr(ch <-chan T, fallback T) T`: Non-blocking receive with a fallback value.
* `io.NewChan(size int) *Chan[T]`: Creates a new safe channel wrapper that prevents double-close panics.

## License
MIT
