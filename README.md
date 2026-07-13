<div align="center">

<img src="https://raw.githubusercontent.com/krigsherre/chanx/master/assets/logo.svg" alt="chanx" width="100" />

# chanx

**Every Go channel operation, one function call.**

*Generics · Zero Dependencies · Fluent API*

<br/>

[![Go Version](https://img.shields.io/badge/Go-1.21%2B-00ADD8?style=for-the-badge&logo=go&logoColor=white)](https://golang.org/doc/devel/release.html)
[![Go Reference](https://img.shields.io/badge/pkg.go.dev-reference-007D9C?style=for-the-badge&logo=go&logoColor=white)](https://pkg.go.dev/github.com/krigsherre/chanx)
[![License: MIT](https://img.shields.io/badge/License-MIT-purple?style=for-the-badge)](LICENSE)

```bash
go get github.com/krigsherre/chanx
```

</div>

---

## Why chanx?

Raw Go channels are powerful — but every common pattern requires dozens of lines of goroutine plumbing. `chanx` wraps those patterns into a single, expressive function call, with full type safety via generics.

```
┌─────────────────────────────────────────────────────────────────┐
│                     chanx pipeline model                        │
│                                                                 │
│  stream.Of  ──▶  Filter  ──▶  Map  ──▶  Batch  ──▶  Drain      │
│  [Source]       [Keep]     [Transform] [Group]    [Collect]     │
│                                                                 │
│  Every stage is a typed channel. No goroutine wiring needed.    │
└─────────────────────────────────────────────────────────────────┘
```

---

## Before & After

<table>
<tr>
<th>❌ Raw Go</th>
<th>✅ chanx</th>
</tr>
<tr>
<td>

```go
// Context-aware send
select {
case <-ctx.Done():
    return ctx.Err()
case ch <- val:
}
```

</td>
<td>

```go
// One line
_, err := io.Send(ch, val,
    opt.WithContext(ctx))
```

</td>
</tr>
<tr>
<td>

```go
// Filtered pipeline
// ~30 lines of goroutines,
// channels, and WaitGroups...
```

</td>
<td>

```go
// Fluent & readable
results := stream.Of(1,2,3,4,5,6).
    Filter(isEven).
    Skip(1).
    Take(2).
    Drain() // → [4, 6]
```

</td>
</tr>
</table>

---

## Quick Start

```go
package main

import (
    "fmt"

    "github.com/krigsherre/chanx/stream"
)

func main() {
    // Build a typed pipeline — no goroutine wiring
    pipeline := stream.Of(1, 2, 3, 4, 5).
        Filter(func(i int) bool { return i > 2 }).
        Buffer(10)

    // Map can change the element type
    doubled := stream.Map(pipeline, func(i int) int { return i * 2 })

    fmt.Println(doubled.Drain()) // [6 8 10]
}
```

---

## API Reference

The library is split into three focused sub-packages:

### `opt` — Operation Options

Configure any channel operation with composable options:

| Option | Description |
|---|---|
| `opt.WithContext(ctx)` | Attach context cancellation |
| `opt.NonBlocking()` | Return immediately if unable to proceed |
| `opt.WithTimeout(d)` | Set a timeout duration |

---

### `stream` — Fluent Stream Processing

`Stream[T]` is a typed `<-chan T` with method chaining.

**Creation**

| Function | Description |
|---|---|
| `stream.Of(values ...T)` | Create a stream from values |
| `stream.FromSlice(s []T)` | Create a stream from a slice |
| `stream.Generate(fn)` | Yield values from a generator |
| `stream.Range(start, end, step)` | Emit a range of integers |
| `stream.Repeat(values ...T)` | Emit values in a continuous loop |

**Methods**

| Method | Description |
|---|---|
| `.Filter(fn func(T) bool)` | Keep items matching a predicate |
| `.Take(n int)` | Keep only the first `n` items |
| `.Skip(n int)` | Skip the first `n` items |
| `.Buffer(size int)` | Buffer the stream |
| `.Throttle(d time.Duration)` | Emit at most once per interval |
| `.Debounce(d time.Duration)` | Emit only after `d` silence |
| `.OrDone(done <-chan struct{})` | Abort stream when `done` closes |
| `.Drain() []T` | Collect all items into a slice |
| `.ForEach(fn func(T) error)` | Iterate synchronously |

**Transformations & Fan Patterns**

| Function | Description |
|---|---|
| `stream.Map(ch, fn)` | Transform items one-by-one (can change type) |
| `stream.Batch(ch, size)` | Group items into `[]T` batches |
| `stream.FanOut(ch, n)` | Distribute values round-robin to `n` streams |
| `stream.Merge(chs ...)` | Merge multiple channels into one stream |

---

### `io` — Unified Channel I/O

| Function | Description |
|---|---|
| `io.Send(ch, val, opts...)` | Send with optional context / timeout / non-blocking |
| `io.Receive(ch, opts...)` | Receive with optional context / timeout / non-blocking |
| `io.RecvOr(ch, fallback)` | Non-blocking receive with a fallback value |
| `io.NewChan(size)` | Safe channel wrapper — prevents double-close panics |

---

## License

[MIT](LICENSE) © krigsherre
