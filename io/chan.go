package io

import (
	"github.com/krigsherre/chanx/opt"
	"github.com/krigsherre/chanx/stream"
)

import (
	"sync"
	"sync/atomic"
)

// Chan is a generic, safe wrapper around a Go channel.
// It provides method-based access to all chanx operations and
// prevents double-close panics.
type Chan[T any] struct {
	ch     chan T
	closed atomic.Bool
	once   sync.Once
}

// NewChan creates a new Chan[T] with the given buffer size.
// Use size 0 for an unbuffered channel.
func NewChan[T any](size int) *Chan[T] {
	if size < 0 {
		panic("chanx: NewChan size must be >= 0")
	}
	return &Chan[T]{
		ch: make(chan T, size),
	}
}

// In returns the send-only direction of the underlying channel.
func (c *Chan[T]) In() chan<- T {
	return c.ch
}

// Out returns the receive-only direction of the underlying channel.
func (c *Chan[T]) Out() <-chan T {
	return c.ch
}

// Stream turns the channel into a fluent Stream for chaining operations.
func (c *Chan[T]) Stream() stream.Stream[T] {
	return c.ch
}

// Len returns the number of items queued in the buffer.
func (c *Chan[T]) Len() int {
	return len(c.ch)
}

// Cap returns the buffer capacity.
func (c *Chan[T]) Cap() int {
	return cap(c.ch)
}

// Send sends a value on the channel. Use options for Context or opt.NonBlocking.
func (c *Chan[T]) Send(val T, opts ...opt.Option) (bool, error) {
	return Send(c.ch, val, opts...)
}

// Receive receives a value from the channel. Use options for Context or opt.NonBlocking.
func (c *Chan[T]) Receive(opts ...opt.Option) (T, bool, error) {
	return Receive(c.ch, opts...)
}

// Close safely closes the channel. Safe to call multiple times (no panic).
func (c *Chan[T]) Close() {
	c.once.Do(func() {
		c.closed.Store(true)
		close(c.ch)
	})
}

// IsClosed reports whether Close has been called.
func (c *Chan[T]) IsClosed() bool {
	return c.closed.Load()
}
