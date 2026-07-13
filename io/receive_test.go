package io

import (
	"github.com/krigsherre/chanx/opt"
)

import (
	"context"
	"testing"
)

func TestReceive(t *testing.T) {
	// Receive with available value succeeds
	ch := make(chan int, 1)
	ch <- 42
	ctx := context.Background()
	v, ok, err := Receive(ch, opt.WithContext(ctx))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if !ok || v != 42 {
		t.Errorf("expected (42, true), got (%d, %t)", v, ok)
	}

	// Receive with cancelled context returns error
	ctx2, cancel := context.WithCancel(context.Background())
	cancel()
	ch2 := make(chan int)
	_, _, err2 := Receive(ch2, opt.WithContext(ctx2))
	if err2 != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err2)
	}

	// Receive on closed channel returns opt.ErrClosed
	ch3 := make(chan int)
	close(ch3)
	_, _, err3 := Receive(ch3, opt.WithContext(ctx))
	if err3 != opt.ErrClosed {
		t.Errorf("expected opt.ErrClosed, got %v", err3)
	}
}

func TestTryReceive(t *testing.T) {
	// Receive opt.NonBlocking with available value returns (val, true)
	ch := make(chan int, 1)
	ch <- 42
	v, ok, _ := Receive(ch, opt.NonBlocking())
	if !ok || v != 42 {
		t.Errorf("expected (42, true), got (%d, %t)", v, ok)
	}

	// Receive opt.NonBlocking on empty channel returns (zero, false)
	ch2 := make(chan int, 1)
	v2, ok2, _ := Receive(ch2, opt.NonBlocking())
	if ok2 || v2 != 0 {
		t.Errorf("expected (0, false), got (%d, %t)", v2, ok2)
	}
}

func TestRecvOr(t *testing.T) {
	// RecvOr returns value when available
	ch := make(chan int, 1)
	ch <- 42
	v := RecvOr(ch, -1)
	if v != 42 {
		t.Errorf("expected 42, got %d", v)
	}

	// RecvOr returns fallback when empty
	ch2 := make(chan int, 1)
	v2 := RecvOr(ch2, -1)
	if v2 != -1 {
		t.Errorf("expected -1, got %d", v2)
	}
}
