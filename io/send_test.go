package io

import (
	"github.com/krigsherre/chanx/opt"
)

import (
	"context"
	"testing"
)

func TestSend(t *testing.T) {
	ch := make(chan int, 1)
	ctx := context.Background()

	// Send to channel with capacity succeeds
	ok, err := Send(ch, 42, opt.WithContext(ctx))
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if !ok {
		t.Errorf("expected true")
	}

	// Send with cancelled context returns error
	ctx2, cancel := context.WithCancel(context.Background())
	cancel()
	ch2 := make(chan int)
	ok2, err2 := Send(ch2, 42, opt.WithContext(ctx2))
	if err2 != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err2)
	}
	if ok2 {
		t.Errorf("expected false")
	}
}

func TestTrySend(t *testing.T) {
	ch := make(chan int, 1)

	// opt.NonBlocking Send to channel with capacity succeeds
	ok, _ := Send(ch, 42, opt.NonBlocking())
	if !ok {
		t.Errorf("expected TrySend to succeed")
	}

	// opt.NonBlocking Send to full channel fails
	ok2, _ := Send(ch, 100, opt.NonBlocking())
	if ok2 {
		t.Errorf("expected TrySend to fail on full channel")
	}
}
