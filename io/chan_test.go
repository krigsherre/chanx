package io

import (
	"context"
	"sync"
	"testing"

	"github.com/krigsherre/chanx/opt"
)

func TestNewChan(t *testing.T) {
	ch1 := NewChan[int](0)
	if ch1.Cap() != 0 {
		t.Errorf("expected cap 0, got %d", ch1.Cap())
	}

	ch2 := NewChan[int](10)
	if ch2.Cap() != 10 {
		t.Errorf("expected cap 10, got %d", ch2.Cap())
	}
	if ch2.Len() != 0 {
		t.Errorf("expected len 0, got %d", ch2.Len())
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on size < 0")
			}
		}()
		NewChan[int](-1)
	}()
}

func TestChanSendReceive(t *testing.T) {
	ch := NewChan[int](1)
	ctx := context.Background()

	_, err := ch.Send(42, opt.WithContext(ctx))
	if err != nil {
		t.Errorf("expected nil error on send, got %v", err)
	}
	v, _, err := ch.Receive(opt.WithContext(ctx))
	if err != nil {
		t.Errorf("expected nil error on receive, got %v", err)
	}
	if v != 42 {
		t.Errorf("expected 42, got %d", v)
	}

	ch.Send(100, opt.WithContext(ctx))
	if ok, _ := ch.Send(101, opt.NonBlocking()); ok {
		t.Errorf("expected TrySend to return false on full channel")
	}

	ch.Receive(opt.WithContext(ctx))
	v2, ok, _ := ch.Receive(opt.NonBlocking())
	if ok || v2 != 0 {
		t.Errorf("expected (0, false), got (%d, %t)", v2, ok)
	}
}

func TestChanClose(t *testing.T) {
	ch := NewChan[int](1)

	if ch.IsClosed() {
		t.Errorf("expected false before close")
	}

	ch.Close()

	if !ch.IsClosed() {
		t.Errorf("expected true after close")
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Errorf("double close panicked: %v", r)
			}
		}()
		ch.Close()
	}()
}

func TestChanInOut(t *testing.T) {
	ch := NewChan[int](1)
	in := ch.In()
	out := ch.Out()

	select {
	case in <- 42:
	default:
		t.Errorf("expected to send successfully")
	}

	select {
	case v := <-out:
		if v != 42 {
			t.Errorf("expected 42, got %d", v)
		}
	default:
		t.Errorf("expected to receive successfully")
	}
}

func TestChanRace(t *testing.T) {
	ch := NewChan[int](100)
	ctx := context.Background()
	var wg sync.WaitGroup

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = ch.Send(i, opt.WithContext(ctx))
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _, _ = ch.Receive(opt.WithContext(ctx))
		}
	}()
	wg.Wait()

	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _, _ = ch.Receive(opt.WithContext(ctx))
		}
	}()

	go func() {
		defer wg.Done()
		ch.Close()
		ch.Close()
	}()
	wg.Wait()
}
