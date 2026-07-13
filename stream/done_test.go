package stream

import (
	"github.com/krigsherre/chanx/opt"
)

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestOrDone(t *testing.T) {
	// done closes -> output closes within 100ms
	done := make(chan struct{})
	ch := make(chan int)
	out := OrDone(done, ch)

	close(done)

	select {
	case _, ok := <-out:
		if ok {
			t.Errorf("expected closed output channel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("timeout waiting for output to close")
	}

	// ch closes -> output closes
	done2 := make(chan struct{})
	ch2 := make(chan int)
	out2 := OrDone(done2, ch2)
	close(ch2)
	select {
	case _, ok := <-out2:
		if ok {
			t.Errorf("expected closed output channel")
		}
	case <-time.After(100 * time.Millisecond):
		t.Errorf("timeout waiting for output to close")
	}

	// values pass through correctly
	done3 := make(chan struct{})
	ch3 := make(chan int, 3)
	ch3 <- 1
	ch3 <- 2
	ch3 <- 3
	close(ch3)
	out3 := OrDone(done3, ch3)
	var got []int
	for v := range out3 {
		got = append(got, v)
	}
	expected := []int{1, 2, 3}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}
}

func TestForEach(t *testing.T) {
	// collects all values, returns nil
	ch := Of(1, 2, 3)
	var got []int
	err := ForEach(ch, func(v int) error {
		got = append(got, v)
		return nil
	})
	if err != nil {
		t.Errorf("expected nil error, got %v", err)
	}
	if !reflect.DeepEqual(got, []int{1, 2, 3}) {
		t.Errorf("expected [1 2 3], got %v", got)
	}

	// fn returns error on 3rd value, iteration stops, error returned
	ch2 := Of(1, 2, 3, 4, 5)
	var got2 []int
	expectedErr := errors.New("stop")
	err2 := ForEach(ch2, func(v int) error {
		if v == 3 {
			return expectedErr
		}
		got2 = append(got2, v)
		return nil
	})
	if err2 != expectedErr {
		t.Errorf("expected %v, got %v", expectedErr, err2)
	}
	if !reflect.DeepEqual(got2, []int{1, 2}) {
		t.Errorf("expected [1 2], got %v", got2)
	}
}

func TestForEachCtx(t *testing.T) {
	// ctx cancelled mid-iteration, returns ctx.Err()
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan int, 5)
	ch <- 1
	ch <- 2

	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	err := ForEach(ch, func(v int) error {
		return nil
	}, opt.WithContext(ctx))

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

func TestDrain(t *testing.T) {
	// returns all values in order
	ch := Of(1, 2, 3)
	got := Drain(ch)
	expected := []int{1, 2, 3}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	// returns empty slice on closed channel
	ch2 := make(chan int)
	close(ch2)
	got2 := Drain(ch2)
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}
}

func TestFirst(t *testing.T) {
	// returns first value
	ch := Of(1, 2, 3)
	v, err := First(ch)
	if err != nil || v != 1 {
		t.Errorf("expected (1, nil), got (%d, %v)", v, err)
	}

	// returns opt.ErrClosed on closed channel
	ch2 := make(chan int)
	close(ch2)
	v2, err2 := First(ch2)
	if err2 != opt.ErrClosed || v2 != 0 {
		t.Errorf("expected (0, opt.ErrClosed), got (%d, %v)", v2, err2)
	}
}

func TestLast(t *testing.T) {
	// returns last value
	ch := Of(1, 2, 3)
	v, err := Last(ch)
	if err != nil || v != 3 {
		t.Errorf("expected (3, nil), got (%d, %v)", v, err)
	}

	// returns opt.ErrClosed on empty closed channel
	ch2 := make(chan int)
	close(ch2)
	v2, err2 := Last(ch2)
	if err2 != opt.ErrClosed || v2 != 0 {
		t.Errorf("expected (0, opt.ErrClosed), got (%d, %v)", v2, err2)
	}
}

func TestTake(t *testing.T) {
	// Take(5, Of(1,2,3,4,5,6,7)) -> [1,2,3,4,5]
	ch := Of(1, 2, 3, 4, 5, 6, 7)
	got := Drain(Take(ch, 5))
	expected := []int{1, 2, 3, 4, 5}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	// Take(0, Of(1,2,3)) -> empty channel
	ch2 := Of(1, 2, 3)
	got2 := Drain(Take(ch2, 0))
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}
}

func TestSkip(t *testing.T) {
	// Skip(2, Of(1,2,3,4,5)) -> [3,4,5]
	ch := Of(1, 2, 3, 4, 5)
	got := Drain(Skip(ch, 2))
	expected := []int{3, 4, 5}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	// Skip(10, Of(1,2)) -> empty channel
	ch2 := Of(1, 2)
	got2 := Drain(Skip(ch2, 10))
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}
}
