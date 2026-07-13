package stream

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/krigsherre/chanx/opt"
)

func TestBuffer(t *testing.T) {
	t.Parallel()
	ch := Of(1, 2, 3)
	out := Buffer(ch, 10)
	got := Drain(out)
	expected := []int{1, 2, 3}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	ch2 := make(chan int)
	out2 := Buffer(ch2, 100)
	go func() {
		for i := 0; i < 50; i++ {
			ch2 <- i
		}
		close(ch2)
	}()

	var got2 []int
	for v := range out2 {
		got2 = append(got2, v)
		time.Sleep(1 * time.Millisecond)
	}
	if len(got2) != 50 {
		t.Errorf("expected 50 items, got %d", len(got2))
	}
}

func TestBatch(t *testing.T) {
	t.Parallel()
	out1 := Batch(Of(1, 2, 3, 4, 5), 2)
	got1 := Drain(out1)
	exp1 := [][]int{{1, 2}, {3, 4}, {5}}
	if !reflect.DeepEqual(got1, exp1) {
		t.Errorf("expected %v, got %v", exp1, got1)
	}

	out2 := Batch(Of(1, 2, 3, 4), 2)
	got2 := Drain(out2)
	exp2 := [][]int{{1, 2}, {3, 4}}
	if !reflect.DeepEqual(got2, exp2) {
		t.Errorf("expected %v, got %v", exp2, got2)
	}

	out3 := Batch(Of(1), 5)
	got3 := Drain(out3)
	exp3 := [][]int{{1}}
	if !reflect.DeepEqual(got3, exp3) {
		t.Errorf("expected %v, got %v", exp3, got3)
	}

	out4 := Batch(Of[int](), 5)
	got4 := Drain(out4)
	if len(got4) != 0 {
		t.Errorf("expected empty, got %v", got4)
	}
}

func TestBatchWithTimeout(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ch := make(chan int)
	out := Batch(ch, 1000, opt.WithContext(ctx), opt.WithTimeout(30*time.Millisecond))

	go func() {
		ch <- 1
		ch <- 2
		time.Sleep(60 * time.Millisecond)
		ch <- 3
		close(ch)
	}()

	batches := Drain(out)
	if len(batches) != 2 {
		t.Fatalf("expected 2 batches, got %d: %v", len(batches), batches)
	}
	if !reflect.DeepEqual(batches[0], []int{1, 2}) {
		t.Errorf("expected first batch [1 2], got %v", batches[0])
	}
	if !reflect.DeepEqual(batches[1], []int{3}) {
		t.Errorf("expected second batch [3], got %v", batches[1])
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := make(chan int)
	out2 := Batch(ch2, 1000, opt.WithContext(ctx2), opt.WithTimeout(100*time.Millisecond))

	go func() {
		ch2 <- 1
		time.Sleep(10 * time.Millisecond)
		cancel2()
	}()

	batches2 := Drain(out2)
	if len(batches2) != 1 || !reflect.DeepEqual(batches2[0], []int{1}) {
		t.Errorf("expected 1 batch [1], got %v", batches2)
	}
}

func TestThrottle(t *testing.T) {
	t.Parallel()
	ch := make(chan int)
	out := Throttle(ch, 50*time.Millisecond)

	start := time.Now()
	go func() {
		for i := 0; i < 5; i++ {
			ch <- i
		}
		close(ch)
	}()

	var times []time.Time
	for range out {
		times = append(times, time.Now())
	}

	if len(times) != 5 {
		t.Fatalf("expected 5 items, got %d", len(times))
	}

	for i := 1; i < len(times); i++ {
		diff := times[i].Sub(times[i-1])
		if diff < 40*time.Millisecond {
			t.Errorf("values too close: %v between item %d and %d", diff, i-1, i)
		}
	}

	total := times[len(times)-1].Sub(start)
	if total < 180*time.Millisecond {
		t.Errorf("total time too short: %v", total)
	}
}

func TestDebounce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	ch := make(chan int)
	out := Debounce(ch, 30*time.Millisecond, opt.WithContext(ctx))

	go func() {
		ch <- 1
		time.Sleep(10 * time.Millisecond)
		ch <- 2
		time.Sleep(10 * time.Millisecond)
		ch <- 3
		time.Sleep(60 * time.Millisecond)

		ch <- 4
		close(ch)
	}()

	var got []int
	for v := range out {
		got = append(got, v)
	}

	expected := []int{3, 4}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	ch2 := make(chan int)
	out2 := Debounce(ch2, 50*time.Millisecond, opt.WithContext(ctx2))
	go func() {
		ch2 <- 42
		time.Sleep(10 * time.Millisecond)
		cancel2()
	}()

	var got2 []int
	for v := range out2 {
		got2 = append(got2, v)
	}
	if len(got2) != 1 || got2[0] != 42 {
		t.Errorf("expected [42], got %v", got2)
	}
}
