package stream

import (
	"reflect"
	"runtime"
	"testing"
	"time"
)

func TestFanOut(t *testing.T) {
	ch := Of(1, 2, 3, 4, 5, 6)
	outs := FanOut(ch, 3)
	if len(outs) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(outs))
	}
	expected := [][]int{
		{1, 4},
		{2, 5},
		{3, 6},
	}
	for i := 0; i < 3; i++ {
		got := Drain(outs[i])
		if !reflect.DeepEqual(got, expected[i]) {
			t.Errorf("output %d expected %v, got %v", i, expected[i], got)
		}
	}

	ch2 := Of(1, 2, 3)
	outs2 := FanOut(ch2, 1)
	got2 := Drain(outs2[0])
	expected2 := []int{1, 2, 3}
	if !reflect.DeepEqual(got2, expected2) {
		t.Errorf("output expected %v, got %v", expected2, got2)
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on n=0")
			}
		}()
		FanOut(Of(1), 0)
	}()
}

func TestFanIn(t *testing.T) {
	out := Merge(Of(1, 2), Of(3, 4), Of(5, 6))
	got := Drain(out)

	if len(got) != 6 {
		t.Fatalf("expected 6 items, got %d", len(got))
	}
	counts := make(map[int]int)
	for _, v := range got {
		counts[v]++
	}
	for i := 1; i <= 6; i++ {
		if counts[i] != 1 {
			t.Errorf("expected 1 instance of %d, got %d", i, counts[i])
		}
	}

	out2 := Merge[int]()
	got2 := Drain(out2)
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}

	out3 := Merge(Of(1, 2, 3))
	got3 := Drain(out3)
	expected3 := []int{1, 2, 3}
	if !reflect.DeepEqual(got3, expected3) {
		t.Errorf("expected %v, got %v", expected3, got3)
	}
}

func TestTee(t *testing.T) {
	ch := Of(1, 2, 3)
	out1, out2 := Tee(ch)
	got1 := Drain(out1)
	got2 := Drain(out2)
	expected := []int{1, 2, 3}
	if !reflect.DeepEqual(got1, expected) {
		t.Errorf("output1 expected %v, got %v", expected, got1)
	}
	if !reflect.DeepEqual(got2, expected) {
		t.Errorf("output2 expected %v, got %v", expected, got2)
	}
}

func TestTeeN(t *testing.T) {
	ch := Of(1, 2, 3)
	outs := TeeN(ch, 3)
	if len(outs) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(outs))
	}
	expected := []int{1, 2, 3}
	for i := 0; i < 3; i++ {
		got := Drain(outs[i])
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("output %d expected %v, got %v", i, expected, got)
		}
	}

	ch2 := Of(1, 2)
	outs2 := TeeN(ch2, 1)
	if outs2[0] != ch2 {
		t.Errorf("expected passthrough, got different channel")
	}

	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on n=0")
			}
		}()
		TeeN(Of(1), 0)
	}()
}

func TestBridge(t *testing.T) {
	inner1 := Of(1, 2)
	inner2 := Of(3, 4)
	outer := Of(inner1, inner2)
	out := Bridge(outer)
	got := Drain(out)
	expected := []int{1, 2, 3, 4}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	outer2 := Of[Stream[int]]()
	out2 := Bridge(outer2)
	got2 := Drain(out2)
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}
}

func TestFanOutLeak(t *testing.T) {
	time.Sleep(10 * time.Millisecond)
	initial := runtime.NumGoroutine()
	func() {
		outs := FanOut(Of(1, 2, 3), 2)
		<-outs[0]
	}()
	time.Sleep(50 * time.Millisecond)
	if runtime.NumGoroutine() > initial+2 {
		t.Errorf("leak detected in FanOut")
	}
}

func TestFanInLeak(t *testing.T) {
	time.Sleep(10 * time.Millisecond)
	initial := runtime.NumGoroutine()
	func() {
		out := Merge(Of(1, 2), Of(3, 4))
		<-out
	}()
	time.Sleep(50 * time.Millisecond)
	if runtime.NumGoroutine() > initial+2 {
		t.Errorf("leak detected in FanIn")
	}
}

func TestTeeLeak(t *testing.T) {
	time.Sleep(10 * time.Millisecond)
	initial := runtime.NumGoroutine()
	func() {
		out1, _ := Tee(Of(1, 2))
		<-out1
	}()
	time.Sleep(50 * time.Millisecond)
	if runtime.NumGoroutine() > initial+2 {
		t.Errorf("leak detected in Tee")
	}
}

func TestBridgeLeak(t *testing.T) {
	time.Sleep(10 * time.Millisecond)
	initial := runtime.NumGoroutine()
	func() {
		out := Bridge(Of(Of(1, 2)))
		<-out
	}()
	time.Sleep(50 * time.Millisecond)
	if runtime.NumGoroutine() > initial+2 {
		t.Errorf("leak detected in Bridge")
	}
}
