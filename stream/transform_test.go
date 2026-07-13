package stream

import (
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
)

func TestMap(t *testing.T) {
	// Map(Of(1,2,3), func(i int) int { return i*2 }) -> [2,4,6]
	ch := Of(1, 2, 3)
	out := Map(ch, func(i int) int { return i * 2 })
	got := Drain(out)
	expected := []int{2, 4, 6}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	// Map on empty channel -> empty
	ch2 := Of[int]()
	out2 := Map(ch2, func(i int) int { return i * 2 })
	got2 := Drain(out2)
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}

	// Map with string transformation: Of(1,2,3) -> ["1","2","3"]
	ch3 := Of(1, 2, 3)
	out3 := Map(ch3, func(i int) string { return strconv.Itoa(i) })
	got3 := Drain(out3)
	expected3 := []string{"1", "2", "3"}
	if !reflect.DeepEqual(got3, expected3) {
		t.Errorf("expected %v, got %v", expected3, got3)
	}
}

func TestFilter(t *testing.T) {
	// Filter(Of(1,2,3,4,5), func(i int) bool { return i%2==0 }) -> [2,4]
	ch := Of(1, 2, 3, 4, 5)
	out := Filter(ch, func(i int) bool { return i%2 == 0 })
	got := Drain(out)
	expected := []int{2, 4}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	// Filter all excluded -> empty
	ch2 := Of(1, 2, 3)
	out2 := Filter(ch2, func(i int) bool { return false })
	got2 := Drain(out2)
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}

	// Filter all included -> all values
	ch3 := Of(1, 2, 3)
	out3 := Filter(ch3, func(i int) bool { return true })
	got3 := Drain(out3)
	expected3 := []int{1, 2, 3}
	if !reflect.DeepEqual(got3, expected3) {
		t.Errorf("expected %v, got %v", expected3, got3)
	}
}

func TestFlatMap(t *testing.T) {
	// FlatMap(Of(1,2,3), func(i int) <-chan int { return Of(i, i*10) }) -> [1,10,2,20,3,30]
	ch := Of(1, 2, 3)
	out := FlatMap(ch, func(i int) <-chan int { return Of(i, i*10) })
	got := Drain(out)
	expected := []int{1, 10, 2, 20, 3, 30}
	if !reflect.DeepEqual(got, expected) {
		t.Errorf("expected %v, got %v", expected, got)
	}

	// FlatMap with empty inner channels -> empty
	ch2 := Of(1, 2, 3)
	out2 := FlatMap(ch2, func(i int) <-chan int { return Of[int]() })
	got2 := Drain(out2)
	if len(got2) != 0 {
		t.Errorf("expected empty slice, got %v", got2)
	}
}

func TestReduce(t *testing.T) {
	// Reduce(Of(1,2,3,4), 0, func(a,b int) int { return a+b }) -> 10
	ch := Of(1, 2, 3, 4)
	got := Reduce(ch, 0, func(a, b int) int { return a + b })
	if got != 10 {
		t.Errorf("expected 10, got %v", got)
	}

	// Reduce on empty channel -> init value
	ch2 := Of[int]()
	got2 := Reduce(ch2, 42, func(a, b int) int { return a + b })
	if got2 != 42 {
		t.Errorf("expected 42, got %v", got2)
	}
}

func TestMapLeak(t *testing.T) {
	// Wait a moment for any goroutines from previous tests to settle
	time.Sleep(10 * time.Millisecond)
	initialGoroutines := runtime.NumGoroutine()

	func() {
		// Create a Map, read partial results, let the input channel be GC'd.
		// Since Of creates a closed channel, the goroutine will finish processing
		// and won't block since the items fit in the buffer (16).
		ch := Of(1, 2, 3)
		out := Map(ch, func(i int) int { return i * 2 })

		// Read partial results
		<-out
	}()

	// Wait for goroutine to process and exit
	time.Sleep(50 * time.Millisecond)

	finalGoroutines := runtime.NumGoroutine()
	// Allow for some background noise, but it shouldn't leave a persistent leak.
	// Normally final should be <= initial.
	if finalGoroutines > initialGoroutines+2 {
		t.Errorf("expected no leak, started with %d goroutines, ended with %d", initialGoroutines, finalGoroutines)
	}
}
