package stream

import "testing"

func TestOf(t *testing.T) {
	// Of with multiple values
	ch := Of(1, 2, 3)
	if cap(ch) != 3 {
		t.Errorf("expected cap 3, got %d", cap(ch))
	}
	for i := 1; i <= 3; i++ {
		if v, ok := <-ch; !ok || v != i {
			t.Errorf("expected %d, got %d (ok: %t)", i, v, ok)
		}
	}
	if _, ok := <-ch; ok {
		t.Errorf("expected channel to be closed")
	}

	// single value
	ch2 := Of("a")
	if v := <-ch2; v != "a" {
		t.Errorf("expected 'a', got '%s'", v)
	}
	if _, ok := <-ch2; ok {
		t.Errorf("expected channel to be closed")
	}

	// zero values
	ch3 := Of[int]()
	if _, ok := <-ch3; ok {
		t.Errorf("expected empty channel to be immediately closed")
	}
}

func TestFromSlice(t *testing.T) {
	// populated slice
	s := []int{1, 2, 3}
	ch := FromSlice(s)
	for i := 1; i <= 3; i++ {
		if v, ok := <-ch; !ok || v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
	}

	// empty slice
	ch2 := FromSlice([]int{})
	if _, ok := <-ch2; ok {
		t.Errorf("expected channel to be closed")
	}

	// nil slice
	var nilSlice []int
	ch3 := FromSlice(nilSlice)
	if _, ok := <-ch3; ok {
		t.Errorf("expected channel to be closed")
	}
}

func TestGenerate(t *testing.T) {
	ch := Generate(func(yield func(int) bool) {
		for i := 0; i < 5; i++ {
			if !yield(i) {
				break
			}
		}
	})

	for i := 0; i < 5; i++ {
		v, ok := <-ch
		if !ok || v != i {
			t.Errorf("expected %d, got %d", i, v)
		}
	}
	if _, ok := <-ch; ok {
		t.Errorf("expected channel to be closed")
	}
}

func TestRange(t *testing.T) {
	// Range(0, 10, 2) produces [0, 2, 4, 6, 8]
	ch := Range(0, 10, 2)
	expected := []int{0, 2, 4, 6, 8}
	for _, exp := range expected {
		v, ok := <-ch
		if !ok || v != exp {
			t.Errorf("expected %d, got %d", exp, v)
		}
	}
	if _, ok := <-ch; ok {
		t.Errorf("expected channel to be closed")
	}

	// Range with step <= 0 panics
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on step <= 0")
			}
		}()
		Range(0, 10, 0)
	}()
	func() {
		defer func() {
			if r := recover(); r == nil {
				t.Errorf("expected panic on step <= 0")
			}
		}()
		Range(0, 10, -1)
	}()

	// Range(5, 5, 1) produces empty channel
	ch2 := Range(5, 5, 1)
	if _, ok := <-ch2; ok {
		t.Errorf("expected channel to be closed")
	}
}

func TestRepeat(t *testing.T) {
	// Repeat with Take(5, Repeat(1, 2)) produces [1, 2, 1, 2, 1]
	// But we don't have Take yet. We will just read 5 items manually.
	ch := Repeat(1, 2)
	expected := []int{1, 2, 1, 2, 1}
	for _, exp := range expected {
		if v := <-ch; v != exp {
			t.Errorf("expected %d, got %d", exp, v)
		}
	}
}
