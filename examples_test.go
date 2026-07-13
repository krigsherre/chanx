package chanx_test

import (
	"fmt"

	"github.com/krigsherre/chanx/stream"
)

func ExampleMap() {
	doubled := stream.Map(
		stream.Of(1, 2, 3), 
		func(i int) int { return i * 2 },
	)
	
	results := doubled.Drain()
	fmt.Println(results)
	// Output: [2 4 6]
}

func ExampleStream_Filter() {
	results := stream.Of(1, 2, 3, 4, 5, 6).
		Filter(func(i int) bool { return i%2 == 0 }).
		Drain()
		
	fmt.Println(results)
	// Output: [2 4 6]
}

func ExampleFanOut() {
	ch := stream.Of(1, 2, 3)
	outs := stream.FanOut(ch, 3)

	var results []int
	for _, out := range outs {
		for v := range out {
			results = append(results, v)
		}
	}
	fmt.Println(results)
	// Output: [1 2 3]
}

func ExampleBatch() {
	ch := stream.Of(1, 2, 3, 4, 5)
	batches := stream.Batch(ch, 2)
	for b := range batches {
		fmt.Println(b)
	}
	// Output:
	// [1 2]
	// [3 4]
	// [5]
}

func Example_workerPool() {
	ch := stream.Range(1, 4, 1)
	workers := stream.FanOut(ch, 2)

	results := make([]<-chan string, 2)
	for i := 0; i < 2; i++ {
		results[i] = stream.Map(workers[i], func(v int) string {
			return fmt.Sprintf("val:%d", v)
		})
	}

	out := stream.Merge(results...)
	
	collected := out.Drain()
	
	for i := 0; i < len(collected); i++ {
		for j := i + 1; j < len(collected); j++ {
			if collected[i] > collected[j] {
				collected[i], collected[j] = collected[j], collected[i]
			}
		}
	}
	
	fmt.Println(collected)
	// Output: [val:1 val:2 val:3]
}

func ExampleStream_OrDone() {
	done := make(chan struct{})
	
	safeStream := stream.Repeat(1).OrDone(done)
	
	go func() {
		close(done)
	}()
	
	count := 0
	for range safeStream {
		count++
		if count > 10 {
			break
		}
	}
	
	fmt.Println("Gracefully stopped.")
	// Output: Gracefully stopped.
}
