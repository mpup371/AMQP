// Created by Yaz Saito on 06/15/12.
// Modified by Geert-Johan Riemer, Foize B.V.

package fifo

import (
	"math/rand"
	"testing"
)

//++ TODO: Add test for empty queue
//++ TODO: Find a way to test the thread-safety
//++ TODO: Add test for large queue

func testAssert(t *testing.T, b bool, objs ...interface{}) {
	if !b {
		t.Fatal(objs...)
	}
}

func TestBasic(t *testing.T) {
	q := NewQueue()
	testAssert(t, q.Len() == 0, "Could not assert that new Queue has length zero (0).")
	q.Add(10)
	testAssert(t, q.Len() == 1, "Could not assert that Queue has lenght 1 after adding one item.")
	n, _ := q.Next()
	testAssert(t, n.(int) == 10, "Could not retrieve item from Queue correctly.")
	testAssert(t, q.Len() == 0, "Could not assert that Queue has length 0 after retrieving item.")
	size := q.Add(11)
	testAssert(t, size == 1, "Could not assert that Queue has length 1 after adding one item the second time.")
	testAssert(t, q.Len() == 1, "Could not assert that Queue has length 1 after adding one item the second time.")
	n, _ = q.Next()
	testAssert(t, n.(int) == 11, "Could not retrieve item from Queue correctly the second time.")
	testAssert(t, q.Len() == 0, "Could not assert that Queue has length 0 after retrieving item the second time.")
}

func TestRandomized(t *testing.T) {
	var first, last int
	q := NewQueue()
	for i := 0; i < 10000; i++ {
		if rand.Intn(2) == 0 {
			count := rand.Intn(128)
			for j := 0; j < count; j++ {
				q.Add(last)
				last++
			}
		} else {
			count := rand.Intn(128)
			if count > (last - first) {
				count = last - first
			}
			for i := 0; i < count; i++ {
				testAssert(t, q.Len() > 0, "len==0", q.Len())
				n, _ := q.Next()
				testAssert(t, n.(int) == first)
				first++
			}
		}
	}
}

func BenchmarkAlloc(b *testing.B) {
	for n := 0; n < b.N; n++ {

		q := NewQueue()
		for i := 0; i < 120000; i++ {
			q.Add(0)
		}
		for i := 0; i < 115000; i++ {
			q.Pop()
		}
		for i := 0; i < 250000; i++ {
			q.Add(0)
		}
	}
	b.ReportAllocs()
}
