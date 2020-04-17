package main

import (
	"sync"

	fifo "github.com/foize/go.fifo"
	"qpid.apache.org/amqp"
)

type queue fifo.Queue

func (q *queue) Add(m amqp.Message) {
	(*fifo.Queue)(q).Add(m)
}

func (q *queue) Pop() {
	(*fifo.Queue)(q).Next()
}

func (q *queue) Peek() amqp.Message {
	m := (*fifo.Queue)(q).Peek()
	if m == nil {
		return nil
	}
	return m.(amqp.Message)
}

// Concurrent-safe map of queues.
type queues struct {
	m    map[string]*queue
	lock sync.Mutex
}

func makeQueues() queues {
	return queues{m: make(map[string]*queue)}
}

// Create a queue if not found.
func (qs *queues) Get(name string) *queue {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	if q, ok := qs.m[name]; ok {
		return q
	}
	q := (*queue)(fifo.NewQueue())
	qs.m[name] = q
	return q
}
