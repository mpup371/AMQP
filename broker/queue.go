package main

import (
	"sync"

	"qpid.apache.org/amqp"
)

// Use a buffered channel as a very simple queue.
type queue chan amqp.Message

// Put a message back on the queue, does not block.
func (q queue) PutBack(m amqp.Message) {
	select {
	case q <- m:
	default:
		// Not an efficient implementation but ensures we don't block the caller.
		go func() { q <- m }()
	}
}

// Concurrent-safe map of queues.
type queues struct {
	queueSize int
	m         map[string]queue
	lock      sync.Mutex
}

func makeQueues(queueSize int) queues {
	return queues{queueSize: queueSize, m: make(map[string]queue)}
}

// Create a queue if not found.
func (qs *queues) Get(name string) queue {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	q := qs.m[name]
	if q == nil {
		q = make(queue, qs.queueSize)
		qs.m[name] = q
	}
	return q
}
