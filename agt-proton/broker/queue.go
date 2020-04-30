package main

import (
	"sync"
	"time"

	fifo "github.com/foize/go.fifo"
	"qpid.apache.org/amqp"
)

type queue struct {
	fq           fifo.Queue
	mu           sync.Mutex
	creationDate time.Time
	lastRead     time.Time
	lastWrite    time.Time
}

func (q *queue) Len() uint {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.fq.Len()
}

func (q *queue) Add(m amqp.Message) uint {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastWrite = time.Now()
	return q.fq.Add(m)
}
func (q *queue) Pop() uint {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.fq.Pop()
}

func (q *queue) Peek() amqp.Message {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.lastRead = time.Now()
	m := q.fq.Peek()
	if m == nil {
		return nil
	}
	return m.(amqp.Message)
}

// Concurrent-safe map of queues.
type queues struct {
	m  map[string]*queue
	mu sync.Mutex
}

func makeQueues() queues {
	return queues{m: make(map[string]*queue)}
}

// Create a queue if not found.
func (qs *queues) Get(name string) *queue {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	if q, ok := qs.m[name]; ok {
		return q
	}
	q := queue{fifo.NewQueue(), sync.Mutex{}, time.Now(), time.Time{}, time.Time{}}
	qs.m[name] = &q
	return &q
}

func (qs *queues) Len() int {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	return len(qs.m)
}

type qInfo struct {
	Name      string
	Size      uint
	Creation  time.Time
	LastRead  time.Time
	LastWrite time.Time
}

func (qs *queues) Infos() (l []qInfo) {
	qs.mu.Lock()
	defer qs.mu.Unlock()
	l = make([]qInfo, 0, len(qs.m))

	for name, q := range qs.m {
		l = append(l, qInfo{name, q.Len(), q.creationDate, q.lastRead, q.lastWrite})
	}
	return l
}
