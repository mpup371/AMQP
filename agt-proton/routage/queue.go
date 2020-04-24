package main

import (
	"sync"
	"time"

	fifo "github.com/foize/go.fifo"
	"qpid.apache.org/amqp"
)

type queue struct {
	fifo.Queue
	creationDate int64
	lastAccess   int64
}

//TODO remonter lock ici

func (q *queue) Add(m amqp.Message) uint {
	return q.Add(m)
}
func (q *queue) Pop() uint {
	return q.Pop()
}

func (q *queue) Peek() amqp.Message {
	m := q.Peek()
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
	q := queue{fifo.NewQueue(), creationDate: time.Now().Unix()}
	qs.m[name] = &q
	return &q
}

func (qs *queues) Len() int {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	return len(qs.m)
}

type Qinfo struct {
	Name string
	Size uint
}

func (qs *queues) Infos() (l []Qinfo) {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	l = make([]Qinfo, 0, len(qs.m))

	for name, q := range qs.m {
		l = append(l, Qinfo{name, (*fifo.Queue)(q).Len()})
	}
	return l
}
