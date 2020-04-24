package main

import (
	"testing"

	"qpid.apache.org/amqp"
)

func TestMakeStats(t *testing.T) {
	queues := makeQueues()
	q := queues.Get("queue1")
	q.Add(amqp.NewMessage())
	q = queues.Get("queue2")
	q.Add(amqp.NewMessage())
	q.Add(amqp.NewMessage())

	if b, err := makeStats(&queues); err == nil {
		t.Log("résultat=", (string)(b))
	} else {
		t.Fatal(err)
	}
}
