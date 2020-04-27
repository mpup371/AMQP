package main

import (
	"testing"

	"qpid.apache.org/amqp"
)

func TestSend(t *testing.T) {
	url, _ := amqp.ParseURL("amqp://localhost:5672/routage")
	connect(url)

}
