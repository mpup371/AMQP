package main

import (
	"jf/AMQP/agt-proton/attributes"

	"qpid.apache.org/amqp"
)

func newMessage() amqp.Message {
	m := amqp.NewMessage()
	attr := attributes.NewAttributes()
	m.SetBody(attr.Marshall())
	return m
}
