package main

import (
	"jf/AMQP/agt-proton/attributes"

	"qpid.apache.org/amqp"
)

func newMessage() amqp.Message {
	m := amqp.NewMessage()
	attr := attributes.NewAttributes()
	attr = attributes.Add(attr, "agt.routage.from", "moi@mamaison")
	attr = attributes.Add(attr, "agt.routage.to", "toi@tamaison")
	attr = attributes.Add(attr, "agt.routage.file", "/tmp/date")
	attr = attributes.Add(attr, "agt.data.bdpe.numero", "123456")
	m.SetBody(attributes.Marshall(attr))
	return m
}
