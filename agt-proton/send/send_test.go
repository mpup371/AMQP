package main

import (
	"jf/AMQP/agt-proton/attributes"
	"os/exec"
	"testing"

	"qpid.apache.org/amqp"
)

func mockMessage() amqp.Message {
	m := amqp.NewMessage()
	attr := attributes.NewAttributes()
	attr = attributes.Add(attr, "agt.routage.from", "moi@mamaison")
	attr = attributes.Add(attr, "agt.routage.to", "toi@tamaison")
	attr = attributes.Add(attr, "agt.routage.file", "/tmp/date")
	attr = attributes.Add(attr, "agt.data.bdpe.numero", "123456")
	m.SetBody(attributes.Marshall(attr))
	return m
}

// lancer le broker d'abord
func TestSend(t *testing.T) {
	makeMessage = mockMessage
	exec.Command("sh", "-c", "date > /tmp/date").Run()
	url, _ := amqp.ParseURL("amqp://localhost:5672/routage")
	if err := connect(url); err != nil {
		t.Error(err)
	}
	if attr, err := attributes.GetAttributes("/tmp/date"); err == nil {
		if len(attr) != 4 {
			t.Error("len(attr) != 4")
		}
		for _, a := range attr {
			t.Logf("k=%s, v=%s", a.Key, a.Value)
		}
	} else {
		t.Errorf("Failed reading attributes: %v", err)
	}
}
