package main

import (
	"jf/AMQP/agt-proton/attributes"
	"os/exec"
	"testing"
	"time"

	"qpid.apache.org/amqp"
)

func mockMessage() amqp.Message {
	m := amqp.NewMessage()
	attr := attributes.NewAttributes()
	attr.Add("agt.routage.from", "moi@mamaison")
	attr.Add("agt.routage.to", "toi@tamaison")
	attr.Add("agt.routage.file", "/tmp/date")
	attr.Add("agt.data.bdpe.numero", "123456")
	m.SetBody(attr.Marshall())
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
	time.Sleep(1 * time.Second)
	if attr, err := attributes.GetAttributes("/tmp/date"); err == nil {
		if len(attr) != 4 {
			t.Error("len(attr) != 4")
		}
		for k, v := range attr {
			t.Logf("k=%s, v=%s", k, v)
		}
	} else {
		t.Errorf("Failed reading attributes: %v", err)
	}
}
