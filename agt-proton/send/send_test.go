package main

import (
	"jf/AMQP/agt-proton/attributes"
	"os"
	"os/exec"
	"testing"
	"time"

	"qpid.apache.org/amqp"
)

func mockMessage() {

	attr = attributes.NewAttributes()
	attr.Add("user.agt.routage.from", "moi@mamaison")
	attr.Add("user.agt.routage.to", "toi@tamaison")
	attr.Add("user.agt.routage.file", "/tmp/date")
	attr.Add("user.agt.data.bdpe.numero", "123456")

}

// lancer le broker et le routage d'abord
// /usr/local/go/bin/go test jf/AMQP/agt-proton/send -run "^(TestSend)$" -v -count=1
func TestSend(t *testing.T) {
	mockMessage()
	os.Remove("/tmp/date")
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
func TestFileNotFound(t *testing.T) {
	mockMessage()
	os.Remove("/tmp/date")
	url, _ := amqp.ParseURL("amqp://localhost:5672/routage")
	if err := connect(url); err != nil {
		t.Error(err)
	}

}

func mockMessageKey() {
	attr = attributes.NewAttributes()
	attr.Add("user.agt.routage.from", "moi@mamaison")
	attr.Add("user.agt.routage.file", "/tmp/date")
	attr.Add("user.agt.data.bdpe.numero", "123456")
	attr.Add("user.agt.routage.key", "clef1")

}
func TestClef(t *testing.T) {
	mockMessageKey()
	os.Remove("/tmp/date")
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
