package main

import (
	"jf/AMQP/agt-proton/attributes"
	"os/exec"
	"testing"

	"qpid.apache.org/amqp"
)

// lancer le broker d'abord
func TestSend(t *testing.T) {
	exec.Command("sh", "-c", "date > /tmp/date")
	url, _ := amqp.ParseURL("amqp://localhost:5672/routage")
	connect(url)
	if attr, err := attributes.GetAttributes("/tmp/date"); err == nil {
		for _, a := range attr {
			t.Logf("k=%s, v=%s", a.Key, a.Value)
		}
	} else {
		t.Errorf("Failed reading attributes: %v", err)
	}
}
