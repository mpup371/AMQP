package main

import (
	"jf/AMQP/logger"
	"log"
	"net"
	"net/url"
	"strings"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

func main() {

	if url, err := amqp.ParseURL("amqp://localhost:5672/routage"); err != nil {
		log.Fatal(err)
	} else if err := connect(url); err != nil {
		log.Fatal(err)
	}
}

func connect(url *url.URL) error {
	logger.Printf("main()", "Connecting to %s", url)
	connection, err := net.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	if err != nil {
		return err
	}

	topic := strings.TrimPrefix(url.Path, "/")
	if err != nil {
		return err
	}

	adapter := proton.NewMessagingAdapter(&handler{topic})
	adapter.AutoAccept = false
	adapter.PeerCloseError = true
	adapter.Prefetch = 0

	engine, err := proton.NewEngine(connection, adapter)
	if err != nil {
		return err
	}

	logger.Printf("main()", "Accepted %v", engine)
	engine.Run()
	logger.Printf("main()", "Terminated %s (%v)", engine, engine.Error())
	return nil
}
