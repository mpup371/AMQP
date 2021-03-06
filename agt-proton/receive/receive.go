package main

import (
	"flag"
	"jf/AMQP/logger"
	"log"
	"net"
	"net/url"
	"os"
	"strings"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		logger.Printf("main()", "No URL provided")
		os.Exit(1)
	}

	urlStr := flag.Args()[0]
	if url, err := amqp.ParseURL(urlStr); err != nil {
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
