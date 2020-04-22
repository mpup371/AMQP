package main

import (
	"flag"
	"jf/AMQP/logger"
	"log"
	"net"
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
	logger.Printf("main()", "Connecting to %s", urlStr)
	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	connection, err := net.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	topic := strings.TrimPrefix(url.Path, "/")
	fatalIf(err)

	adapter := proton.NewMessagingAdapter(&handler{topic})
	// adapter.AutoSettle = false
	adapter.AutoAccept = false
	// adapter.AutoOpen = false
	// adapter.Prefetch = 1
	engine, err := proton.NewEngine(connection, adapter)
	fatalIf(err)
	logger.Printf("main()", "Accepted %v", engine)
	engine.Run()
	logger.Printf("main()", "Terminated %s (%v)", engine, engine.Error())
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
