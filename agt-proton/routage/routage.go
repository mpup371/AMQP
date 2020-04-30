package main

import (
	"jf/AMQP/logger"
	"log"
	"net"
	"net/url"
	"strings"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

//TODO intercepter le kill pour finir le traitement en cours avant de quitter
func main() {
	//TODO url paramétrable
	url, err := amqp.ParseURL("amqp://localhost:5672/routage")
	if err != nil {
		log.Fatal(err)
	}
	for {
		if err := connect(url); err != nil {
			logger.Printf("main()", "Error: %v", err)
			time.Sleep(10 * time.Second)
		}
	}
}

func connect(url *url.URL) error {
	logger.Printf("main()", "Connecting to %s", url)

	topic := strings.TrimPrefix(url.Path, "/")
	handler := newHandler(topic)
	adapter := proton.NewMessagingAdapter(handler)
	adapter.AutoAccept = false
	adapter.PeerCloseError = true
	adapter.Prefetch = 0

	connection, err := net.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	if err != nil {
		logger.Debugf("connect", "Error connecting to %v: %v", url, err)
		return err
	}
	engine, err := proton.NewEngine(connection, adapter)
	logger.Printf("main()", "Accepted %v", engine)
	handler.engine = engine
	err = engine.Run()
	logger.Debugf("connect", "Error engine =%v", engine.Error())
	if err != nil {
		return err
	}
	return nil
}
