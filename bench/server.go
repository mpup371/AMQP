package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func runServer(container electron.Container) {

	listener, err := net.Listen("tcp", *ipPort)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s\n", listener.Addr().String())

	connection, err := container.Accept(listener)
	for ; err == nil; connection, err = container.Accept(listener) {
		beginConnection := time.Now()
		log.Printf("Beginning  : connection: %s\n", connection)
		defer func() {
			log.Println("Closing connection")
			connection.Close(nil)
		}()
		var nbmsg int
		for incoming := range connection.Incoming() {
			log.Printf("Incomming : %+v\n", incoming)

			switch incoming := incoming.(type) {
			case *electron.IncomingConnection, *electron.IncomingSession:
				incoming.Accept()
			case *electron.IncomingReceiver:
				receiver := incoming.Accept().(electron.Receiver)
				PrintLink(receiver)
				nbmsg = 0
				for rm, err := receiver.Receive(); err == nil; rm, err = receiver.Receive() {
					// fmt.Printf("server received: %q\n", rm.Message.Body())
					if *sync {
						rm.Accept()
					}
					nbmsg++
				}
				fmt.Printf("receiver closed: %+v,%+v\n", receiver.Error(), err)
				break
			case nil:
				break // Connection is closed
			default:
				incoming.Reject(amqp.Errorf("AMQPserver", "unexpected endpoint %s", incoming))
			}
		}
		endConnection := time.Now()
		elapsed := endConnection.Sub(beginConnection)
		ratio := int((float64)(nbmsg) / elapsed.Seconds())
		log.Printf("%d messages received in %s (%d msg/s)", nbmsg, elapsed, ratio)
	}
	if err != nil {
		log.Fatal(err)
	}
}
