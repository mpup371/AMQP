package main

import (
	"fmt"
	"log"
	"net"
	"os"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {
	container := electron.NewContainer("jfbus")
	log.Printf("container created : %s\n", container.Id())

	listener, err := net.Listen("tcp", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Listening on %s\n", listener.Addr().String())

	connection, err := container.Accept(listener)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connection : %s\n", connection)
	defer func() {
		log.Println("Closing connection")
		connection.Close(nil)
	}()

	listener.Close() // This server only accepts one connection

	for incoming := range connection.Incoming() {
		log.Printf("Incomming : %+v\n", incoming)

		switch incoming := incoming.(type) {
		case *electron.IncomingConnection, *electron.IncomingSession:
			incoming.Accept()
		case *electron.IncomingReceiver:
			receiver := incoming.Accept().(electron.Receiver)
			PrintLink(receiver)
			for rm, err := receiver.Receive(); err == nil; rm, err = receiver.Receive() {
				fmt.Printf("server received: subject:%q, body:%q\n",
					rm.Message.Subject(), rm.Message.Body())
				rm.Accept() // Signal to the client that the message was accepted
				// connection.Close(amqp.Errorf(amqp.DecodeError, "Ah non !"))
				// log.Fatal("exit!")
			}
			fmt.Printf("receiver closed: %+v,%+v\n", receiver.Error(), err)
			return
		case nil:
			return // Connection is closed
		default:
			incoming.Reject(amqp.Errorf("AMQPserver", "unexpected endpoint %s", incoming))
		}
	}
}

func PrintLink(r electron.LinkSettings) {
	fmt.Printf("Connection: %s\n", r.Session().Connection())
	fmt.Printf("LinkName: %s, Source: %s, Target: %s \n", r.LinkName(), r.Source(), r.Target())
	fmt.Printf("SndSettleMode: %+v, RcvSettleMode: %+v\n", r.SndSettle(), r.RcvSettle())
}
