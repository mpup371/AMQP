package main

import (
	"log"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func runClient(container electron.Container) {

	connection, err := container.Dial("tcp", *ipPort)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connection : %s\n", connection)
	defer func() {
		log.Println("Closing connection")
		connection.Close(nil)
	}()

	sender, err := connection.Sender()
	PrintLink(sender)
	if err != nil {
		log.Fatal(err)
	}

	log.Println("starting benchmark ...")
	beginConnection := time.Now()
	if *sync {
		for i := 0; i < *nbMessages; i++ {
			sender.SendSync(amqp.NewMessageWith("Hello"))
		}
	} else {
		for i := 0; i < *nbMessages; i++ {
			sender.SendForget(amqp.NewMessageWith("Hello"))
		}
	}
	endConnection := time.Now()

	elapsed := endConnection.Sub(beginConnection)

	ratio := int((float64)(*nbMessages) / elapsed.Seconds())
	log.Printf("%d messages sent in %s (%d msg/s)", *nbMessages, elapsed, ratio)

}
