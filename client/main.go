package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {
	container := electron.NewContainer("jfbus")
	log.Printf("Container created : %s\n", container.Id())

	connection, err := container.Dial("tcp", os.Args[1])
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("Connection : %s\n", connection)
	defer func() {
		log.Println("Closing connection")
		connection.Close(nil)
	}()

	sender, err := connection.Sender(electron.Source("maSource"),
		electron.Target("maTarget"))
	PrintLink(sender)
	if err != nil {
		log.Fatal(err)
	}

	msg := amqp.NewMessageWith("Hello")
	msg.SetAddress("jfAddress")
	msg.SetSubject("Sujet: Hello")
	outcome := sender.SendSyncTimeout(msg, 10*time.Second)
	log.Printf("server response: %+v\n", outcome)
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("continuer ?")
	reader.ReadString('\n')
	// c := (sender.Session().Connection()).(net.Conn)

	outcome = sender.SendSyncTimeout(amqp.NewMessageWith("World"), 10*time.Second)
	log.Printf("server response: %+v\n", outcome)
	// connection.Close(nil)
	fmt.Println("quitter ?")
	reader.ReadString('\n')
}

func PrintLink(r electron.LinkSettings) {
	fmt.Printf("Connection: %s\n", r.Session().Connection())
	fmt.Printf("LinkName: %s, Source: %s, Target: %s \n", r.LinkName(), r.Source(), r.Target())
	fmt.Printf("SndSettleMode: %+v, RcvSettleMode: %+v\n", r.SndSettle(), r.RcvSettle())
}
