/*
go build -tags debug && PN_TRACE_FRM=1 ./send amqp://localhost:5672/queue1
*/

package main

import (
	"flag"
	"fmt"
	"jf/AMQP/logger"
	"log"
	"os"
	"strings"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		logger.Printf("main()", "No URL provided")
		os.Exit(1)
	}

	urlStr := flag.Args()[0] // Non-flag arguments are URLs to receive from
	container := electron.NewContainer(fmt.Sprintf("agtSend[%v]", os.Getpid()))
	// Start a goroutine for each URL to send messages.
	logger.Printf("main()", "Connecting to %s", urlStr)
	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	connection, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	topic := strings.TrimPrefix(url.Path, "/")
	sender, err := connection.Sender(electron.Target(topic))
	fatalIf(err)
	// sendForget(urlStr, sender)
	sendSynAck(urlStr, sender)
	logger.Debugf(urlStr, "Closing receiver...")
	sender.Close(nil)
	logger.Debugf(urlStr, "... closed")
	logger.Debugf(urlStr, "Closing connexion...")
	connection.Close(nil)
	logger.Debugf(urlStr, "... closed")
}

func sendForget(id string, sender electron.Sender) {
	m := amqp.NewMessage()
	body := fmt.Sprintf("message body")
	m.Marshal(body)
	logger.Debugf(id, "sendSynAck(%s)", body)
	sender.SendForget(m)
}

func sendSynAck(id string, sender electron.Sender) {
	m := amqp.NewMessage()
	body := fmt.Sprintf("message body")
	m.Marshal(body)
	logger.Debugf(id, "sendSynAck(%s)", body)
	out := sender.SendSync(m)
	if out.Error != nil {
		logger.Printf(id, "%v error: %v", out.Value, out.Error)
	} else if out.Status != electron.Accepted {
		logger.Printf(id, "unexpected status: %v", out.Status)
	} else {
		logger.Printf(id, "%v (%v)", out.Value, out.Status)
	}
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
