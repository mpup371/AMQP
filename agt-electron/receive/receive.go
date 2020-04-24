/*
go build -tags debug && PN_TRACE_FRM=1 ./receive  amqp://localhost:5672/queue1
*/

package main

import (
	"flag"
	"fmt"
	"jf/AMQP/logger"
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

	// container := electron.NewContainer(fmt.Sprintf("receive[%v]", os.Getpid()))
	container := electron.NewContainer(fmt.Sprintf("agtReceive[%v]", os.Getpid()))

	logger.Debugf("main()", "Connecting to %s", urlStr)

	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	connection, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	addr := strings.TrimPrefix(url.Path, "/")
	opts := []electron.LinkOption{electron.Source(addr)}
	receiver, err := connection.Receiver(opts...)
	fatalIf(err)

	//TODO mode non bloquant ?
	if rm, err := receiver.Receive(); err == nil {
		logger.Printf(urlStr, "accept %s", rm.Message.Body())
		if err := rm.Accept(); err != nil {
			logger.Printf(urlStr, "Error on accept: %s", err)
		}
	} else {
		logger.Printf(urlStr, "receive error %v: %v", urlStr, err)
	}

	// time.Sleep(1 * time.Second)
	logger.Debugf(urlStr, "Closing receiver...")
	receiver.Close(nil) // ne sert à rien ?
	logger.Debugf(urlStr, "... closed")
	// time.Sleep(1 * time.Second)
	logger.Debugf(urlStr, "Closing connexion...")
	connection.Close(nil)
	logger.Debugf(urlStr, "... closed")

}

func fatalIf(err error) {
	if err != nil {
		logger.Fatalf("main()", "error: %s", err)
	}
}
