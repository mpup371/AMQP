/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

package main

import (
	"flag"
	"fmt"
	"jf/AMQP/logger"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

// Usage and command-line flags
func usage() {
	fmt.Fprintf(os.Stderr, `Usage: %s url [url ...]
Send messages to each URL concurrently.
URLs are of the form "amqp://<host>:<port>/<amqp-address>"
`, os.Args[0])
	flag.PrintDefaults()
}

var count int
var ack, synack bool

func init() {
	flag.IntVar(&count, "count", 1, "Send this many messages to each address.")
	flag.BoolVar(&ack, "ack", true, "Expect for acknowledgements")
	flag.BoolVar(&synack, "sync", false, "Wait for ack")
}

func main() {
	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) == 0 {
		logger.Printf("main()", "No URL provided")
		usage()
		os.Exit(1)
	}

	urlStr := flag.Args()[0] // Non-flag arguments are URLs to receive from
	container := electron.NewContainer(fmt.Sprintf("send[%v]", os.Getpid()))
	// Start a goroutine for each URL to send messages.
	logger.Debugf("main()", "Connecting to %s", urlStr)
	beginConnection := time.Now()
	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	connection, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	addr := strings.TrimPrefix(url.Path, "/")
	sender, err := connection.Sender(electron.Target(addr), electron.Capacity(count), electron.LinkName("sendto-"+addr))
	fatalIf(err)
	// Loop sending messages.

	if !ack {
		sendForget(urlStr, addr, sender, count)
	} else if synack {
		sendSynAck(urlStr, addr, sender, count)
	} else {
		sentChan := make(chan electron.Outcome) // Channel to receive acknowledgements.
		var wait sync.WaitGroup
		go func() {
			wait.Add(1)
			acknowledge(sentChan, count)
			wait.Done()
		}()
		sendAck(urlStr, addr, sender, sentChan, count)
		wait.Wait()
	}

	endConnection := time.Now()
	elapsed := endConnection.Sub(beginConnection)
	ratio := int((float64)(count) / elapsed.Seconds())
	logger.Printf(urlStr, "%d messages sent in %s (%d msg/s)", count, elapsed, ratio)

	// sender.Close(nil) ne pas fermer le sender sinon connection.Close reste bloqué
	connection.Close(nil)
}

func sendForget(urlStr string, addr string, sender electron.Sender, count int) {
	for i := 0; i < count; i++ {
		m := amqp.NewMessage()
		body := fmt.Sprintf("%v%v", addr, i)
		m.Marshal(body)
		logger.Debugf(urlStr, "SendForget(%s)", body)
		sender.SendForget(m)
	}
}

func sendSynAck(urlStr string, addr string, sender electron.Sender, count int) {
	for i := 0; i < count; i++ {
		m := amqp.NewMessage()
		body := fmt.Sprintf("%v%v", addr, i)
		m.Marshal(body)
		logger.Debugf(urlStr, "sendSynAck(%s)", body)
		out := sender.SendSync(m)
		if out.Error != nil {
			logger.Fatalf(urlStr, "synack[%v] %v error: %v", i, out.Value, out.Error)
		} else if out.Status != electron.Accepted {
			logger.Fatalf(urlStr, "synack[%v] unexpected status: %v", i, out.Status)
		} else {
			logger.Debugf(urlStr, "synack[%v] %v (%v)", i, out.Value, out.Status)
		}
	}
}

func sendAck(urlStr string, addr string, sender electron.Sender, sentChan chan electron.Outcome, count int) {
	for i := 0; i < count; i++ {
		m := amqp.NewMessage()
		body := fmt.Sprintf("%v%v", addr, i)
		m.Marshal(body)
		logger.Debugf(urlStr, "SendAsync(%s)", body)
		sender.SendAsync(m, sentChan, body) // Outcome will be sent to sentChan
	}
}

func acknowledge(sentChan chan electron.Outcome, count int) {
	beginAck := time.Now()
	// Wait for all the acknowledgements
	logger.Debugf("main()", "Started senders, expect %v acknowledgements", count)
	for i := 0; i < count; i++ {
		out := <-sentChan // Outcome of async sends.
		if out.Error != nil {
			log.Fatalf("async ack[%v] %v error: %v", i, out.Value, out.Error)
		} else if out.Status != electron.Accepted {
			log.Fatalf("async ack[%v] unexpected status: %v", i, out.Status)
		} else {
			logger.Debugf("main()", "async ack[%v] %v (%v)", i, out.Value, out.Status)
		}
	}
	close(sentChan)
	endAck := time.Now()
	elapsed := endAck.Sub(beginAck)
	ratio := int((float64)(count) / elapsed.Seconds())
	logger.Debugf("main()", "%d ack received in %s (%d msg/s)", count, elapsed, ratio)

}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
