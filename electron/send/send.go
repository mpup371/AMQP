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

var count = flag.Int("count", 1, "Send this many messages to each address.")
var ack = flag.Bool("ack", true, "Expect for acknowledgements")
var synack = flag.Bool("sync", false, "Wait for ack")

func main() {
	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) == 0 {
		logger.Printf("main()", "No URL provided")
		usage()
		os.Exit(1)
	}
	urlStr := flag.Args()[0] // Non-flag arguments are URLs to receive from

	sentChan := make(chan electron.Outcome) // Channel to receive acknowledgements.

	container := electron.NewContainer(fmt.Sprintf("send[%v]", os.Getpid()))
	connections := make(chan electron.Connection, len(urlStr)) // Connections to close on exit
	// Start a goroutine for each URL to send messages.
	logger.Debugf("main()", "Connecting to %s", urlStr)
	beginConnection := time.Now()
	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	c, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	connections <- c // Save connection so we can Close() when main() ends
	addr := strings.TrimPrefix(url.Path, "/")
	s, err := c.Sender(electron.Target(addr), electron.Capacity(*count))
	fatalIf(err)
	// Loop sending messages.
	for i := 0; i < *count; i++ {
		m := amqp.NewMessage()
		body := fmt.Sprintf("%v%v", addr, i)
		m.Marshal(body)
		if *ack {
			if *synack {
				logger.Debugf(urlStr, "SendSync(%s)", body)
				out := s.SendSync(m)
				if out.Error != nil {
					logger.Fatalf(urlStr, "synack[%v] %v error: %v", i, out.Value, out.Error)
				} else if out.Status != electron.Accepted {
					logger.Fatalf(urlStr, "synack[%v] unexpected status: %v", i, out.Status)
				} else {
					logger.Debugf(urlStr, "synack[%v] %v (%v)", i, out.Value, out.Status)
				}
			} else {
				logger.Debugf(urlStr, "SendAsync(%s)", body)
				s.SendAsync(m, sentChan, body) // Outcome will be sent to sentChan
			}
		} else {
			logger.Debugf(urlStr, "SendForget(%s)", body)
			s.SendForget(m)
		}
	}
	endConnection := time.Now()
	elapsed := endConnection.Sub(beginConnection)
	ratio := int((float64)(*count) / elapsed.Seconds())
	logger.Printf(urlStr, "%d messages sent in %s (%d msg/s)", *count, elapsed, ratio)

	if !*synack && *ack {
		beginAck := time.Now()
		// Wait for all the acknowledgements
		logger.Debugf("main()", "Started senders, expect %v acknowledgements", *count)
		for i := 0; i < *count; i++ {
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
		ratio := int((float64)(*count) / elapsed.Seconds())
		logger.Debugf("main()", "%d ack received in %s (%d msg/s)", *count, elapsed, ratio)
	}

	close(connections)
	for c := range connections { // Close all connections
		if c != nil {
			c.Close(nil)
		}
	}
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
