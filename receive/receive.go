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
Receive messages from all URLs concurrently and print them.
URLs are of the form "amqp://<host>:<port>/<amqp-address>"
`, os.Args[0])
	flag.PrintDefaults()
}

var count = flag.Int("count", 1, "Stop after receiving this many messages in total")
var prefetch = flag.Int("prefetch", 0, "enable a pre-fetch window for flow control")
var debug = flag.Bool("debug", false, "Print detailed debug output")
var debugf logger.DebugFunction // Default no debugging output

func main() {
	flag.Usage = usage
	flag.Parse()

	if *debug {
		debugf = logger.Debugf
	}
	urls := flag.Args() // Non-flag arguments are URLs to receive from
	if len(urls) == 0 {
		logger.Printf("main()", "No URL provided")
		usage()
		os.Exit(1)
	}

	messages := make(chan amqp.Message) // Channel for messages from goroutines to main()
	defer close(messages)

	var wait sync.WaitGroup // Used by main() to wait for all goroutines to end.
	wait.Add(len(urls))     // Wait for one goroutine per URL.

	container := electron.NewContainer(fmt.Sprintf("receive[%v]", os.Getpid()))
	connections := make(chan electron.Connection, len(urls)) // Connections to close on exit

	// Start a goroutine to for each URL to receive messages and send them to the messages channel.
	// main() receives and prints them.
	for _, urlStr := range urls {
		debugf("main()", "Connecting to %s", urlStr)
		go func(urlStr string) { // Start the goroutine
			defer wait.Done() // Notify main() when this goroutine is done.
			beginConnection := time.Now()
			url, err := amqp.ParseURL(urlStr)
			fatalIf(err)
			c, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
			fatalIf(err)
			connections <- c // Save connection so we can Close() when main() ends
			addr := strings.TrimPrefix(url.Path, "/")
			opts := []electron.LinkOption{electron.Source(addr)}
			if *prefetch > 0 { // Use a pre-fetch window
				opts = append(opts, electron.Capacity(*prefetch), electron.Prefetch(true))
			} else { // Grant credit for all expected messages at once
				opts = append(opts, electron.Capacity(*count), electron.Prefetch(false))
			}
			r, err := c.Receiver(opts...)
			fatalIf(err)
			// Loop receiving messages and sending them to the main() goroutine

			for i := 0; ; i++ {
				if rm, err := r.Receive(); err == nil {
					if i < 3 {
						debugf(urlStr, "accept %s", rm.Message.Body())
						rm.Accept()
					} else {
						debugf(urlStr, "reject %s", rm.Message.Body())
						rm.Reject()
					}
					messages <- rm.Message
				} else if err == electron.Closed {
					debugf(urlStr, "Connection closed")
					break
				} else {
					logger.Fatalf(urlStr, "receive error %v: %v", urlStr, err)
				}
			}
			endConnection := time.Now()
			expect := int(*count) * len(urls)
			elapsed := endConnection.Sub(beginConnection)
			ratio := int((float64)(expect) / elapsed.Seconds())
			logger.Printf(urlStr, "%d messages received in %s (%d msg/s)", expect, elapsed, ratio)
		}(urlStr)
	}

	// All goroutines are started, we are receiving messages.
	logger.Printf("main()", "Listening on %d connections", len(urls))

	// print each message until the count is exceeded.
	for i := 0; i < *count; i++ {
		m := <-messages
		debugf("main()", "%v", m.Body())
	}
	logger.Printf("main()", "Received %d messages", *count)

	// Close all connections, this will interrupt goroutines blocked in Receiver.Receive()
	// with electron.Closed.
	for i := 0; i < len(urls); i++ {
		c := <-connections
		debugf("main()", "close %s", c)
		c.Close(nil)
	}
	wait.Wait() // Wait for all goroutines to finish.
}

func fatalIf(err error) {
	if err != nil {
		logger.Fatalf("main()", "error: %s", err)
	}
}
