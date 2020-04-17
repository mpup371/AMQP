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
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

// Usage and command-line flags
func usage() {
	fmt.Fprintf(os.Stderr, `Usage: %s url [url ...]
Receive messages from a URL and print them.
URL is of the form "amqp://<host>:<port>/<amqp-address>"
`, os.Args[0])
	flag.PrintDefaults()
}

var count = flag.Int("count", 1, "Stop after receiving this many messages in total")
var prefetch = flag.Int("prefetch", 0, "enable a pre-fetch window for flow control")

func main() {
	flag.Usage = usage
	flag.Parse()

	if len(flag.Args()) == 0 {
		logger.Printf("main()", "No URL provided")
		usage()
		os.Exit(1)
	}
	urlStr := flag.Args()[0] // Non-flag arguments are URLs to receive from

	// container := electron.NewContainer(fmt.Sprintf("receive[%v]", os.Getpid()))
	container := electron.NewContainer("receive")

	logger.Debugf("main()", "Connecting to %s", urlStr)

	beginConnection := time.Now()
	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	c, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	addr := strings.TrimPrefix(url.Path, "/")
	opts := []electron.LinkOption{electron.Source(addr), electron.LinkName("receiver-" + addr)}
	if *prefetch > 0 { // Use a pre-fetch window
		opts = append(opts, electron.Capacity(*prefetch), electron.Prefetch(true))
	} else { // Grant credit for all expected messages at once
		opts = append(opts, electron.Capacity(*count), electron.Prefetch(false))
	}
	r, err := c.Receiver(opts...)
	fatalIf(err)
	// Loop receiving messages and sending them to the main() goroutine

	for i := 0; i < *count; i++ {
		if rm, err := r.Receive(); err == nil {
			logger.Debugf(urlStr, "accept %s", rm.Message.Body())
			if err := rm.Accept(); err != nil {
				logger.Printf(urlStr, "Error on accept: %s", err)
			}
		} else {
			logger.Fatalf(urlStr, "receive error %v: %v", urlStr, err)
			break
		}
	}

	// time.Sleep(1 * time.Second)
	// logger.Debugf(urlStr, "Closing receiver...")
	// r.Close(nil) ne sert à rien
	// logger.Debugf(urlStr, "... closed")
	time.Sleep(1 * time.Second)
	logger.Debugf(urlStr, "Closing connexion...")
	c.Close(nil)
	logger.Debugf(urlStr, "... closed")
	endConnection := time.Now()
	expect := int(*count)
	elapsed := endConnection.Sub(beginConnection)
	ratio := int((float64)(expect) / elapsed.Seconds())
	logger.Printf(urlStr, "%d messages received in %s (%d msg/s)", expect, elapsed, ratio)

}

func fatalIf(err error) {
	if err != nil {
		logger.Fatalf("main()", "error: %s", err)
	}
}
