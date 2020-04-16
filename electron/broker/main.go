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

//
// This is a simple AMQP broker implemented using the procedural electron package.
//
// It maintains a set of named in-memory queues of messages. Clients can send
// messages to queues or subscribe to receive messages from them.
//

package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"qpid.apache.org/electron"
)

// Usage and command-line flags
func usage() {
	fmt.Fprintf(os.Stderr, `
Usage: %s
A simple message broker.
Queues are created automatically for sender or receiver addresses.
`, os.Args[0])
	flag.PrintDefaults()
}

var addr = flag.String("addr", ":amqp", "Network address to listen on, in the form \"host:port\"")
var credit = flag.Int("credit", 100, "Receiver credit window")
var qsize = flag.Int("qsize", 1000, "Max queue size")
var ack = flag.Bool("ack", true, "Send acknowledgements")

func main() {
	flag.Usage = usage
	flag.Parse()

	b := &broker{
		queues:    makeQueues(*qsize),
		container: electron.NewContainer(fmt.Sprintf("broker[%v]", os.Getpid())),
		// acks:      make(chan electron.Outcome),
		sent: make(chan sentMessage),
	}

	if err := b.run(); err != nil {
		log.Fatal(err)
	}
}
