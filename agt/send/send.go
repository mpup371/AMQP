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
	container := electron.NewContainer(fmt.Sprintf("send[%v]", os.Getpid()))
	// Start a goroutine for each URL to send messages.
	logger.Debugf("main()", "Connecting to %s", urlStr)
	url, err := amqp.ParseURL(urlStr)
	fatalIf(err)
	connection, err := container.Dial("tcp", url.Host) // NOTE: Dial takes just the Host part of the URL
	fatalIf(err)
	topic := strings.TrimPrefix(url.Path, "/")
	sender, err := connection.Sender(electron.Target(topic), electron.LinkName("send-"+topic))
	fatalIf(err)

	sendSynAck(urlStr, topic, sender)

	// sender.Close(nil) ne pas fermer le sender sinon connection.Close reste bloqué
	connection.Close(nil)
}

func sendSynAck(urlStr string, topic string, sender electron.Sender) {
	m := amqp.NewMessage()
	body := fmt.Sprintf("%v%v", topic)
	m.Marshal(body)
	logger.Debugf(urlStr, "sendSynAck(%s)", body)
	out := sender.SendSync(m)
	if out.Error != nil {
		logger.Fatalf(urlStr, "%v error: %v", out.Value, out.Error)
	} else if out.Status != electron.Accepted {
		logger.Fatalf(urlStr, "unexpected status: %v", out.Status)
	} else {
		logger.Debugf(urlStr, "%v (%v)", out.Value, out.Status)
	}
}

func fatalIf(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
