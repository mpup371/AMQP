/*
PN_TRACE_FRM=1 go run -tags debug .

*/

package main

import (
	"flag"
	"fmt"
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"
	"log"
	"net"
	"sync"

	"qpid.apache.org/proton"
)

var addr = flag.String("addr", ":amqp", "Listening address")
var credit = flag.Int("credit", 100, "Receiver credit window")
var qsize = flag.Int("qsize", 1000, "Max queue size")

// State for the broker
type broker struct {
	queues   queues // queues immutable et a son propre mutex
	handlers map[string]*handler
	mu       sync.Mutex // pour l'accès aux handlers uniquement
}

var agt broker

func main() {
	flag.Parse()
	agt = broker{queues: makeQueues(),
		handlers: make(map[string]*handler)}
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

// Listens for connections and starts a proton.Engine for each one.
func run() error {
	listener, err := net.Listen("tcp", *addr)

	if err != nil {
		return err
	}

	defer listener.Close()
	logger.Printf("broker", "Listening on %s\n", listener.Addr())
	for {
		conn, err := listener.Accept()

		if err != nil {
			logger.Printf("broker", "Accept error: %v", err)
			continue
		}

		h := newHandler(&agt.queues)
		adapter := proton.NewMessagingAdapter(h)
		// We want to accept messages when they are enqueued, not just when they
		// are received, so we turn off auto-accept and prefetch by the adapter.
		adapter.Prefetch = 0
		adapter.AutoAccept = false
		adapter.AutoOpen = true
		adapter.AutoSettle = true
		engine, err := proton.NewEngine(conn, adapter)

		if err != nil {
			logger.Printf("broker", "Connection error: %v", err)
			continue
		}
		engine.Connection().SetContainer(util.GetName())
		engine.Server() // Enable server-side protocol negotiation.
		logger.Printf("broker", "Accepted connection %s", engine)
		h.engine = engine.Id()
		h.connection = fmt.Sprintf("%s-%s", conn.LocalAddr(), conn.RemoteAddr())
		agt.mu.Lock()
		agt.handlers[h.engine] = h
		agt.mu.Unlock()
		go func() { // Start goroutine to run the engine event loop
			engine.Run()
			logger.Printf("broker", "Closed %s (%v)", engine, engine.Error())
			agt.mu.Lock()
			delete(agt.handlers, h.engine)
			agt.mu.Unlock()
		}()
	}
}
