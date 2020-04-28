/*
PN_TRACE_FRM=1 go run -tags debug .

*/

package main

import (
	"flag"
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"
	"log"
	"net"

	"qpid.apache.org/proton"
)

var addr = flag.String("addr", ":amqp", "Listening address")
var credit = flag.Int("credit", 100, "Receiver credit window")
var qsize = flag.Int("qsize", 1000, "Max queue size")

// State for the broker
type broker struct {
	queues queues
}

func main() {
	flag.Parse()

	b := &broker{makeQueues()}
	if err := b.run(); err != nil {
		log.Fatal(err)
	}
}

// Listens for connections and starts a proton.Engine for each one.
func (b *broker) run() error {
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

		adapter := proton.NewMessagingAdapter(newHandler(&b.queues))
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

		go func() { // Start goroutine to run the engine event loop
			engine.Run()
			logger.Printf("broker", "Closed %s (%v)", engine, engine.Error())
		}()
	}
}

// handler handles AMQP events. There is one handler per connection.  The
// handler does not need to be concurrent-safe as proton.Engine will serialize
// all calls to the handler. We use channels to communicate between the handler
// goroutine and other goroutines sending and receiving messages.
type handler struct {
	queues *queues
	q      *queue // link Source/Target
}

func newHandler(queues *queues) *handler {
	return &handler{
		queues: queues,
	}
}
