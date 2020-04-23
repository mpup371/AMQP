/*
PN_TRACE_FRM=1 go run -tags debug .

*/

package main

import (
	"flag"
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
	queues queues
}

func main() {
	flag.Parse()

	b := &broker{} //TODO queues
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
		engine, err := proton.NewEngine(conn, adapter)
		if err != nil {
			logger.Printf("broker", "Connection error: %v", err)
			continue
		}
		engine.Server() // Enable server-side protocol negotiation.
		logger.Printf("broker", "Accepted connection %s", engine)
		go func() { // Start goroutine to run the engine event loop
			engine.Run()
			logger.Printf("broker", "Closed %s", engine)
		}()
	}
}

// handler handles AMQP events. There is one handler per connection.  The
// handler does not need to be concurrent-safe as proton.Engine will serialize
// all calls to the handler. We use channels to communicate between the handler
// goroutine and other goroutines sending and receiving messages.
type handler struct {
	queues *queues
	// receivers map[proton.Link]*receiver
	// senders   map[proton.Link]*sender
	// injecter  proton.Injecter
}

func newHandler(queues *queues) *handler {
	return &handler{
		queues: queues,
		// receivers: make(map[proton.Link]*receiver),
		// senders:   make(map[proton.Link]*sender),
	}
}

// HandleMessagingEvent handles an event, called in the handler goroutine.
func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {
	switch t {

	case proton.MStart:
		logger.Debugf("handler", "Handle: %v", t)

		// h.injecter = e.Injecter()

	case proton.MLinkOpening:
		logger.Debugf("handler", "Handle: %v", t)

		if e.Link().IsReceiver() {
			e.Link().Flow(1) // Give credit to fill the buffer to capacity.
		} else {
			h.startSender(e)
		}

	case proton.MLinkClosed:
		logger.Debugf("handler", "Handle: %v", t)

	case proton.MSendable:
		logger.Debugf("handler", "Handle: %v", t)

	case proton.MMessage:
		logger.Debugf("handler", "Handle: %v", t)
		m, err := e.Delivery().Message() // Message() must be called while handling the MMessage event.
		if err != nil {
			proton.CloseError(e.Link(), err)
			break
		}

		logger.Debugf("broker", "link %s received %#v", e.Link(), m)
		e.Delivery().Accept()
		logger.Debugf("handler", "Delivery accepted, settled=%#v", e.Delivery().Settled())

	case proton.MConnectionClosed, proton.MDisconnected:
		logger.Debugf("handler", "Handle: %v", t)

	default:
		logger.Debugf("handler", "default: %v", t)
	}
}

// link has some common data and methods that are used by the sender and receiver types.
//
// An active link is represented by a sender or receiver value and a goroutine
// running its run() method. The run() method communicates with the handler via
// channels.
type link struct {
	l proton.Link
	q queue
	h *handler
}

func makeLink(l proton.Link, q queue, h *handler) link {
	lnk := link{l: l, q: q, h: h}
	return lnk
}

// startReceiver creates a receiver and a goroutine for its run() method.
func (h *handler) startReceiver(e proton.Event) {
	logger.Debugf("broker", "push message to %s", e.Link().RemoteTarget().Address())
	// TODO q := h.queues.Get(e.Link().RemoteTarget().Address())
	logger.Debugf("broker", "accept delivery %v", e.Delivery())
	e.Delivery().Accept() // Accept the delivery
}

// startSender creates a sender and starts a goroutine for sender.run()
func (h *handler) startSender(e proton.Event) {
	logger.Debugf("broker", "get message from %s", e.Link().RemoteSource().Address())
	//TODO  q := h.queues.Get(e.Link().RemoteSource().Address())

}

// // sendable signals that the sender has credit, it does not block.
// // sender.credit has capacity 1, if it is already full we carry on.
// func (s *sender) sendable() {
// 	select { // Non-blocking
// 	case s.credit <- struct{}{}:
// 	default:
// 	}
// }

// sendOne runs in the handler goroutine. It sends a single message.
// func sendOne(m amqp.Message) error {
// 	delivery, err := s.l.Send(m)
// 	logger.Debugf("sendOne", "delivery: %#v", delivery)
// 	if err == nil {
// 		// //TODO pas unreliable
// 		// delivery.Settle() // Pre-settled, unreliable.
// 		logger.Debugf("sendOne", "link %s sent %#v", s.l, m)
// 	} else {
// 		logger.Printf("sendOne", "error: ", err)
// 		q.PutBack(m) // Put the message back on the queue, don't block
// 	}
// 	return err
// }

// Use a buffered channel as a very simple queue.
type queue *struct{}

// Concurrent-safe map of queues.
type queues struct {
	m    map[string]queue
	lock sync.Mutex
}

func makeQueues(queueSize int) queues {
	return queues{m: make(map[string]queue)}
}

// Create a queue if not found.
func (qs *queues) Get(name string) queue {
	qs.lock.Lock()
	defer qs.lock.Unlock()
	q, ok := qs.m[name]
	if !ok {
		qs.m[name] = q
	}
	return q
}
