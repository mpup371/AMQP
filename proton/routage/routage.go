/*
PN_TRACE_FRM=1 go run -tags debug .

*/

package main

import (
	"flag"
	"fmt"
	"jf/AMQP/logger"
	"log"
	"net"
	"os"
	"time"

	"qpid.apache.org/proton"
)

var addr = flag.String("addr", ":amqp", "Listening address")
var credit = flag.Int("credit", 100, "Receiver credit window")
var qsize = flag.Int("qsize", 1000, "Max queue size")

// State for the broker
type broker struct {
	queues queues
}

var name string

func init() {

}

func main() {
	flag.Parse()
	host, err := os.Hostname()
	if err == nil {
		name = fmt.Sprintf("routage(%s)[%d]", host, os.Getegid())
	} else {
		name = fmt.Sprintf("routage[%d]", os.Getegid())
	}
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
		engine.Connection().SetContainer(name)
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

/*TODO
voir comment gérer les timeout ici:
[0x1dea220]: AMQP:FRAME:0 -> @attach(18) [name="receiver", handle=0, role=true, snd-settle-mode=2, rcv-settle-mode=0,
 source=@source(40) [address="queue1", durable=0, timeout=0, dynamic=false],
  target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
*/

func logEvent(t proton.MessagingEvent, e proton.Event) {
	logger.Debugf("event", "type=%v", t)
}

// HandleMessagingEvent handles an event, called in the handler goroutine.
func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {
	switch t {
	case proton.MStart:
		logEvent(t, e)
	case proton.MLinkOpening:
		logEvent(t, e)
		if e.Link().IsReceiver() {
			addr := e.Link().RemoteTarget().Address()
			logger.Debugf("broker", "push message to %s", addr)
			h.q = h.queues.Get(addr)
			e.Link().Flow(1) // Give credit to fill the buffer to capacity.
		} else {
			addr := e.Link().RemoteSource().Address()
			logger.Debugf("broker", "pull message from %s", addr)
			h.q = h.queues.Get(addr)
			h.sendMsg(e.Link())
		}
	case proton.MLinkClosed:
		logEvent(t, e)
	case proton.MSendable:
		logEvent(t, e)
	case proton.MMessage:
		logEvent(t, e)
		h.recvMsg(e)
	case proton.MAccepted:
		logEvent(t, e)
		n := h.q.Pop()
		logger.Printf("sendMsg", "message sent from  %s(%d): accepted", addr, n)
	//AutoSettle suffisant
	// case proton.MSettled:
	// 	logEvent(t, e)
	// 	e.Delivery().Settle()
	default:
		logEvent(t, e)
	}
}

// startReceiver creates a receiver and a goroutine for its run() method.
func (h *handler) recvMsg(e proton.Event) {

	if msg, err := e.Delivery().Message(); err == nil {
		n := h.q.Add(msg)
		logger.Printf("broker", "message queued in %v(%d)", addr, n)
		e.Delivery().Accept() // Accept the delivery
	} else {
		logger.Printf("broker", "error reading message: %v", err)
		e.Delivery().Release(true) // Accept the delivery
	}
}

func (h *handler) sendMsg(sender proton.Link) {
	logger.Debugf("sendMsg", "sending on link %v", sender)
	msg := h.q.Peek()

	for ; msg == nil; msg = h.q.Peek() {
		time.Sleep(1 * time.Second)
	}

	if _, err := sender.Send(msg); err == nil {
		logger.Debugf("sendMsg", "msg=%#v", msg)
	} else {
		logger.Printf("sendMsg", "error: %v", err)
	}
}
