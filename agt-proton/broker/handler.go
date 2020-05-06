package main

import (
	"jf/AMQP/logger"
	"time"

	"qpid.apache.org/proton"
)

/*TODO
voir comment gérer les timeout ici:
[0x1dea220]: AMQP:FRAME:0 -> @attach(18) [name="receiver", handle=0, role=true, snd-settle-mode=2, rcv-settle-mode=0,
 source=@source(40) [address="queue1", durable=0, timeout=0, dynamic=false],
  target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
*/

// handler handles AMQP events. There is one handler per connection.  The
// handler does not need to be concurrent-safe as proton.Engine will serialize
// all calls to the handler. We use channels to communicate between the handler
// goroutine and other goroutines sending and receiving messages.
type handler struct {
	queues     *queues
	links      map[proton.Link]*link
	engine     string
	connection string
	container  string
	lastEvent  time.Time
}

type link struct {
	topic        string
	rw           string
	q            *queue // link Source/Target
	creationDate time.Time
}

func newHandler(queues *queues) *handler {
	return &handler{queues: queues,
		links: make(map[proton.Link]*link)}
}

// HandleMessagingEvent handles an event, called in the handler goroutine.
func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {
	logger.Debugf("event", "[%s] type=%v", h.engine, t)
	h.lastEvent = time.Now()
	switch t {
	case proton.MConnectionOpening:
		h.container = e.Connection().RemoteContainer()
	// The peer initiates the opening of the link.
	case proton.MLinkOpening:
		logger.Debugf("broker", "RemoteSndSettleMode=%v, RemoteRcvSettleMode=%v, State=%v",
			e.Link().RemoteSndSettleMode(), e.Link().RemoteRcvSettleMode(),
			e.Link().State())
		h.links[e.Link()] = &link{}
	case proton.MLinkClosing:
		delete(h.links, e.Link())
	case proton.MLinkOpened:
		l, ok := h.links[e.Link()]
		l.creationDate = time.Now()
		if !ok {
			logger.Printf("broker", "Link not found %v", e.Link().Name())
			break
		}
		if e.Link().IsReceiver() {
			topic := e.Link().RemoteTarget().Address()
			logger.Debugf("broker", "push message to %s", topic)
			l.topic = topic
			l.rw = "write"
			l.q = h.queues.Get(topic)
			e.Link().Flow(1) // Give credit to fill the buffer to capacity.
		} else {
			topic := e.Link().RemoteSource().Address()
			logger.Debugf("broker", "pull message from %s", topic)
			l.topic = topic
			l.rw = "read"
			if topic == "admin" {
				l.q = nil
			} else {
				l.q = h.queues.Get(topic)
			}
		}
	case proton.MSendable:
		l, ok := h.links[e.Link()]
		if !ok {
			logger.Printf("broker", "Link not found %v", e.Link().Name())
			break
		}
		topic := e.Link().RemoteSource().Address()
		if topic == "admin" {
			h.sendAdmin(e.Link())
		} else {
			h.sendMsg(e.Link(), l)
		}
	case proton.MMessage:
		l, ok := h.links[e.Link()]
		if !ok {
			logger.Printf("broker", "Link not found %v", e.Link().Name())
			break
		}
		h.recvMsg(e, l)
	case proton.MAccepted:
		l, ok := h.links[e.Link()]
		if !ok {
			logger.Printf("broker", "Link not found %v", e.Link().Name())
			break
		}
		if l.q != nil {
			n := l.q.Pop()
			logger.Printf(h.engine, "message sent from %s(%d): accepted", l.topic, n)
		}
		//TODO suppression fichier
	case proton.MRejected:
		l, ok := h.links[e.Link()]
		if !ok {
			logger.Printf("broker", "Link not found %v", e.Link().Name())
			break
		}
		if l.q != nil {
			n := l.q.Pop()
			logger.Printf(h.engine, "message sent from %s(%d): rejected", l.topic, n)
		}
		//TODO suppression fichier
	}
}

// startReceiver creates a receiver and a goroutine for its run() method.
func (h *handler) recvMsg(e proton.Event, l *link) {
	if msg, err := e.Delivery().Message(); err == nil {
		n := l.q.Add(msg)
		logger.Printf(h.engine, "message queued in %s(%d)", l.topic, n)
		logger.Debugf("broker", "Delivery settled=%v", e.Delivery().Settled())
		if !e.Delivery().Settled() {
			e.Delivery().Accept() // Accept the delivery
		}
	} else {
		logger.Printf(h.engine, "error reading message: %v", err)
		e.Delivery().Release(true)
	}
}

// TODO goroutine
func (h *handler) sendMsg(sender proton.Link, l *link) {
	logger.Debugf("sendMsg", "waiting to send on link %v", sender)

	msg := l.q.Peek()
	for ; msg == nil; msg = l.q.Peek() {
		time.Sleep(1 * time.Second)
	}

	if _, err := sender.Send(msg); err == nil {
		logger.Debugf("sendMsg", " send msg=%#v", msg)
	} else {
		logger.Printf(h.engine, "send error: %v", err)
	}
}
