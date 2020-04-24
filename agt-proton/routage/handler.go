package main

import (
	"jf/AMQP/agt-proton/util"
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

// HandleMessagingEvent handles an event, called in the handler goroutine.
func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {
	switch t {
	case proton.MStart:
		util.LogEvent(t, e)
	case proton.MLinkOpening:
		util.LogEvent(t, e)
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
		util.LogEvent(t, e)
	case proton.MSendable:
		util.LogEvent(t, e)
	case proton.MMessage:
		util.LogEvent(t, e)
		h.recvMsg(e)
	case proton.MAccepted:
		util.LogEvent(t, e)
		n := h.q.Pop()
		logger.Printf("sendMsg", "message sent from  %s(%d): accepted", addr, n)
	//AutoSettle suffisant
	// case proton.MSettled:
	// 	util.LogEvent(t, e)
	// 	e.Delivery().Settle()
	default:
		util.LogEvent(t, e)
	}
}

// startReceiver creates a receiver and a goroutine for its run() method.
func (h *handler) recvMsg(e proton.Event) {

	if msg, err := e.Delivery().Message(); err == nil {
		n := h.q.Add(msg)
		logger.Printf("broker", "message queued in %s(%d)", addr, n)
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
