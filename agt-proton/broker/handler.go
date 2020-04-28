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
	util.LogEvent(t, e)
	switch t {
	case proton.MLinkOpening:
		logger.Debugf("broker", "RemoteSndSettleMode=%v, RemoteRcvSettleMode=%v, State=%v",
			e.Link().RemoteSndSettleMode(), e.Link().RemoteRcvSettleMode(),
			e.Link().State())
	case proton.MLinkOpened:
		if e.Link().IsReceiver() {
			addr := e.Link().RemoteTarget().Address()
			logger.Debugf("broker", "push message to %s", addr)
			h.q = h.queues.Get(addr)
			e.Link().Flow(1) // Give credit to fill the buffer to capacity.
		} else {
			addr := e.Link().RemoteSource().Address()
			logger.Debugf("broker", "pull message from %s", addr)
			if addr == "admin" {
				h.q = nil
				h.sendAdmin(e.Link())
			} else {
				h.q = h.queues.Get(addr)
				h.sendMsg(e.Link())
			}
		}
	case proton.MMessage:
		h.recvMsg(e)
	case proton.MAccepted:
		if h.q != nil {
			n := h.q.Pop()
			logger.Printf("sendMsg", "message sent from %s(%d): accepted", addr, n)
		}
	}
}

// startReceiver creates a receiver and a goroutine for its run() method.
func (h *handler) recvMsg(e proton.Event) {

	if msg, err := e.Delivery().Message(); err == nil {
		n := h.q.Add(msg)
		logger.Printf("broker", "message queued in %s(%d)", *addr, n)
		logger.Debugf("broker", "Delivery settled=%v", e.Delivery().Settled())
		if !e.Delivery().Settled() {
			e.Delivery().Accept() // Accept the delivery
		}
	} else {
		logger.Printf("broker", "error reading message: %v", err)
		e.Delivery().Release(true)
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
