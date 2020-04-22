/*
PN_TRACE_FRM=1 go run -tags debug ./receive amqp://localhost:5672/queue1
*/
package main

import (
	"jf/AMQP/logger"

	"qpid.apache.org/proton"
)

type handler struct {
	topic string
}

func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {

	switch t {
	case proton.MStart:
		logger.Debugf("handler", "Handle: %v", t)
		e.Connection().SetContainer("jfContainer")
		e.Connection().Open()
		/*
			[0xce0220]: AMQP:FRAME:  -> AMQP
			[0xce0220]: AMQP:FRAME:0 -> @open(16) [container-id="jfContainer", channel-max=32767]
			[0xce0220]: AMQP:FRAME:  <- AMQP
			[0xce0220]: AMQP:FRAME:0 <- @open(16) [container-id="", channel-max=32767]
		*/
	case proton.MConnectionOpened:
		logger.Debugf("handler", "Handle: %v", t)
		session, err := e.Connection().Session()
		fatalIf(err)
		logger.Debugf("handler", "session: state=%v", session.State())
		session.Open()
		/*
			[0xce0220]: AMQP:FRAME:0 -> @begin(17) [next-outgoing-id=0, incoming-window=2147483647, outgoing-window=2147483647]
			[0xce0220]: AMQP:FRAME:0 <- @begin(17) [remote-channel=0, next-outgoing-id=0, incoming-window=2147483647, outgoing-window=2147483647]
		*/
		logger.Debugf("handler", "session: state=%v", session.State())
	case proton.MSessionOpened:
		logger.Debugf("handler", "session: state=%v", e.Session().State())
		receiver := e.Session().Receiver("receiver")
		logger.Debugf("handler", "receiver: state=%v", receiver.State())
		receiver.SetRcvSettleMode(proton.RcvFirst)
		receiver.Source().SetAddress(h.topic)
		receiver.Open()
		/*
			[0x1554220]: AMQP:FRAME:0 -> @attach(18) [name="receiver", handle=0, role=true, snd-settle-mode=2, rcv-settle-mode=0, source=@source(40) [durable=0, timeout=0, dynamic=false], target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
			[0x1554220]: AMQP:FRAME:0 <- @attach(18) [name="receiver", handle=0, role=false, snd-settle-mode=2, rcv-settle-mode=0, target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
		*/
	case proton.MLinkOpening:
		logger.Debugf("handler", "Handle: %v", t)
		e.Link().Flow(1)
		/*
			[0x15ab220]: AMQP:FRAME:0 -> @flow(19) [next-incoming-id=0, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=0, link-credit=1, drain=false]
			[0x15ab220]: AMQP:FRAME:0 <- @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"1", message-format=0] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
		*/
	case proton.MLinkOpened:
		logger.Debugf("handler", "receiver: state=%v", e.Link().State())
		/*
			[0x24d3250]: AMQP:FRAME:0 -> @flow(19) [next-incoming-id=0, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=0, link-credit=1, drain=false]
			[0x24d3250]: AMQP:FRAME:0 <- @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"7", message-format=0] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
		*/
	case proton.MMessage:
		if msg, err := e.Delivery().Message(); err == nil {
			logger.Printf("handler", "Message: body=%v", msg.Body())
			e.Delivery().Accept()
		} else {
			logger.Printf("handler", "Error: %v", err)
		}
		/*
			[0xf14260]: AMQP:FRAME:0 -> @disposition(21) [role=true, first=0, settled=true, state=@accepted(36) []]
		*/
		//TODO mode bloquant
		e.Link().Close()
		e.Session().Close()
		e.Connection().Close()
	case proton.MLinkClosed:
		logger.Debugf("handler", "link closed")
		e.Session().Close()
		/*
			[0xd5c490]: AMQP:FRAME:0 -> @end(23) []
			[0xd5c490]: AMQP:FRAME:0 <- @end(23) []
		*/
	case proton.MSessionClosed:
		e.Connection().Close()
		/*
			[0x146d4b0]: AMQP:FRAME:0 -> @close(24) []
			[0x146d4b0]:   IO:FRAME:  -> EOS
			[0x146d4b0]: AMQP:FRAME:0 <- @close(24) []
			[0x146d4b0]:   IO:FRAME:  <- EOS
		*/
	case proton.MSendable:
		logger.Debugf("handler", "Sendable, credit=%d", e.Link().Credit())
		/*
			[0xe31280]: AMQP:FRAME:0 -> @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"1", message-format=0, settled=true] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
		*/
	case proton.MConnectionClosed:
		logger.Debugf("handler", "connection closed: %v", e.Connection().String())
		/*
			[0xd5c490]:   IO:FRAME:  <- EOS
		*/
	case proton.MDisconnected:
		logger.Debugf("handler", "Disconnected: %v (%v)", e.Connection(), e.Connection().Error())
	case proton.MAccepted:
		logger.Debugf("handler", "Accepted: settled=%v", e.Delivery().Settled())
	case proton.MSettled:
		logger.Debugf("handler", "Settled: settled=%v", e.Delivery().Settled())
		//TODO répéter si non accepté ou timeout
		e.Link().Close()
		/*
			[0x146d4b0]: AMQP:FRAME:0 -> @detach(22) [handle=0, closed=true]
			[0x146d4b0]: AMQP:FRAME:0 <- @detach(22) [handle=0, closed=true]
		*/
	default:
		logger.Debugf("handler", "default: %v", t)
	}
}

// func rcvMsg(receiver proton.Link) error {
// 	logger.Debugf("rcvMsg", "receiving on link %v", receiver)

// 	delivery, err := receiver.
// 	if err == nil {
// 		logger.Debugf("sendMsg", "%#v", delivery)
// 		// delivery.Settle() // Pre-settled, unreliable.
// 		logger.Debugf("sendMsg", "sent: %#v", m)
// 	} else {
// 		logger.Debugf("sendMsg", "error: %v", err)
// 	}
// 	return err
// }
