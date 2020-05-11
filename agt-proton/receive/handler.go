/*
PN_TRACE_FRM=1 go run -tags debug ./receive amqp://localhost:5672/queue1
*/
package main

import (
	"fmt"
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"

	"qpid.apache.org/proton"
)

type handler struct {
	topic string
}

func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {
	logger.Debugf("handler", "[event] type=%v", t)
	switch t {
	case proton.MStart:
		e.Connection().SetContainer(util.GetName())
		e.Connection().Open()
		/*
			[0xce0220]: AMQP:FRAME:  -> AMQP
			[0xce0220]: AMQP:FRAME:0 -> @open(16) [container-id="jfContainer", channel-max=32767]
			[0xce0220]: AMQP:FRAME:  <- AMQP
			[0xce0220]: AMQP:FRAME:0 <- @open(16) [container-id="", channel-max=32767]
		*/
	case proton.MConnectionOpened:
		session, err := e.Connection().Session()
		if err != nil {
			logger.Printf("handler", "error opening session: %v", err)
			e.Connection().Close()
			break
		}
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
	case proton.MLinkOpened:
		e.Link().Flow(1)
		logger.Debugf("handler", "receiver: state=%v", e.Link().State())
		/*
			[0x24d3250]: AMQP:FRAME:0 -> @flow(19) [next-incoming-id=0, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=0, link-credit=1, drain=false]
			[0x24d3250]: AMQP:FRAME:0 <- @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"7", message-format=0] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
		*/
	case proton.MMessage:
		if msg, err := e.Delivery().Message(); err == nil {
			logger.Printf("handler", "Message: %v\n", e.Delivery().HasMessage())
			fmt.Println(msg.Body())
			e.Delivery().Accept()
		} else {
			logger.Printf("handler", "Error: %v", err)
		}
		/*
			[0xf14260]: AMQP:FRAME:0 -> @disposition(21) [role=true, first=0, settled=true, state=@accepted(36) []]
		*/
		e.Link().Close()
	case proton.MLinkClosed:
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
	case proton.MConnectionClosed:
		logger.Debugf("handler", "connection closed: %v", e.Connection().String())
		/*
			[0xd5c490]:   IO:FRAME:  <- EOS
		*/
	case proton.MDisconnected:
		logger.Debugf("handler", "Disconnected: %v (%v)", e.Connection(), e.Connection().Error())

	}
}
