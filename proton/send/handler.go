package main

import (
	"fmt"
	"jf/AMQP/logger"

	"qpid.apache.org/amqp"
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
			[0xe05220]: AMQP:FRAME:  -> AMQP
			[0xe05220]: AMQP:FRAME:0 -> @open(16) [container-id="jfContainer", channel-max=32767]
			[0xe05220]: AMQP:FRAME:  <- AMQP
			[0xe05220]: AMQP:FRAME:0 <- @open(16) [container-id="", channel-max=32767]
		*/
	case proton.MConnectionOpened:
		logger.Debugf("handler", "Handle: %v", t)
		session, err := e.Connection().Session()
		fatalIf(err)
		logger.Debugf("handler", "session: state=%v", session.State())
		session.Open()
		/*
			[0xe05220]: AMQP:FRAME:0 -> @begin(17) [next-outgoing-id=0, incoming-window=2147483647, outgoing-window=2147483647]
			[0xe05220]: AMQP:FRAME:0 <- @begin(17) [remote-channel=0, next-outgoing-id=0, incoming-window=2147483647, outgoing-window=2147483647]
		*/
		logger.Debugf("handler", "session: state=%v", session.State())
	case proton.MSessionOpened:
		logger.Debugf("handler", "session: state=%v", e.Session().State())
		sender := e.Session().Sender("sender")
		logger.Debugf("handler", "sender: state=%v", sender.State())
		sender.SetSndSettleMode(proton.SndUnsettled)
		sender.Target().SetAddress(h.topic)
		sender.Open()
		/*
			[0xe05220]: AMQP:FRAME:0 -> @attach(18) [name="sender", handle=0, role=false, snd-settle-mode=2, rcv-settle-mode=0, source=@source(40) [durable=0, timeout=0, dynamic=false], target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
			[0xe05220]: AMQP:FRAME:0 <- @attach(18) [name="sender", handle=0, role=true, snd-settle-mode=2, rcv-settle-mode=0, target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
			[0xe05220]: AMQP:FRAME:0 <- @flow(19) [next-incoming-id=0, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=0, link-credit=100, drain=false]
		*/
	case proton.MLinkOpened:
		logger.Debugf("handler", "sender: state=%v", e.Link().State())
		sendMsg(e.Link())
		/*
			[0x2438220]: AMQP:FRAME:0 -> @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"1", message-format=0] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
			[0x2438220]: AMQP:FRAME:0 <- @flow(19) [next-incoming-id=1, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=1, link-credit=100, drain=false]
			[0x2438220]: AMQP:FRAME:0 <- @disposition(21) [role=true, first=0, settled=true, state=@accepted(36) []]
		*/
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
		logger.Debugf("handler", "Disconnected : %v (%v)", e.Connection(), e.Connection().Error())
	case proton.MAccepted:
		logger.Debugf("handler", "Delivery accepted: settled=%v", e.Delivery().Settled())
	case proton.MSettled:
		logger.Debugf("handler", "Delivery settled: settled=%v", e.Delivery().Settled())
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

func sendMsg(sender proton.Link) error {
	logger.Debugf("sendMsg", "sending on link %v", sender)

	m := amqp.NewMessage()
	body := fmt.Sprintf("message body")
	m.Marshal(body)
	delivery, err := sender.Send(m)
	logger.Debugf("sendMsg", "delivery: %#v", delivery.Local())
	if err == nil {
		logger.Debugf("sendMsg", "%#v", m)
		logger.Printf("sendMsg", "message sent")
	} else {
		logger.Printf("sendMsg", "error: %v", err)
	}
	return err
}
