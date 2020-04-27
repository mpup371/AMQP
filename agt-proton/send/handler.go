package main

import (
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"

	"qpid.apache.org/proton"
)

type handler struct {
	topic string
}

func (h *handler) HandleMessagingEvent(t proton.MessagingEvent, e proton.Event) {
	util.LogEvent(t, e)
	switch t {
	case proton.MStart:
		e.Connection().SetContainer(util.GetName())
		// e.Connection().Transport().SetIdleTimeout(5 * time.Second) //TODO paramétrable // n'a pas l'air très efficace
		e.Connection().Open()
		/*
			[0xe05220]: AMQP:FRAME:  -> AMQP
			[0xe05220]: AMQP:FRAME:0 -> @open(16) [container-id="jfContainer", channel-max=32767]
			[0xe05220]: AMQP:FRAME:  <- AMQP
			[0xe05220]: AMQP:FRAME:0 <- @open(16) [container-id="", channel-max=32767]
		*/
	case proton.MConnectionOpened:
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
		sender.SetSndSettleMode(proton.SndSettled)
		sender.Target().SetAddress(h.topic)
		// sender.Target().SetTimeout(5 * time.Second) pas très utile
		sender.Open()
		/*
			[0xe05220]: AMQP:FRAME:0 -> @attach(18) [name="sender", handle=0, role=false, snd-settle-mode=2, rcv-settle-mode=0, source=@source(40) [durable=0, timeout=0, dynamic=false], target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
			[0xe05220]: AMQP:FRAME:0 <- @attach(18) [name="sender", handle=0, role=true, snd-settle-mode=2, rcv-settle-mode=0, target=@target(41) [durable=0, timeout=0, dynamic=false], initial-delivery-count=0, max-message-size=0]
			[0xe05220]: AMQP:FRAME:0 <- @flow(19) [next-incoming-id=0, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=0, link-credit=100, drain=false]
		*/
	case proton.MLinkOpened:
		logger.Debugf("handler", "sender: state=%v", e.Link().State())
		sendMsg(e.Link())
		e.Link().Close()
		/*
			[0x2438220]: AMQP:FRAME:0 -> @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"1", message-format=0] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
			[0x2438220]: AMQP:FRAME:0 <- @flow(19) [next-incoming-id=1, incoming-window=2147483647, next-outgoing-id=0, outgoing-window=2147483647, handle=0, delivery-count=1, link-credit=100, drain=false]
			[0x2438220]: AMQP:FRAME:0 <- @disposition(21) [role=true, first=0, settled=true, state=@accepted(36) []]
		*/
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
	// case proton.MSendable:
	// logger.Debugf("handler", "Sendable, credit=%d", e.Link().Credit())
	/*
		[0xe31280]: AMQP:FRAME:0 -> @transfer(20) [handle=0, delivery-id=0, delivery-tag=b"1", message-format=0, settled=true] (25) "\x00SpE\x00SsE\x00Sw\xa1\x0cmessage body"
	*/
	case proton.MConnectionClosed:
		logger.Debugf("handler", "connection closed: %v", e.Connection().String())
	/*
		[0xd5c490]:   IO:FRAME:  <- EOS
	*/
	case proton.MSettled:
		logger.Debugf("handler", "Delivery settled: settled=%v", e.Delivery().Settled())
		/*
			[0x146d4b0]: AMQP:FRAME:0 -> @detach(22) [handle=0, closed=true]
			[0x146d4b0]: AMQP:FRAME:0 <- @detach(22) [handle=0, closed=true]
		*/
	}
}

func sendMsg(sender proton.Link) error {
	logger.Debugf("sendMsg", "sending on link %v", sender)
	m := newMessage()
	delivery, err := sender.Send(m)
	if err == nil {
		delivery.Settle()
		logger.Debugf("sendMsg", "%#v", m)
		logger.Printf("sendMsg", "message sent")
	} else {
		logger.Printf("sendMsg", "error: %v", err)
	}
	return err
}
