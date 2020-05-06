package main

import (
	"fmt"
	"jf/AMQP/agt-proton/attributes"
	"jf/AMQP/logger"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

//TODO clefs de routage
func (h *handler) dispatch(attr attributes.Attributes) {
	user, host, err := attr.GetRecipient()
	if err != nil {
		logger.Printf("dispatch", "%v", err)
		return
	}
	logger.Printf("dispatch", "sending message to %s@%s", user, host)
	msg := amqp.NewMessage()
	msg.SetBody(attr.Marshall())
	for i := 0; i < 3; i++ {
		h.forward(fmt.Sprintf("%s_%d", host, i), msg)
	}
}

func (h *handler) forward(host string, msg amqp.Message) {
	var sender proton.Link
	events := make(chan proton.MessagingEvent)

	err := h.engine.InjectWait(func() error {
		if h.session.State().Has(proton.SRemoteClosed | proton.SLocalClosed) {
			return fmt.Errorf("session closed: %s", h.session.String())
		}
		sender = h.session.Sender("sendto:" + host)
		logger.Debugf("dispatch", "sender: state=%v", sender.State())
		sender.SetSndSettleMode(proton.SndSettled)
		sender.Target().SetAddress(host)
		sender.Open()
		h.senders[sender] = events //TODO Lock ou pas goroutine
		return nil
	})
	if err != nil {
		logger.Printf("dispatch", "Error inject: %v", err)
		return
	}

	evt := <-events
	if evt == 0 {
		logger.Printf("dispatch", "channel closed: %v", sender.Name())
	}
	logger.Debugf("dispatch", "ready to send: %v", evt)
	h.engine.Inject(func() {
		if sender.State().Has(proton.SRemoteClosed | proton.SLocalClosed) {
			logger.Printf("dispatch", "sender closed: %v", sender.Name())
		}
		logger.Debugf("sendMsg", "sending on link %v", sender)
		delivery, err := sender.Send(msg)
		if err == nil {
			delivery.Settle()
			logger.Debugf("sendMsg", "%#v", msg)
			logger.Printf("sendMsg", "message sent to %s", host)
		} else {
			logger.Printf("sendMsg", "error: %v", err)
		}
		sender.Close()
	})
}
