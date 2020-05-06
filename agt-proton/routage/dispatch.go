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
	sender := h.session.Sender("sendto:" + host)
	logger.Debugf("forward", "sender: state=%v", sender.State())
	sender.SetSndSettleMode(proton.SndSettled)
	sender.Target().SetAddress(host)
	h.senders[sender] = msg
	sender.Open()
}

func (h *handler) sendMsg(sender proton.Link, msg amqp.Message) {
	logger.Debugf("sendMsg", "sending on link %v", sender)
	delivery, err := sender.Send(msg)
	if err == nil {
		// delivery.Settle() TODO utile ?
		logger.Debugf("sendMsg", "%#v", msg)
		logger.Printf("sendMsg", "message sent to %s", delivery.Link().Name())
	} else {
		logger.Printf("sendMsg", "error: %v", err)
	}
	sender.Close()
	// delete(h.senders, sender)TODO
}
