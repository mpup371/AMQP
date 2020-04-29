package main

import (
	"fmt"
	"jf/AMQP/agt-proton/attributes"
	"jf/AMQP/logger"

	"qpid.apache.org/proton"
)

//TODO clefs de routage
func (h *handler) dispatch(attr attributes.Attributes, events chan proton.MessagingEvent) {
	user, host, err := attr.GetRecipient()
	if err != nil {
		logger.Printf("dispatch", "%v", err)
		return
	}
	logger.Printf("dispatch", "sending message to %s@%s", user, host)
	var sender proton.Link
	err = h.engine.InjectWait(func() error {
		if h.session.State().Has(proton.SRemoteClosed | proton.SLocalClosed) {
			return fmt.Errorf("session closed")
		}
		sender = h.session.Sender("sendto:" + host)
		logger.Debugf("dispatch", "sender: state=%v", sender.State())
		sender.SetSndSettleMode(proton.SndSettled)
		sender.Target().SetAddress(host)
		sender.Open()
		h.senders[sender] = events
		return nil
	})
	if err != nil {
		logger.Printf("dispatch", "Error inject: %v", err)
		return
	}
	evt := <-events
	logger.Debugf("dispatch", "ready to send: %v", evt)
}
