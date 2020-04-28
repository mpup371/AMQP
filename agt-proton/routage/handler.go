/*
PN_TRACE_FRM=1 go run -tags debug ./receive amqp://localhost:5672/queue1
*/
package main

import (
	"fmt"
	"jf/AMQP/agt-proton/attributes"
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"

	"qpid.apache.org/amqp"
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
		e.Connection().Open()
	case proton.MConnectionOpened:
		session, err := e.Connection().Session()
		if err != nil {
			logger.Printf("handler", "error opening session: %v", err)
			e.Connection().Close()
			break
		}
		logger.Debugf("handler", "session: state=%v", session.State())
		session.Open()
		logger.Debugf("handler", "session: state=%v", session.State())
	case proton.MSessionOpened:
		logger.Debugf("handler", "session: state=%v", e.Session().State())
		receiver := e.Session().Receiver("receiver")
		logger.Debugf("handler", "receiver: state=%v", receiver.State())
		receiver.SetRcvSettleMode(proton.RcvFirst)
		receiver.Source().SetAddress(h.topic)
		receiver.Open()
	case proton.MLinkOpening:
		e.Link().Flow(1)
	case proton.MLinkOpened:
		logger.Debugf("handler", "receiver: state=%v", e.Link().State())
	case proton.MMessage:
		if msg, err := e.Delivery().Message(); err == nil {
			logger.Printf("handler", "Message: body=%v", msg.Body())
			if err := persist(msg); err == nil {
				e.Delivery().Accept()
			} else {
				logger.Debugf("handler", "Error persist attributes: %v", err)
				e.Delivery().Reject()
			}
		} else {
			logger.Printf("handler", "Error: %v", err)
		}
		e.Link().Close()
	case proton.MLinkClosed:
		e.Session().Close()
	case proton.MSessionClosed:
		e.Connection().Close()
	case proton.MConnectionClosed:
		logger.Debugf("handler", "connection closed: %v", e.Connection().String())
	case proton.MDisconnected:
		logger.Debugf("handler", "Disconnected: %v (%v)", e.Connection(), e.Connection().Error())
	}
}

//TODO rajouter la taille du body dans le message (delivery ?)
func persist(msg amqp.Message) error {
	var buf []byte = make([]byte, 0)
	msg.Unmarshal(&buf)
	logger.Debugf("persist", "buffer=%s", buf)
	attr, err := attributes.Unmarshal(buf)
	logger.Debugf("persist", "map=%s", attr.Marshall())
	if err != nil {
		logger.Debugf("persist", "Error reading attributes: %v", err)
		return err
	}

	if path, ok := attr.Get("agt.routage.file"); ok {
		logger.Debugf("persist", "path=%s", path)
		return attributes.SetAttributes(path, attr)
	}

	logger.Debugf("persist", "Error : path not found in attributes: %v", attr)
	return fmt.Errorf("path not found in attributes")

}
