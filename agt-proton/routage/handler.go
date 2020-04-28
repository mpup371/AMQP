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
		route(e.Delivery())
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
func route(delivery proton.Delivery) {
	msg, err := delivery.Message()
	if err != nil {
		logger.Printf("route", "Error reading message: %v", err)
		delivery.Reject()
		return
	}
	logger.Printf("route", "Message: body=%v", msg.Body())
	attr, err := persist(msg)
	if err != nil {
		logger.Printf("route", "Error persisting attributes: %v", err)
		delivery.Reject()
		return
	}
	to, ok := attr.Get(TO)
	if !ok {
		logger.Printf("route", "recipient not found")
		delivery.Reject()
		return
	}

	user, host, err := attributes.Split(to, "@")
	if err != nil {
		logger.Printf("route", "recipient not readable: %v", err)
		delivery.Reject()
		return
	}
	send(attr, user, host)
	delivery.Accept()
}

func send(attr attributes.Attributes, user, host string) {
	logger.Debugf("send", "sending message to %s@%s", user, host)
}

//TODO rajouter la taille du body dans le message (delivery ?)
func persist(msg amqp.Message) (attr attributes.Attributes, err error) {
	var buf []byte = make([]byte, 0)
	msg.Unmarshal(&buf)
	logger.Debugf("persist", "buffer=%s", buf)
	attr, err = attributes.Unmarshal(buf)
	logger.Debugf("persist", "attr=\n%s", attr.Marshall())
	if err != nil {
		logger.Debugf("persist", "Error reading attributes: %v", err)
		return
	}
	path, ok := attr.Get(FILE)
	if !ok {
		logger.Debugf("persist", "Error : file path not found in attributes")
		err = fmt.Errorf("file path not found in attributes")
		return
	}
	logger.Printf("persist", "write attributes to %s", path)
	err = attributes.SetAttributes(path, attr)
	return
}
