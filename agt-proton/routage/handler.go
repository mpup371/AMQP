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
	topic   string
	engine  *proton.Engine
	session proton.Session
	senders map[proton.Link]chan proton.MessagingEvent
}

func newHandler(topic string) *handler {
	h := handler{topic: topic}
	h.senders = make(map[proton.Link]chan proton.MessagingEvent)
	return &h
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
		h.session = e.Session()
		receiver := e.Session().Receiver("receiver")
		logger.Debugf("handler", "receiver: state=%v", receiver.State())
		receiver.SetRcvSettleMode(proton.RcvFirst)
		receiver.Source().SetAddress(h.topic)
		receiver.Open()
	case proton.MLinkOpening:
		logger.Debugf("handler", "Link opening: name=%s, source=%s",
			e.Link().Name(),
			e.Link().Source().Address())
		if e.Link().Source().Address() == h.topic {
			e.Link().Flow(1)
		}
	case proton.MLinkOpened:
		logger.Debugf("handler", "link opened: state=%v", e.Link().State())
	case proton.MMessage:
		attr := route(e.Delivery())
		if attr != nil {
			go h.dispatch(attr, make(chan proton.MessagingEvent))
		}
	case proton.MSendable:
		if ch, ok := h.senders[e.Link()]; ok {
			// ch <- t
			close(ch)
		} else {
			logger.Printf("handler", "Sender not found: %s", e.Link().Name())
		}
	case proton.MLinkClosed:
		if ch, ok := h.senders[e.Link()]; ok {
			close(ch)
			delete(h.senders, e.Link())
		}
		e.Session().Close()
	case proton.MSessionClosed:
		e.Connection().Close()
	case proton.MConnectionClosed:
		logger.Debugf("handler", "connection closed: %v", e.Connection().String())
	case proton.MDisconnected:
		logger.Debugf("handler", "Disconnected: %v (%v)", e.Connection(), e.Connection().Error())
	}
}

//TODO: si mesg pourri release au lieu de reject pour que le broker supprime le msg
func route(delivery proton.Delivery) attributes.Attributes {
	msg, err := delivery.Message()
	if err != nil {
		logger.Printf("route", "Error reading message: %v", err)
		delivery.Reject()
		return nil
	}
	logger.Printf("route", "Message: body=%v", msg.Body())
	attr, err := persist(msg)
	if err != nil {
		logger.Printf("route", "Error persisting attributes: %v", err)
		delivery.Reject()
		return nil
	}
	if _, _, err := attr.GetRecipient(); err != nil {
		logger.Printf("route", "recipient not found (%v)", err)
		delivery.Reject()
	}

	delivery.Accept()
	// il faut accepter avant de lancer les goroutines sinon on
	// va relire le même message du broker

	return attr
}

func persist(msg amqp.Message) (attr attributes.Attributes, err error) {
	var buf []byte = make([]byte, 0, 1024)
	msg.Unmarshal(&buf)
	logger.Debugf("persist", "buffer=%s", buf)
	attr, err = attributes.Unmarshal(buf)
	logger.Debugf("persist", "attr=\n%s", attr.Marshall())
	if err != nil {
		logger.Debugf("persist", "Error reading attributes: %v", err)
		return
	}
	path, err := attr.GetFile()
	if err != nil {
		logger.Debugf("persist", "Error : file path not found in attributes")
		err = fmt.Errorf("file path not found in attributes")
		return
	}
	logger.Printf("persist", "write attributes to %s", path)
	err = attributes.SetAttributes(path, attr)
	return
}
