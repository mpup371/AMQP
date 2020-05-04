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
	logger.Debugf("handler", "[event] type=%v", t)

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
		logger.Debugf("handler", "Link opening: name=%s, source=%s, target=%s",
			e.Link().Name(),
			e.Link().Source().Address(),
			e.Link().Target().Address())
	case proton.MLinkOpened:
		logger.Debugf("handler", "link opened: state=%v", e.Link().State())
		if e.Link().Source().Address() == h.topic {
			e.Link().Flow(1)
		}
	case proton.MMessage:
		attr := route(e.Delivery())
		if attr != nil {
			go func() {
				h.dispatch(attr)
			}()
		} else {

		}
	case proton.MSendable:
		if ch, ok := h.senders[e.Link()]; ok {
			ch <- t
		} else {
			logger.Printf("handler", "Sender not found: %s", e.Link().Name())
		}
	case proton.MSettled:
		e.Link().Flow(1)
	case proton.MLinkClosed:
		if ch, ok := h.senders[e.Link()]; ok {
			close(ch)
			delete(h.senders, e.Link())
		} else {
			logger.Printf("handler", "receive link closed, closing session")
			e.Session().Close()
		}
	case proton.MSessionClosed:
		logger.Printf("handler", "session closed, closing connection")
		e.Connection().Close()
	case proton.MConnectionClosed:
		logger.Printf("handler", "connection closed: %v", e.Connection().String())
	case proton.MDisconnected:
		logger.Printf("handler", "Disconnected: %v (%v)", e.Connection(), e.Connection().Error())
	}
}

//TODO Release si erreur interne et qu'on veut garder le message en queue
func route(delivery proton.Delivery) attributes.Attributes {
	msg, err := delivery.Message()
	if err != nil {
		logger.Printf("route", "Error reading message: %v", err)
		// ne pas faire le settle ici pour que ce soit le broker qui le fasse
		// c'est Update() qui envoie la trame, Settle() ne fait que rajouter le flag dans
		// la trame avant envoi.
		// delivery.Reject()
		delivery.Update(proton.Rejected)
		return nil
	}
	logger.Printf("route", "Message: body=%v", msg.Body())
	attr, err := persist(msg)
	if err != nil {
		logger.Printf("route", "Error persisting attributes: %v", err)
		// delivery.Reject()
		delivery.Update(proton.Rejected)
		return nil
	}
	if _, _, err := attr.GetRecipient(); err != nil {
		logger.Printf("route", "recipient not found (%v)", err)
		// delivery.Reject()
		delivery.Update(proton.Rejected)
		return nil
	}

	// delivery.Accept()
	delivery.Update(proton.Accepted)
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
