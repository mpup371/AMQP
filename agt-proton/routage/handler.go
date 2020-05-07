/*
PN_TRACE_FRM=1 go run -tags debug ./receive amqp://localhost:5672/queue1
*/
package main

import (
	"jf/AMQP/agt-proton/attributes"
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"

	fifo "github.com/foize.bck/go.fifo"
	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

type handler struct {
	topic   string
	engine  *proton.Engine
	session proton.Session
	senders map[string]*fifo.Queue
}

func newHandler(topic string) *handler {
	h := handler{topic: topic}
	h.senders = make(map[string]*fifo.Queue)
	return &h
}

func (h *handler) push(host string, msg amqp.Message) {

	if _, ok := h.senders[host]; !ok {
		h.senders[host] = fifo.NewQueue()
		sender := h.session.Sender("sendTo:" + host)
		sender.SetSndSettleMode(proton.SndSettled)
		sender.Target().SetAddress(host)
		sender.Open()
	}
	h.senders[host].Add(msg)
}
func (h *handler) pop(host string) amqp.Message {
	if q, ok := h.senders[host]; ok {
		msg, size := q.Next()
		logger.Debugf("pop", "host=%s, msg=%v, size=%d", host, msg, size)
		if msg == nil {
			logger.Printf("pop", "no message found for %s", host)
		} else {
			return msg.(amqp.Message)
		}
	} else {
		logger.Printf("pop", "Sender not found: %s", host)
	}
	return nil
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
		receiver := e.Session().Receiver("agtRoutage")
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
		attr := accept(e.Delivery())
		if attr != nil {
			h.dispatch(attr)
		}
	case proton.MSettled:
		e.Link().Flow(1)
	case proton.MSendable:
		host := e.Link().Target().Address()
		logger.Debugf("handler", "link sendable host=%s", host)
		if msg := h.pop(host); msg != nil {
			h.sendMsg(e.Link(), msg)
		}
		// ne doit pas arriver:
		// on ne ferme pas les links
	case proton.MLinkClosed:
		host := e.Link().Target().Address()
		if _, ok := h.senders[host]; ok {
			delete(h.senders, host)
		} else {
			logger.Printf("handler", "Close sender not found: %s", e.Link().Name())
		}
		if e.Link().Source().Address() == h.topic { // ne doit jamais arriver
			logger.Printf("handler", "receive link closed by peer")
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

func accept(delivery proton.Delivery) attributes.Attributes {
	msg, err := delivery.Message()
	if err != nil {
		logger.Printf("route", "Error reading message: %v", err)
		delivery.Update(proton.Rejected)
		return nil
	}
	logger.Printf("route", "Message: body=%v", msg.Body())
	attr, err := decode(msg)
	if err != nil {
		logger.Printf("route", "Error persisting attributes: %v", err)
		delivery.Update(proton.Rejected)
		return nil
	}
	path, err := attr.GetFile()
	if err != nil {
		logger.Printf("route", "Error : file path not found in attributes")
		delivery.Update(proton.Rejected)
		return nil
	}
	if err := persist(attr, path); err != nil {
		logger.Printf("route", "Error persisting attributes: %v", err)
		delivery.Update(proton.Rejected) //TODO release
		return nil
	}
	delivery.Update(proton.Accepted)
	return attr
}

func decode(msg amqp.Message) (attr attributes.Attributes, err error) {
	var buf []byte = make([]byte, 0, 1024)
	msg.Unmarshal(&buf)
	logger.Debugf("decode", "buffer=%s", buf)
	attr, err = attributes.Unmarshal(buf)
	logger.Debugf("decode", "attr=\n%s", attr.Marshall())
	if err != nil {
		logger.Printf("decode", "Error reading attributes: %v", err)
	}
	return
}

//TODO Release si erreur interne et qu'on veut garder le message en queue
func persist(attr attributes.Attributes, path string) (err error) {
	logger.Printf("persist", "write attributes to %s", path)
	err = attributes.SetAttributes(path, attr)
	return
}
