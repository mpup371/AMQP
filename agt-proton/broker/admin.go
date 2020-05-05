package main

import (
	"encoding/json"
	"jf/AMQP/agt-proton/util"
	"jf/AMQP/logger"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

type jInfo struct {
	Queue     string
	Size      uint
	Creation  string
	LastRead  string
	LastWrite string
}

type hInfo struct {
	Engine     string
	Connection string
	Container  string
	LastEvent  string
	Links      []lInfo
}

type lInfo struct {
	Topic    string
	RW       string
	Creation string
}

func makeStats(queues *queues) ([]byte, error) {
	infos := make([]interface{}, 0)

	agt.mu.Lock()
	infoHandlers := make([]hInfo, 0, len(agt.handlers))
	for _, info := range agt.handlers {
		h := hInfo{Engine: info.engine,
			Connection: info.connection,
			Container:  info.container,
			LastEvent:  util.FormatTime(info.lastEvent),
			Links:      make([]lInfo, 0)}

		for _, l := range info.links {
			li := lInfo{Topic: l.topic, RW: l.rw, Creation: util.FormatTime(l.creationDate)}
			h.Links = append(h.Links, li)
		}
		infoHandlers = append(infoHandlers, h)
	}
	agt.mu.Unlock()
	infos = append(infos, infoHandlers)

	infoQueues := make([]jInfo, 0, queues.Len())
	for _, info := range queues.Infos() {
		j := jInfo{Queue: info.Name, Size: info.Size}
		j.Creation = util.FormatTime(info.Creation)
		j.LastRead = util.FormatTime(info.LastRead)
		j.LastWrite = util.FormatTime(info.LastWrite)
		infoQueues = append(infoQueues, j)
	}
	infos = append(infos, infoQueues)

	return json.Marshal(infos)
}

func (h *handler) sendAdmin(sender proton.Link) {
	logger.Debugf("sendAdmin", "sending stats on link %v", sender)
	var msg amqp.Message

	if b, err := makeStats(&agt.queues); err == nil {
		logger.Debugf("admin", string(b))
		msg = amqp.NewMessageWith(b)
	} else {
		logger.Printf(h.engine, "error marshalling json: %v", err)
		msg = amqp.NewMessageWith("Error making stats")
	}

	if _, err := sender.Send(msg); err == nil {
		logger.Debugf("sendMsg", "msg=%#v", msg)
	} else {
		logger.Printf(h.engine, "error: %v", err)
	}
}
