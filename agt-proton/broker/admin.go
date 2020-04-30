package main

import (
	"encoding/json"
	"jf/AMQP/logger"
	"time"

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
}

func formatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

func makeStats(queues *queues) ([]byte, error) {
	infos := make([]interface{}, 0)

	agt.mu.Lock()
	infoHandlers := make([]hInfo, 0, len(agt.handlers))
	for _, info := range agt.handlers {
		h := hInfo{Engine: info.engine,
			Connection: info.connection,
			Container:  info.container}
		infoHandlers = append(infoHandlers, h)
	}
	agt.mu.Unlock()
	infos = append(infos, infoHandlers)

	infoQueues := make([]jInfo, 0, queues.Len())
	for _, info := range queues.Infos() {
		j := jInfo{Queue: info.Name, Size: info.Size}
		j.Creation = formatTime(info.Creation)
		j.LastRead = formatTime(info.LastRead)
		j.LastWrite = formatTime(info.LastWrite)
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
