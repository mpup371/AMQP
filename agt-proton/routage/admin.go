package main

import (
	"encoding/json"
	"jf/AMQP/logger"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

type jList []jInfo
type jInfo struct {
	Queue string
	Size  uint
}

func makeStats(queues *queues) ([]byte, error) {
	infoQueues := make([]jInfo, 0, queues.Len())

	for _, info := range queues.Infos() {
		infoQueues = append(infoQueues, jInfo{info.Name, info.Size})
	}
	return json.Marshal(infoQueues)
}

func (h *handler) sendAdmin(sender proton.Link) {
	logger.Debugf("sendAdmin", "sending stats on link %v", sender)
	var msg amqp.Message
	if b, err := makeStats(h.queues); err == nil {
		logger.Debugf("admin", string(b))
		msg = amqp.NewMessageWith(b)
	} else {
		logger.Printf("admin", "error marshalling json: %v", err)
		msg = amqp.NewMessageWith("Error making stats")
	}

	if _, err := sender.Send(msg); err == nil {
		logger.Debugf("sendMsg", "msg=%#v", msg)
	} else {
		logger.Printf("sendMsg", "error: %v", err)
	}
}
