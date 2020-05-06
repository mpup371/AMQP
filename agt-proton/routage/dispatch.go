package main

import (
	"jf/AMQP/agt-proton/attributes"
	"jf/AMQP/logger"
	"os"

	"qpid.apache.org/amqp"
	"qpid.apache.org/proton"
)

var keys = map[string][]string{
	"clef1": {"client1@host1", "client2@host2"},
}

func (h *handler) dispatch(attr attributes.Attributes) {

	if key, ok := attr.Get(attributes.KEY); ok {
		logger.Printf("dispatch", "key=%s", key)
		if recipients, ok := keys[key]; ok {
			file, ok := attr.Get(attributes.FILE)
			if !ok {
				logger.Printf("dispatch", "filepath not found")
				return
			}
			// à ce stade, le message a déjà été retiré de la queue,
			// donc il faut supprimer l'original quoi qu'il arrive
			defer rm(file)
			for _, to := range recipients {
				newAttr := attr
				newAttr.Remove(attributes.KEY)
				newAttr.Add(attributes.TO, to)

				if newFile, err := link(file, to); err == nil {
					newAttr.Add(attributes.FILE, newFile)
				} else {
					logger.Printf("dispatch", "Error link file %s (%v)", file, err)
					return
				}

				logger.Debugf("dispatch", "original: %v", attr)
				logger.Debugf("dispatch", "new: %v", newAttr)
				msg := amqp.NewMessage()
				msg.SetBody(newAttr.Marshall())
				h.forward(to, msg)
			}
		} else {
			logger.Printf("dispatch", "key not found: %s", key)
		}
	} else {
		if to, ok := attr.Get(attributes.TO); ok {
			msg := amqp.NewMessage()
			msg.SetBody(attr.Marshall())
			h.forward(to, msg)
		} else {
			logger.Printf("dispatch", "no recipient found")
		}
	}
}

func link(path, suffix string) (newPath string, err error) {
	newPath = path + "-" + suffix
	err = os.Link(path, newPath)
	return
}

func rm(path string) {
	err := os.Remove(path)
	logger.Printf("rm", "rm %s (%v)", path, err)
}

//TODO mock session pour tests unitaires
func (h *handler) forward(to string, msg amqp.Message) {
	user, host, err := attributes.GetRecipient(to)
	if err != nil {
		logger.Printf("forward", "%v", err)
		return
	}
	logger.Printf("forward", "sending message to %s@%s", user, host)
	sender := h.session.Sender("sendTo:" + host)
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
