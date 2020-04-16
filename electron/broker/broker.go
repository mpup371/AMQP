package main

import (
	"fmt"
	"jf/AMQP/logger"
	"net"
	"time"

	"qpid.apache.org/amqp"
	"qpid.apache.org/electron"
)

// State for the broker
type broker struct {
	queues    queues                // A collection of queues.
	container electron.Container    // electron.Container manages AMQP connections.
	sent      chan sentMessage      // Channel to record sent messages.
	acks      chan electron.Outcome // Channel to receive the Outcome of sent messages.
}

// Record of a sent message and the queue it came from.
// If a message is rejected or not acknowledged due to a failure, we will put it back on the queue.
type sentMessage struct {
	m *amqp.Message
}

// run listens for incoming net.Conn connections and starts an electron.Connection for each one.
func (b *broker) run() error {
	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	logger.Printf("broker.run()", "Listening on %v\n", listener.Addr())

	go b.acknowledgements() // Handles acknowledgements for all connections.

	// Start a goroutine for each new connections
	for {
		c, err := b.container.Accept(listener)
		if err != nil {
			logger.Debugf("broker.run()", "Accept error: %v", err)
			continue
		}
		cc := &connection{b, c}
		go cc.run() // Handle the connection
		logger.Debugf("broker.run()", "Accepted %v", c)
	}
}

// State for a broker connection
type connection struct {
	broker     *broker
	connection electron.Connection
}

// accept remotely-opened endpoints (Session, Sender and Receiver) on a connection
// and start goroutines to service them.
func (c *connection) run() {
	for in := range c.connection.Incoming() {
		logger.Debugf("connection.run()", "incoming %v", in)

		switch in := in.(type) {
		case *electron.IncomingConnection, *electron.IncomingSession:
			in.Accept()

		case *electron.IncomingSender:
			s := in.Accept().(electron.Sender)
			go c.sender(s)

		case *electron.IncomingReceiver:
			in.SetPrefetch(true)
			in.SetCapacity(*credit) // Pre-fetch up to credit window.
			r := in.Accept().(electron.Receiver)
			go c.receiver(r)

		default:
			in.Reject(amqp.Errorf("AMQPserver", "unexpected endpoint %s", in))
		}
	}
	logger.Debugf("connection.run()", "connection closed: %v", c.connection)
}

// receiver receives messages and pushes to a queue.
func (c *connection) receiver(receiver electron.Receiver) {
	id := fmt.Sprintf("connection.receiver(%s)", receiver.LinkName())
	logger.Debugf(id, "push to queue %s", receiver.Target())
	beginConnection := time.Now()
	q := c.broker.queues.Get(receiver.Target())
	var count int = 0
	for {
		if rm, err := receiver.Receive(); err == nil {
			// logger.Debugf("%v: received %v", receiver, rm.Message.Body())
			q.Add(rm.Message)
			if *ack {
				rm.Accept()
			}
		} else {
			logger.Debugf(id, "%v error: %v", receiver, err)
			break
		}
		count++
	}
	endConnection := time.Now()
	elapsed := endConnection.Sub(beginConnection)
	ratio := int((float64)(count) / elapsed.Seconds())
	logger.Printf("id", "%d messages received in %s (%d msg/s)", count, elapsed, ratio)
}

// sender pops messages from a queue and sends them.
func (c *connection) sender(sender electron.Sender) {
	id := fmt.Sprintf("connection.sender(%s)", sender.LinkName())
	logger.Debugf(id, "sender from queue %s", sender.Source())
	beginConnection := time.Now()
	q := c.broker.queues.Get(sender.Source())
	var count int = 0
	for {
		if sender.Error() != nil {
			logger.Debugf(id, "%s closed: %v", sender, sender.Error())
			break
		}
		if m := q.Next(); m != nil {
			count++
			logger.Debugf(id, "sending %v ...", m.Body())
			// sender.SendForget(m)
			sm := sentMessage{&m}
			sender.SendAsync(m, c.broker.acks, sm) // Receive outcome on c.broker.acks with Value sm
			// c.broker.sent <- sm                    // Record sent message
			logger.Debugf(id, "... sent %v", m.Body())
			//TODO: timeout paramétrable
			// outcome := sender.SendSyncTimeout(m, 1*time.Second)
			// if outcome.Status != electron.Accepted { // Error, release or rejection
			// 	logger.Debugf(id, "send error: status %v, error %v", outcome.Status, outcome.Error)
			// } else {
			// 	logger.Debugf(id, "message acknowledged by receiver")
			// }
		}

		// select {
		// case <-sender.Done(): // break if sender is closed
		// 	break
		// default:
		// }
	}
	endConnection := time.Now()
	elapsed := endConnection.Sub(beginConnection)
	ratio := int((float64)(count) / elapsed.Seconds())
	logger.Printf(id, "%d messages sent in %s (%d msg/s)", count, elapsed, ratio)
}

// acknowledgements keeps track of sent messages and receives outcomes.
//
// We could have handled outcomes separately per-connection, per-sender or even
// per-message. Message outcomes are returned via channels defined by the user
// so they can be grouped in any way that suits the application.
func (b *broker) acknowledgements() {
	logger.Debugf("broker.acknowledgements()", "running")
	// sentMap := make(map[sentMessage]bool)
	for {
		select {
		// case sm, ok := <-b.sent: // A local sender records that it has sent a message.
		// 	if ok {
		// 		sentMap[sm] = true
		// 	} else {
		// 		return // Closed
		// 	}
		case outcome := <-b.acks: // The message outcome is available
			sm := outcome.Value.(sentMessage)
			logger.Debugf("broker.acknowledgements()", "outcome: %v", sm)
			// delete(sentMap, sm)
			if outcome.Status != electron.Accepted { // Error, release or rejection
				// sm.q.Add(sm.m) // Put the message back on the queue.
				logger.Debugf("broker.acknowledgements()", "message %v put back, status %v, error %v", (*sm.m).Body(), outcome.Status, outcome.Error)
			}
		}
	}
}
