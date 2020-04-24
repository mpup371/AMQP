package util

import (
	"fmt"
	"jf/AMQP/logger"
	"os"

	"qpid.apache.org/proton"
)

func GetName() string {
	var name string
	host, err := os.Hostname()
	if err == nil {
		name = fmt.Sprintf("%s@%s[%d]", os.Args[0], host, os.Getegid())
	} else {
		name = fmt.Sprintf("routage[%d]", os.Getegid())
	}
	return name
}

func LogEvent(t proton.MessagingEvent, e proton.Event) {
	logger.Debugf("event", "type=%v", t)
}
