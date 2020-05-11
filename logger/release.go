// +build !debug

package logger

import (
	"jf/AMQP/agt-proton/util"
	"log"
	"os"
)

func init() {
	Debugf = debugf
	Printf = printf
	Fatalf = fatalf
	logger = log.New(os.Stderr, util.GetName(), log.Ldate|log.Ltime|log.LUTC|log.Lmicroseconds|log.Lmsgprefix)
}

func debugf(id string, format string, data ...interface{}) {
}

func printf(id string, format string, data ...interface{}) {
	logger.Printf(format, data...)
}

func fatalf(id string, format string, data ...interface{}) {
	logger.Fatalf(format, data...)
}
