package logger

import "log"

const (
	resetColor   = "\033[0m"
	infoColor    = "\033[1;34m"
	noticeColor  = "\033[1;36m"
	warningColor = "\033[1;33m"
	rrrorColor   = "\033[1;31m"
	debugColor   = "\033[0;36m"
)

func Debugf(format string, data ...interface{}) {
	log.Printf(debugColor+format+resetColor, data...)
}
