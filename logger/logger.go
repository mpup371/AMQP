package logger

import "log"

const (
	resetColor   = "\033[0m"
	infoColor    = "\033[1;34m"
	noticeColor  = "\033[1;36m"
	warningColor = "\033[1;33m"
	errorColor   = "\033[1;31m"
	debugColor   = "\033[0;36m"
)

type DebugFunction func(id string, format string, data ...interface{})

//TODO: utiliser tag  à la compil
func Debugf(id string, format string, data ...interface{}) {
	log.Printf(noticeColor+id+" "+debugColor+format+resetColor, data...)
}

func Printf(id string, format string, data ...interface{}) {
	log.Printf(noticeColor+id+" "+resetColor+format, data...)
}

func Fatalf(id string, format string, data ...interface{}) {
	log.Fatalf(errorColor+id+" "+debugColor+format+resetColor, data...)
}
