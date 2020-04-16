// +build !debug

package logger

import "log"

func Debugf(id string, format string, data ...interface{}) {
	log.Printf(format, data...)
}

func Printf(id string, format string, data ...interface{}) {
	log.Printf(format, data...)
}

func Fatalf(id string, format string, data ...interface{}) {
	log.Fatalf(format, data...)
}
