// +build !debug

package logger

import "log"

func init() {
	Debugf = debugf
	Printf = printf
	Fatalf = fatalf
}

func debugf(id string, format string, data ...interface{}) {
	log.Printf(format, data...)
}

func printf(id string, format string, data ...interface{}) {
	log.Printf(format, data...)
}

func fatalf(id string, format string, data ...interface{}) {
	log.Fatalf(format, data...)
}
