// +build !debug

package logger

import "log"

func init() {
	Debugf = debugf
	Printf = printf
	Fatalf = fatalf
}

func debugf(id string, format string, data ...interface{}) {
}

func printf(id string, format string, data ...interface{}) {
	log.Printf(id+" "+format, data...)
}

func fatalf(id string, format string, data ...interface{}) {
	log.Fatalf(id+" "+format, data...)
}
