package logger

import "log"

type debugFunction func(id string, format string, data ...interface{})

var logger *log.Logger

var Debugf, Printf, Fatalf debugFunction
