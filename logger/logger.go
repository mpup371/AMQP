package logger

type debugFunction func(id string, format string, data ...interface{})

var Debugf, Printf, Fatalf debugFunction
