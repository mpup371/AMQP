package logger

type DebugFunction func(id string, format string, data ...interface{})

var Debugf, Printf, Fatalf DebugFunction
