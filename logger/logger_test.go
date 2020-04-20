package logger

import (
	"strings"
	"testing"
)

const (
	resetColor   = "\033[0m"
	infoColor    = "\033[1;34m"
	noticeColor  = "\033[1;36m"
	warningColor = "\033[1;33m"
	errorColor   = "\033[1;31m"
	debugColor   = "\033[0;36m"
)

func BenchmarkOne(b *testing.B) {
	id := "coucou"
	format := "coucou %s et %d"
	for i := 0; i < b.N; i++ {
		_ = errorColor + id + " " + debugColor + format + resetColor
	}
}

func BenchmarkTwo(b *testing.B) {
	id := "coucou"
	format := "coucou %s et %d"
	for i := 0; i < b.N; i++ {
		var b strings.Builder
		b.WriteString(noticeColor)
		b.WriteString(id)
		b.WriteString(" ")
		b.WriteString(debugColor)
		b.WriteString(format)
		b.WriteString(resetColor)
	}
}
