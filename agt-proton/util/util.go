package util

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func GetName() string {
	var name string
	host, err := os.Hostname()
	i := strings.LastIndex(os.Args[0], "/")
	cmd := os.Args[0][i+1:]
	if err == nil {
		name = fmt.Sprintf("%s@%s[%d]", cmd, host, os.Getegid())
	} else {
		name = fmt.Sprintf("%s[%d]", cmd, os.Getegid())
	}
	return name
}

func FormatTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}
