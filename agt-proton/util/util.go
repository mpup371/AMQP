package util

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

func GetName() string {
	var name string
	host, err := os.Hostname()
	cmd := filepath.Base(os.Args[0])
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
