package util

import (
	"fmt"
	"os"
	"strings"
)

func GetName() string {
	var name string
	host, err := os.Hostname()
	//TODO
	cmd := strings.TrimPrefix(os.Args[0], "/")
	if err == nil {
		name = fmt.Sprintf("%s@%s[%d]", cmd, host, os.Getegid())
	} else {
		name = fmt.Sprintf("%s[%d]", cmd, os.Getegid())
	}
	return name
}
