package attributes

import (
	"bufio"
	"bytes"
	"fmt"
	"jf/AMQP/logger"
	"strings"

	"github.com/pkg/xattr"
)

const (
	TO   = "user.agt.routage.to"
	FROM = "user.agt.routage.from"
	FILE = "user.agt.routage.file"
	KEY  = "user.agt.routage.key"
)

var Mandatory = map[string]bool{TO: false, FROM: true, FILE: true, KEY: false}

type Attributes map[string]string

func NewAttributes() Attributes {
	return make(Attributes)
}

func (attr *Attributes) Put(k, v string) {
	(*attr)[k] = v
}

func (attr Attributes) Get(k string) (string, bool) {
	v, ok := attr[k]
	return v, ok
}

func (attr *Attributes) Remove(k string) {
	delete(*attr, k)
}

func (attr Attributes) Copy() Attributes {
	nattr := NewAttributes()
	for k, v := range attr {
		nattr[k] = v
	}
	return nattr
}

func (attr Attributes) Marshall() []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	for k, v := range attr {
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(v)
		buf.WriteString("\n")
	}
	return buf.Bytes()
}

func Unmarshal(body []byte) (Attributes, error) {
	attr := NewAttributes()
	scanner := bufio.NewScanner(bytes.NewBuffer(body))
	for scanner.Scan() {
		line := scanner.Text()
		logger.Debugf("Unmarshal", "line=%v", line)
		k, v, err := Split(line, "=")
		if err != nil {
			logger.Printf("Unmarshal", "Unmarshal attributes: %v", err)
			continue
		}
		attr.Put(k, v)

	}
	return attr, scanner.Err()
}

func SetAttributes(path string, attr Attributes) error {
	for k, v := range attr {
		if err := xattr.Set(path, k, ([]byte)(v)); err != nil {
			return err
		}
	}
	return nil
}

func GetAttributes(path string) (attr Attributes, err error) {
	attr = NewAttributes()
	var keys []string
	if keys, err = xattr.List(path); err != nil {
		return
	}
	for _, k := range keys {
		if b, e := xattr.Get(path, k); e == nil {
			attr.Put(k, (string)(b))
		} else {
			logger.Printf("xattr", "error reading attribute %s on file %s: %v", k, path, e)
		}
	}
	return
}

func Split(line, sep string) (s1, s2 string, err error) {
	tuple := strings.Split(line, sep)
	if len(tuple) == 2 {
		s1 = tuple[0]
		s2 = tuple[1]
	} else {
		err = fmt.Errorf("Error reading attribute: %s", line)
	}
	return
}

func GetRecipient(to string) (user, host string, err error) {
	user, host, err = Split(to, "@")
	if err != nil {
		err = fmt.Errorf("malformed recipient %v: %v", to, err)
	}
	return
}

func (attr Attributes) GetFile() (string, error) {
	path, ok := attr.Get(FILE)
	if !ok {
		err := fmt.Errorf("path field not found")
		return "", err
	}
	return path, nil
}
