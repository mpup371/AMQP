package attributes

import (
	"bufio"
	"bytes"
	"fmt"
	"jf/AMQP/logger"
	"strings"

	"github.com/pkg/xattr"
)

type Attributes map[string]string

func NewAttributes() Attributes {
	return make(Attributes, 0)
}

func (attr *Attributes) Add(k, v string) {
	(*attr)[k] = v
}

func (attr Attributes) Get(k string) (string, bool) {
	v, ok := attr[k]
	return v, ok
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
		attr.Add(k, v)

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
			attr.Add(k, (string)(b))
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
