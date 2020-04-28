package attributes

import (
	"bufio"
	"bytes"
	"io"
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
	reader := bufio.NewReader(bytes.NewBuffer(body))

	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		logger.Debugf("attributes", "line=%v, err=%v", line, err)
		if err != nil && err != io.EOF {
			return attr, err
		}
		tuple := strings.Split(line, "=")
		if len(tuple) != 2 {
			logger.Printf("attributes", "Error reading attribute: %s", line)
			continue
		}
		attr.Add(tuple[0], tuple[1])
	}
	return attr, nil
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
