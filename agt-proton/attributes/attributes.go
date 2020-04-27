package attributes

import (
	"bytes"
	"jf/AMQP/logger"

	"github.com/pkg/xattr"
)

type tuple struct {
	Key   string
	Value string
}
type Attributes []tuple

func NewAttributes() Attributes {
	return make(Attributes, 0)
}

func Add(attr Attributes, k, v string) Attributes {
	return append(attr, tuple{k, v})
}

func Marshall(attr Attributes) []byte {
	buf := bytes.NewBuffer(make([]byte, 0))
	for _, a := range attr {
		buf.WriteString(a.Key)
		buf.WriteString(`=`)
		buf.WriteString(a.Value)
		buf.WriteString("\n")
	}
	return buf.Bytes()
}

func SetAttributes(path string, attr Attributes) error {
	for _, a := range attr {
		if err := xattr.Set(path, a.Key, ([]byte)(a.Value)); err != nil {
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
			attr = Add(attr, k, (string)(b))
		} else {
			logger.Printf("xattr", "error reading attribute %s on file %s: %v", k, path, e)
		}
	}
	return
}
