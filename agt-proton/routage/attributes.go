package main

import (
	"jf/AMQP/logger"

	"github.com/pkg/xattr"
)

type tuple struct {
	key   string
	value string
}
type attributes []tuple

func SetAttributes(path string, attr attributes) error {
	for _, a := range attr {
		if err := xattr.Set(path, a.key, ([]byte)(a.value)); err != nil {
			return err
		}
	}
	return nil
}

func GetAttributes(path string) (attr attributes, err error) {
	attr = make(attributes, 0)

	var keys []string
	if keys, err = xattr.List(path); err != nil {
		return
	}
	for _, k := range keys {
		if b, e := xattr.Get(path, k); e == nil {
			attr = append(attr, tuple{k, (string)(b)})
		} else {
			logger.Printf("xattr", "error reading attribute %s on file %s: %v", k, path, e)
		}
	}
	return
}
