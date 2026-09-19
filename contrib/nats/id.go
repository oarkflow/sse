package ssenats

import "github.com/nats-io/nuid"

func newRandomID() string {
	return nuid.Next()
}
