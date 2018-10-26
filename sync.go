package cache

import (
	"errors"
	"time"
)

type Future chan bool

var doneFuture Future
var failFuture Future

func init() {
	doneFuture = make(Future, 1)
	doneFuture.done()

	failFuture = make(Future, 1)
	failFuture.fail()
}

func (dc Future) Sync() (bool, error) {
	return <-dc, nil
}

func (dc Future) Wait(t time.Duration) (bool, error) {
	if t == 0 {
		return <-dc, nil
	} else {
		select {
		case v := <-dc:
			return v, nil
		case <-time.After(t):
			return false, errors.New("timeout")
		}
	}
}

func (dc Future) done() {
	dc <- true
	close(dc)
}

func (dc Future) fail() {
	dc <- false
	close(dc)
}

type optWrapper struct {
	future Future
	meta   interface{}
}

func newOptWrapper(m interface{}) *optWrapper {
	return &optWrapper{
		future: make(Future, 1),
		meta:   m,
	}
}
