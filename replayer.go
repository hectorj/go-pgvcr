package pgvcr

import (
	"sync"
)

type constError string

func (e constError) Error() string {
	return string(e)
}

const errNoMoreMessages constError = "no more messages"

type replayer struct {
	lock     sync.Mutex
	messages []messageWithID
	cursor   int

	greetingsLock   sync.Mutex
	greetings       [][]messageWithID
	greetingsCursor int
}

func (r *replayer) ConsumeGreetings(fn func(msgs []messageWithID) error) error {
	r.greetingsLock.Lock()
	defer r.greetingsLock.Unlock()

	msgs := r.greetings[r.greetingsCursor]

	if r.greetingsCursor+1 < len(r.messages) {
		r.greetingsCursor++
	}

	return fn(msgs)
}

func (r *replayer) ConsumeNext(fn func(m messageWithID) error) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.cursor >= len(r.messages) {
		return errNoMoreMessages
	}

	msg := r.messages[r.cursor]
	r.cursor++

	return fn(msg)
}