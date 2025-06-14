package pgvcr

import (
	"braces.dev/errtrace"
	"bytes"
	"encoding/gob"
	"github.com/jackc/pgx/v5/pgproto3"
	"os"
	"path"
	"sync"
)

type recorder struct {
	buffer   bytes.Buffer
	encoder  *gob.Encoder
	lock     sync.Mutex
	isClosed bool
}

type messageWithID struct {
	ConnectionID recordedConnectionID
	Message      pgproto3.Message
	MessageBytes []byte // gob being dumb with empty/nil slices, we need both the message and it's byte encoding, see https://github.com/golang/go/issues/10905
	IsIncoming   bool
}

func (r *recorder) Record(connectionID recordedConnectionID, msg pgproto3.Message, isIncoming bool) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.isClosed {
		return errtrace.New("recorder.Record: already closed")
	}

	if r.encoder == nil {
		r.encoder = gob.NewEncoder(&r.buffer)
	}

	msgBytes, err := msg.Encode(nil)
	if err != nil {
		return errtrace.Wrap(err)
	}

	newMsg := messageWithID{
		ConnectionID: connectionID,
		Message:      msg,
		MessageBytes: msgBytes,
		IsIncoming:   isIncoming,
	}

	return errtrace.Wrap(r.encoder.Encode(&newMsg))
}

func (r *recorder) FlushToFile(filepath string) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.isClosed {
		return errtrace.New("recorder.FlushToFile: already closed")
	}
	r.isClosed = true

	err := os.MkdirAll(path.Dir(filepath), 0755)
	if err != nil {
		return errtrace.Wrap(err)
	}

	return errtrace.Wrap(os.WriteFile(filepath, r.buffer.Bytes(), 0600))
}
