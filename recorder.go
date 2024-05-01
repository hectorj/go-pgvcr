package pgvcr

import (
	"bytes"
	"encoding/gob"
	"github.com/jackc/pgx/v5/pgproto3"
	"os"
	"sync"
)

type recorder struct {
	buffer  bytes.Buffer
	encoder *gob.Encoder
	lock    sync.Mutex
}

type messageWithID struct {
	ConnectionID uint64
	Message      pgproto3.Message
	IsIncoming   bool
}

func (r *recorder) Record(connectionID uint64, msg pgproto3.Message, isIncoming bool) error {
	r.lock.Lock()
	defer r.lock.Unlock()

	if r.encoder == nil {
		r.encoder = gob.NewEncoder(&r.buffer)
	}

	newMsg := messageWithID{
		ConnectionID: connectionID,
		Message:      msg,
		IsIncoming:   isIncoming,
	}

	return r.encoder.Encode(&newMsg)
}

func (r *recorder) FlushToFile(filepath string) error {
	return os.WriteFile(filepath, r.buffer.Bytes(), 0600)
}
