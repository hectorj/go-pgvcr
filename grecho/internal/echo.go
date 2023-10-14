package internal

import (
	"encoding/json"
	"regexp"
	"slices"

	"github.com/jeroenrinzema/psql-wire/pkg/types"
)

type Echo struct {
	ConnectionID uint64      `json:"connectionID"`
	Sequences    []*Sequence `json:"sequences"`
}

type Sequence struct {
	ClientMessages []ClientMessage `json:"clientMessages"`
	ServerMessages []ServerMessage `json:"serverMessages"`
}

func NewEcho(sessionID uint64) *Echo {
	return &Echo{
		ConnectionID: sessionID,
		Sequences: []*Sequence{
			new(Sequence),
		},
	}
}

func (e *Echo) AddServerMessage(message ServerMessage) {
	sequence := e.Sequences[len(e.Sequences)-1]
	sequence.ServerMessages = append(sequence.ServerMessages, message)
}

func (e *Echo) AddClientMessage(message ClientMessage) {
	sequence := e.Sequences[len(e.Sequences)-1]
	if len(sequence.ServerMessages) > 0 {
		sequence = &Sequence{}
		e.Sequences = append(e.Sequences, sequence)
	}
	sequence.ClientMessages = append(sequence.ClientMessages, message)
}

type ClientMessage struct {
	Type    types.ClientMessage `json:"type"`
	Content []byte              `json:"content"`
}

func (m ClientMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			Type          types.ClientMessage `json:"type"`
			Content       []byte              `json:"content"`
			StringContent string              `json:"stringContent"`
		}{
			Type:          m.Type,
			Content:       m.Content,
			StringContent: string(m.Content),
		},
	)
}

func (m ClientMessage) Match(other ClientMessage) bool {
	if other.Type != m.Type {
		return false
	}
	if !slices.Equal(fuzzMessage(other.Content), fuzzMessage(m.Content)) {
		return false
	}
	return true
}

var statementCacheFuzzingRegexp = regexp.MustCompile(`stmtcache_\d+`)

func fuzzMessage(message []byte) []byte {
	// pgx uses a global counter for its statement names
	return statementCacheFuzzingRegexp.ReplaceAll(message, []byte(`stmtcache_xXfuzzedXx`))
}

type ServerMessage struct {
	Type    types.ServerMessage `json:"type"`
	Content []byte              `json:"content"`
}

func (m ServerMessage) MarshalJSON() ([]byte, error) {
	return json.Marshal(
		struct {
			Type          types.ServerMessage `json:"type"`
			Content       []byte              `json:"content"`
			StringContent string              `json:"stringContent"`
		}{
			Type:          m.Type,
			Content:       m.Content,
			StringContent: string(m.Content),
		},
	)
}
