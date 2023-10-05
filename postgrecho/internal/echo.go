package internal

import (
	"encoding/json"
	wire "github.com/jeroenrinzema/psql-wire"
	"github.com/lib/pq/oid"
	"slices"
)

type Echo struct {
	SessionID int64             `json:"sessionID"`
	Prepare   Nullable[Prepare] `json:"prepare"`
	Execute   Nullable[Execute] `json:"execute"`
}

func (e Echo) MatchPrepare(query string) bool {
	return e.Prepare.Valid && e.Prepare.Value.Match(query)
}

func (e Echo) MatchExecute(query string, parameters []string) bool {
	return e.Execute.Valid && e.Execute.Value.Match(query, parameters)
}

type Nullable[T any] struct {
	Valid bool `json:"valid"`
	Value T    `json:"value"`
}

func (n Nullable[T]) MarshalJSON() ([]byte, error) {
	if n.Valid {
		return json.Marshal(n.Value)
	}
	return []byte("null"), nil
}

func (n *Nullable[T]) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		n.Valid = false
		return nil
	}
	n.Valid = true
	return json.Unmarshal(data, &n.Value)
}

type Prepare struct {
	Query   string       `json:"query"`
	Oids    []oid.Oid    `json:"oids"`
	Columns wire.Columns `json:"columns"`
	Error   string       `json:"error"`
}

func (p Prepare) Match(query string) bool {
	return query == p.Query
}

type Execute struct {
	Query      string   `json:"query"`
	Parameters []string `json:"parameters"`
	Rows       [][]any  `json:"rows"`
	CommandTag string   `json:"commandTag"`
	Error      string   `json:"error"`
}

func (e Execute) Match(query string, parameters []string) bool {
	return query == e.Query && slices.Equal(parameters, e.Parameters)
}
