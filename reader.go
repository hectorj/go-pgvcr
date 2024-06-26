package pgvcr

import (
	"braces.dev/errtrace"
	"encoding/gob"
	"errors"
	"github.com/jackc/pgx/v5/pgproto3"
	"io"
	"os"
	"strings"
)

func readMessages(filepath string) ([][]messageWithID, []messageWithID, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, nil, errtrace.Wrap(err)
	}
	defer file.Close()
	decoder := gob.NewDecoder(file)

	var records []messageWithID
	for {
		var msg messageWithID
		err = decoder.Decode(&msg)
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, nil, errtrace.Wrap(err)
		}

		// TODO: drop the Message field and rebuild the message completely from its byte encoding

		// gob being dumb with empty/nil slices, we need both the message and it's byte encoding, see https://github.com/golang/go/issues/10905
		err = msg.Message.Decode(msg.MessageBytes[5:]) // remove the 1 byte message type identifier and the 4 byte message length
		if err != nil {
			return nil, nil, errtrace.Wrap(err)
		}

		records = append(records, msg)
	}
	_ = file.Close()

	records = filterOutAuth(records)
	records = filterOutPings(records)
	records = filterOutTerminate(records)

	return errtrace.Wrap3(splitGreetingsByConnectionID(records))
}

func filterOutTerminate(records []messageWithID) []messageWithID {
	filteredRecords := make([]messageWithID, 0, len(records))

	for _, record := range records {
		switch record.Message.(type) {
		case *pgproto3.Terminate:
			// Skip this record
			continue
		default:
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}

func filterOutAuth(records []messageWithID) []messageWithID {
	filteredRecords := make([]messageWithID, 0, len(records))

	for _, record := range records {
		switch record.Message.(type) {
		case pgproto3.AuthenticationResponseMessage, *pgproto3.SASLInitialResponse, *pgproto3.SASLResponse:
			// Skip this record
			continue
		default:
			filteredRecords = append(filteredRecords, record)
		}
	}

	return filteredRecords
}

func splitGreetingsByConnectionID(records []messageWithID) ([][]messageWithID, []messageWithID, error) {
	greetingsByConnectionID := make(map[recordedConnectionID][]messageWithID)
	connectionFinished := make(map[recordedConnectionID]bool)
	var leftovers []messageWithID

	for _, record := range records {
		switch record.Message.(type) {
		case *pgproto3.ParameterStatus, *pgproto3.BackendKeyData:
			if !connectionFinished[record.ConnectionID] {
				greetingsByConnectionID[record.ConnectionID] = append(greetingsByConnectionID[record.ConnectionID], record)
			} else {
				leftovers = append(leftovers, record)
			}
		case *pgproto3.ReadyForQuery:
			if !connectionFinished[record.ConnectionID] {
				greetingsByConnectionID[record.ConnectionID] = append(greetingsByConnectionID[record.ConnectionID], record)
				// Stop for this ConnectionID after we encounter the first ReadyForQuery
				connectionFinished[record.ConnectionID] = true
			} else {
				leftovers = append(leftovers, record)
			}
		default:
			leftovers = append(leftovers, record)
		}
	}

	var greetings [][]messageWithID
	for _, greeting := range greetingsByConnectionID {
		greetings = append(greetings, greeting)
	}

	return greetings, leftovers, nil
}

func filterOutPings(records []messageWithID) []messageWithID {
	filteredRecords := make([]messageWithID, 0, len(records))
	connectionInPingSequence := make(map[recordedConnectionID]bool)

	for _, record := range records {
		if isPingQuery(record.Message) {
			connectionInPingSequence[record.ConnectionID] = true
			continue
		}
		if connectionInPingSequence[record.ConnectionID] {
			if isPingResponse(record.Message) {
				continue
			}
			if isReadyForQuery(record.Message) {
				connectionInPingSequence[record.ConnectionID] = false
				continue
			}
		}

		connectionInPingSequence[record.ConnectionID] = false
		filteredRecords = append(filteredRecords, record)
	}

	return filteredRecords
}

func isReadyForQuery(msg pgproto3.Message) bool {
	_, ok := msg.(*pgproto3.ReadyForQuery)
	return ok
}

func isPingQuery(msg pgproto3.Message) bool {
	q, ok := msg.(*pgproto3.Query)
	return ok && strings.EqualFold(q.String, "-- ping")
}

func isPingResponse(msg pgproto3.Message) bool {
	_, ok := msg.(*pgproto3.EmptyQueryResponse)
	return ok
}
