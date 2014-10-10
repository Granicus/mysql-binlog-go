package binlog

import (
	"fmt"
	"io"
	"log"
)

type EventData interface{}

type EventDeserializer interface {
	Deserialize(io.ReadSeeker, *Event) EventData
}

type Event struct {
	TotalLength  int
	HeaderLength int
	DataLength   int
	Header       *EventHeader
	Data         EventData
}

func ReadEvent(r io.ReadSeeker) *Event {
	event := new(Event)

	startingPosition, err := r.Seek(0, 1)
	if err != nil {
		log.Fatal(err)
	}

	event.Header = deserializeEventHeader(r)
	fmt.Println("Event:")
	fmt.Println("  Head:", event.Header)
	fmt.Println("  Type:", event.Header.Type)

	postHeaderPosition, err := r.Seek(0, 1)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("post:", postHeaderPosition, ", start:", startingPosition)

	event.HeaderLength = 19 // constant header length
	event.TotalLength = int(int64(event.Header.NextPosition) - startingPosition)
	event.DataLength = event.TotalLength - event.HeaderLength

	// event.header.Type = UPDATE_ROWS_EVENTv2

	event.Data = event.Header.DataDeserializer().Deserialize(r, event)

	currentPos, err := r.Seek(0, 1)
	fatalErr(err)

	if currentPos != int64(event.Header.NextPosition) {
		_, err = r.Seek(int64(event.Header.NextPosition), 0)
		// Alternative, slightly faster:
		// _, err = r.Seek(int64(event.header.NextPosition) - currentPos, 1)
	}

	return event
}
