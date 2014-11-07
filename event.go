package binlog

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/granicus/mysql-binlog-go/deserialization"
)

type EventData interface{}

type EventHeader struct {
	Timestamp    uint32
	Type         MysqlBinlogEventType
	ServerId     uint32
	Length       uint32
	NextPosition uint32
	Flag         [2]byte
}

// TODO: move this over to use encoding/binary with struct pointer
func ReadEventHeader(r io.Reader) *EventHeader {
	// Read number of bytes in header
	b, err := deserialization.ReadBytes(r, 4+1+4+4+4+2)
	fatalErr(err)

	var h EventHeader
	fatalErr(binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &h))

	return &h
}

type Event struct {
	eventType      MysqlBinlogEventType
	readerPosition int64
	binlog         *Binlog
	header         *EventHeader
	data           *EventData
}

func newIndexedEvent(binlog *Binlog, eventType MysqlBinlogEventType, position int64) *Event {
	return &Event{
		eventType:      eventType,
		readerPosition: position,
		binlog:         binlog,
	}
}

func newDeserializedEvent(binlog *Binlog, position int64, header *EventHeader, data EventData) *Event {
	return &Event{
		eventType:      header.Type,
		readerPosition: position,
		binlog:         binlog,
		header:         header,
		data:           &data,
	}
}

func (e *Event) deserializeHeader() {
	e.header = e.binlog.deserializeEventHeader(e.readerPosition)
}

func (e *Event) deserializeData() {
	data := e.binlog.deserializeEventData(e.readerPosition, e.Header())
	dataInterface := EventData(data) // compiler bug (can't do "&(interface{}(data))")
	e.data = &dataInterface
}

func (e *Event) Type() MysqlBinlogEventType {
	return e.eventType
}

func (e *Event) Position() int64 {
	return e.readerPosition
}

func (e *Event) Header() *EventHeader {
	if e.header == nil {
		e.deserializeHeader()
	}

	return e.header
}

func (e *Event) Data() EventData {
	if e.data == nil {
		e.deserializeData()
	}

	return *e.data
}
