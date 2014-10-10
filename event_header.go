package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/granicus/mysql-binlog-go/deserialization"
)

type EventHeader struct {
	Timestamp    uint32
	Type         MysqlBinlogEventType
	ServerId     uint32
	Length       uint32
	NextPosition uint32
	Flag         [2]byte
}

// TODO: move this over to use encoding/binary with struct pointer
func deserializeEventHeader(r io.Reader) *EventHeader {
	// Read number of bytes in header
	b, err := deserialization.ReadBytes(r, 4+1+4+4+4+2)
	fatalErr(err)

	var h EventHeader
	fatalErr(binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &h))

	return &h
}

func (h *EventHeader) DataDeserializer() EventDeserializer {
	switch h.Type {
	case WRITE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv0, DELETE_ROWS_EVENTv0,
		WRITE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv1, DELETE_ROWS_EVENTv1,
		WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv2:
		return &RowsEventDeserializer{}

	case TABLE_MAP_EVENT:
		return &TableMapEventDeserializer{}

	default:
		fmt.Println("unsupported event data deserialization:", h.Type)

		return &SkipEventDeserializer{}
	}

	return nil
}
