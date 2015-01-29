package binlog

import (
	"encoding/binary"
	"fmt"

	"github.com/granicus/mysql-binlog-go/bitset"
	. "github.com/granicus/mysql-binlog-go/deserialization"
)

type RowsEvent struct {
	Type            MysqlBinlogEventType
	TableId         uint64
	NumberOfColumns uint64
	UsedSet         bitset.Bitset
	Rows            []RowImage
}

func (e *RowsEvent) UsedFields() int {
	used := 0

	for i := uint(0); i < uint(e.NumberOfColumns); i++ {
		if e.UsedSet.Bit(i) {
			used++
		}
	}

	return used
}

/*
ROWS EVENT DATA
===============

Let:
P = number determined by byte key; can be 0, 2, 3, or 8
N = (7 + number of columns) / 8
J = (7 + number of true bits in column used bitfield) / 8
K = number of false bits in null bitfield (not counting padding in last byte)
U = 2 if update event, 1 for any other ones
B = number of rows (determined by reading till data length reached)

Fixed Section:
6 bytes = table id
2 bytes = reserved (skip)

Variable Section:
1 byte  = packed int byte key (see ReadPackedInteger)
P bytes = number of columns
N bytes = column used bitfield
U * B * (
	J bytes = null bitfield
	K bytes = row image
)

FOR ROW IMAGE CELL DESERIALIZATION:
http://bazaar.launchpad.net/~mysql/mysql-server/5.6/view/head:/sql/log_event.cc#L1942

*/

func (b *Binlog) DeserializeRowsEvent(header *EventHeader) EventData {
	e := new(RowsEvent)
	e.Type = header.Type

	// Read 6 bytes for table id, pad to 8, read as uint64
	tableIdBytes, err := ReadBytes(b.reader, 6)
	fatalErr(err)
	e.TableId = binary.LittleEndian.Uint64(append(tableIdBytes, byte(0), byte(0)))

	// If the TableMapEvent has not been logged, find it and deserialize it
	tableMap, ok := b.TableMapCollection[e.TableId]
	if !ok {
		// TODO: create position stash/pop system
		oldPosition, err := b.reader.Seek(0, 1)
		fatalErr(err)

		tableMap = b.findTableMapEvent(e.TableId)

		_, err = b.reader.Seek(oldPosition, 0)
		fatalErr(err)
	}

	// Skip reserved bytes
	_, err = b.reader.Seek(2, 1)
	fatalErr(err)

	// Skip extra v2 row event info
	switch header.Type {
	case WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv2:
		extraInfoLength, err := ReadUint16(b.reader)
		fatalErr(err)

		_, err = b.reader.Seek(int64(extraInfoLength-2), 1)
		fatalErr(err)
	}

	e.NumberOfColumns, err = ReadPackedInteger(b.reader)
	fatalErr(err)

	e.UsedSet, err = ReadBitset(b.reader, int(e.NumberOfColumns))
	fatalErr(err)

	numberOfFields := e.UsedFields()
	e.Rows = []RowImage{}

	// Rows deserialization loop
	for {
		currentPos, err := b.reader.Seek(0, 1)
		fatalErr(err)

		// Check if there are any more rows to deserialize
		if currentPos >= int64(header.NextPosition)-4 {
			if currentPos > int64(header.NextPosition) {
				panic(fmt.Errorf("** ROW EVENT OVERSHOT READING: %v", currentPos-int64(header.NextPosition)))
			}

			break
		}

		nullSet, err := ReadBitset(b.reader, numberOfFields)
		fatalErr(err)

		if len(e.UsedSet) != len(nullSet) {
			panic(fmt.Errorf("UsedSet and NullSet length mismatched"))
		}

		if uint64(len(tableMap.ColumnTypes)) != e.NumberOfColumns {
			panic(fmt.Errorf("Table map does not contain expected number of column types %v %v", len(tableMap.ColumnTypes), e.NumberOfColumns))
		}

		cells := make(RowImage, e.NumberOfColumns)
		for i := 0; i < int(e.NumberOfColumns); i++ {
			if e.UsedSet.Bit(uint(i)) {
				if nullSet.Bit(uint(i)) {
					cells[i] = NewNullRowImageCell(tableMap.ColumnTypes[i])
				} else {
					cells[i] = DeserializeRowImageCell(b.reader, tableMap, i)
				}
			} else {
				cells[i] = nil
			}
		}

		e.Rows = append(e.Rows, cells)
	}

	return e
}
