package binlog

import (
	"encoding/binary"
	"fmt"

	"github.com/granicus/mysql-binlog-go/bitset"
	. "github.com/granicus/mysql-binlog-go/deserialization"
)

type TableMapEvent struct {
	TableId         uint64
	DatabaseName    string
	TableName       string
	NumberOfColumns uint64
	ColumnTypes     []MysqlType
	Metadata        []*ColumnMetadata
	CanBeNull       bitset.Bitset
}

/*
TABLE MAP DATA
==============

Fixed:
6 bytes = table id
2 bytes = reserved (skip)

Let:
X = database name length
Y = table name length
P = number determined by byte key; can be 0, 2, 3, or 8
C = number of columns
N = (7 + C) / 8
M = metadata length

Variable:
1 byte    = database name length
X+1 bytes = database name (null terminated)
1 byte    = table name length
Y+1 bytes = table name (null terminated)
1 byte    = packed int byte key (see ReadPackedInteger)
P bytes   = number of columns
C bytes   = column types
1 byty    = packed int byte key (see ReadPackedInteger)
P bytes   = metdata length
M bytes   = metadata
N bytes   = can be null bitset

*/

func (b *Binlog) DeserializeTableMapEvent(header *EventHeader) EventData {
	e := new(TableMapEvent)

	// Read 6 bytes for table id, pad to 8, read as uint64
	tableIdBytes, err := ReadBytes(b.reader, 6)
	fatalErr(err)
	e.TableId = binary.LittleEndian.Uint64(append(tableIdBytes, byte(0), byte(0)))

	// Skip 2 reserved and 1 database name length bytes
	_, err = b.reader.Seek(3, 1)
	fatalErr(err)

	e.DatabaseName, err = ReadNullTerminatedString(b.reader)
	fatalErr(err)

	// Skip table name length
	_, err = b.reader.Seek(1, 1)
	fatalErr(err)

	e.TableName, err = ReadNullTerminatedString(b.reader)
	fatalErr(err)

	e.NumberOfColumns, err = ReadPackedInteger(b.reader)
	fatalErr(err)

	// Read column types as bytes and convert them to MysqlTypes
	columnTypesBytes, err := ReadBytes(b.reader, int(e.NumberOfColumns))
	fatalErr(err)

	e.ColumnTypes = make([]MysqlType, len(columnTypesBytes))
	for i, b := range columnTypesBytes {
		e.ColumnTypes[i] = MysqlType(b)
	}

	metadataLength, err := ReadPackedInteger(b.reader)
	fatalErr(err)

	preMetadataPosition, err := b.reader.Seek(0, 1)
	fatalErr(err)

	e.Metadata = make([]*ColumnMetadata, len(e.ColumnTypes))
	for i, t := range e.ColumnTypes {
		e.Metadata[i] = DeserializeColomnMetadata(b.reader, t)
	}

	postMetadataPosition, err := b.reader.Seek(0, 1)
	fatalErr(err)

	if postMetadataPosition-preMetadataPosition != int64(metadataLength) {
		panic(fmt.Sprintf("Overshot metadata length by %v", postMetadataPosition-preMetadataPosition-int64(metadataLength)))
	}

	e.CanBeNull, err = ReadBitset(b.reader, int(e.NumberOfColumns))
	fatalErr(err)

	// Insert into tableMapCollectionInstance
	b.TableMapCollection[e.TableId] = e

	return e
}
