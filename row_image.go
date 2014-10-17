package binlog

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/granicus/mysql-binlog-go/deserialization"
)

type RowImage []RowImageCell

// TODO: decide how to structure this relationally to table
type RowImageCell interface{}

type NullRowImageCell MysqlType
type NumberRowImageCell uint64
type FloatingPointNumberRowImageCell float32
type LargeFloatingPointNumberRowImageCell float64
type BlobRowImageCell []byte
type StringRowImageCell struct {
	Type  MysqlType
	Value string
}
type DurationRowImageCell time.Duration
type TimeRowImageCell struct {
	Type  MysqlType
	Value time.Time
}

func NewNullRowImageCell(mysqlType MysqlType) NullRowImageCell {
	return NullRowImageCell(mysqlType)
}

func DeserializeRowImageCell(r io.Reader, tableMap *TableMapEvent, columnIndex int) RowImageCell {
	fmt.Println("Deserializing cell:", tableMap.ColumnTypes[columnIndex].String())

	mysqlType := tableMap.ColumnTypes[columnIndex]

	switch mysqlType {
	// impossible cases
	case MYSQL_TYPE_ENUM, MYSQL_TYPE_NEWDATE, MYSQL_TYPE_SET,
		MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB:
		log.Fatal("Impossible type found in binlog!")

	case MYSQL_TYPE_TINY:
		v, err := deserialization.ReadUint8(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_SHORT:
		v, err := deserialization.ReadUint16(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_INT24:
		b, err := deserialization.ReadBytes(r, 3)
		fatalErr(err)

		return NumberRowImageCell(binary.LittleEndian.Uint32(b))

	case MYSQL_TYPE_LONG:
		v, err := deserialization.ReadUint32(r)
		fatalErr(err)

		fmt.Println("long:", v)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_LONGLONG:
		v, err := deserialization.ReadUint64(r)
		fatalErr(err)

		return NumberRowImageCell(v)

	case MYSQL_TYPE_FLOAT:
		var v float32
		b, err := deserialization.ReadBytes(r, 4)
		fatalErr(err)

		fatalErr(binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &v))

		return FloatingPointNumberRowImageCell(v)

	case MYSQL_TYPE_DOUBLE:
		// Not sure if C doubles convert to Go float64 properly
		var v float64
		b, err := deserialization.ReadBytes(r, 8)
		fatalErr(err)

		fatalErr(binary.Read(bytes.NewBuffer(b), binary.LittleEndian, &v))

		return LargeFloatingPointNumberRowImageCell(v)

	case MYSQL_TYPE_NULL:
		return NewNullRowImageCell(mysqlType)

	case MYSQL_TYPE_TIMESTAMP, MYSQL_TYPE_TIME, MYSQL_TYPE_DATETIME:
		log.Fatal("time fields disabled")

	case MYSQL_TYPE_DATE:
		date, err := deserialization.ReadDate(r)
		fatalErr(err)

		return TimeRowImageCell{
			Type:  mysqlType,
			Value: date,
		}

	case MYSQL_TYPE_TIME_V2:
		v, err := deserialization.ReadTimeV2(r)
		fatalErr(err)

		return DurationRowImageCell(v)

	case MYSQL_TYPE_DATETIME_V2, MYSQL_TYPE_TIMESTAMP_V2:
		var fn func(io.Reader, deserialization.Metadata) (time.Time, error)

		if mysqlType == MYSQL_TYPE_DATETIME_V2 {
			fn = deserialization.ReadDatetimeV2
			fmt.Println("datetime")
		} else {
			fn = deserialization.ReadTimestampV2
			fmt.Println("timestamp")
		}

		v, err := fn(r, tableMap.Metadata[columnIndex])
		fatalErr(err)

		return TimeRowImageCell{
			Type:  mysqlType,
			Value: v,
		}

	case MYSQL_TYPE_YEAR:
		v, err := deserialization.ReadUint8(r)
		fatalErr(err)

		return NumberRowImageCell(1900 + uint64(v))

	case MYSQL_TYPE_BIT:
		log.Fatal("BIT currently disabled")
		// metadata := tableMap.Metadata[columnIndex]

	case MYSQL_TYPE_NEWDECIMAL:
		// Not currently supported, may never be supported
		log.Fatal("NEWDECIMAL values are not supported.")

	case MYSQL_TYPE_VARCHAR:
		// If you see this and this has been working fine for a while, remove this
		tempErr := func(err error) {
			if err != nil {
				fmt.Println("!!! SOMETHING WENT WRONG IN VARCHAR")
				fmt.Println("Hint: lengthBytes var may be stored as packed int or based on max length (use metadata).")
				fatalErr(err)
			}
		}

		metadata := tableMap.Metadata[columnIndex]

		var length uint16
		var err error

		if metadata.MaxLength() <= 255 {
			smallLength, err := deserialization.ReadUint8(r)
			tempErr(err)

			length = uint16(smallLength)
		} else {
			length, err = deserialization.ReadUint16(r)
			tempErr(err)
		}

		b, err := deserialization.ReadBytes(r, int(length))
		tempErr(err)

		fmt.Println("max length:", metadata.MaxLength())
		fmt.Println("bytes length:", len(b))
		fmt.Println("bytes:", b)
		fmt.Println("bytes string:", string(b))

		return StringRowImageCell{
			Type:  mysqlType,
			Value: string(b),
		}

	case MYSQL_TYPE_STRING, MYSQL_TYPE_VAR_STRING:
		tempErr := func(err error) {
			if err != nil {
				fmt.Println("!!! SOMETHING WENT WRONG IN STRING/VAR_STRING")
				fatalErr(err)
			}
		}

		metadata := tableMap.Metadata[columnIndex]
		fmt.Println("** STRING METADATA:", metadata)
		fmt.Println("** STRING REALTYPE:", metadata.RealType())

		if metadata.RealType() == MYSQL_TYPE_ENUM {
			fmt.Println("************ DESERIALIZING ENUM")

			/*(
			lengthByte, err := deserialization.ReadByte(r)
			tempErr(err)

			fmt.Println("** LENGTH BYTE:", lengthByte)
			*/

			b, err := deserialization.ReadBytes(r, int(metadata.PackSize()))
			tempErr(err)

			enumString := string(b[0] + byte(48))

			fmt.Println("** ENUM READ:", enumString)
			return StringRowImageCell{
				Type:  mysqlType,
				Value: enumString,
			}
		}

		var length uint16
		var err error

		if metadata.PackSize() <= 255 {
			smallLength, err := deserialization.ReadUint8(r)
			tempErr(err)

			length = uint16(smallLength)
		} else {
			length, err = deserialization.ReadUint16(r)
			tempErr(err)
		}

		fmt.Println("** STRING READ LENGTH:", length)

		b, err := deserialization.ReadBytes(r, int(length))
		tempErr(err)

		fmt.Println("** BYTES READ:", b)
		fmt.Println("** STRING READ:", string(b))

		/*
			I'm still not completely sure if this is the correct way to do this.

			Here is a possible alternative for acquiring the length:

			length, err := ReadUint8(r)

			or

			metadata := tableMap.Metadata[columnIndex]
			packSize := metadata.PackSize()
			lengthBytes, err := ReadBytes(r, int(packSize))

		*/

		/*
			length, err := deserialization.ReadPackedInteger(r)
			tempErr(err)

			b, err := deserialization.ReadBytes(r, int(length))
			tempErr(err)
		*/

		return StringRowImageCell{
			Type:  mysqlType,
			Value: string(b),
		}

	case MYSQL_TYPE_BLOB:
		metadata := tableMap.Metadata[columnIndex]
		lengthBytes, err := deserialization.ReadBytes(r, int(metadata.PackSize()))
		fatalErr(err)

		if len(lengthBytes) != 8 {
			padding := make([]byte, 8-len(lengthBytes))
			for i := range padding {
				padding[i] = byte(0)
			}

			lengthBytes = append(lengthBytes, padding...)
		}

		length := binary.LittleEndian.Uint64(lengthBytes)

		b, err := deserialization.ReadBytes(r, int(length))
		fatalErr(err)

		fmt.Println("** BLOB LENGTH:", length)
		fmt.Println("** BLOB VALUE:", string(b))

		return BlobRowImageCell(b)

	case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_GEOMETRY:
		log.Fatal("Mysql type discovered but not supported at this time.")

	default:
		log.Fatal("Unsupported mysql type:", mysqlType)
	}

	return NewNullRowImageCell(mysqlType)
}
