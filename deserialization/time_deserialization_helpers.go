package deserialization

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/granicus/mysql-binlog-go/bitset"
	"github.com/granicus/mysql-binlog-go/date"
)

// Metadata interface for ColumnMetadata structs from main package
// Add methods as needed
type Metadata interface {
	FractionalSecondsPrecision() uint8
}

func expandBitsetToBytesBigEndian(set bitset.Bitset, bitsetBitCount int) []byte {
	byteArray := make([]byte, int((bitsetBitCount+7)/8))

	for i := uint(bitsetBitCount); i > 0; i-- {
		if set.Bit(i) {
			byteArray[int((i+1)/8)] |= 1 << i
		}
	}

	return byteArray
}

func padBytesBigEndian(b []byte, count int) []byte {
	padding := make([]byte, count)
	for i := range padding {
		padding[i] = byte(0)
	}

	return append(padding, b...)
}

// We could do this with int((fsp + 1) / 2), but that is less clear
func fractionalSecondsPackSize(fsp int) int {
	switch fsp {
	case 1, 2:
		return 1
	case 3, 4:
		return 2
	case 5, 6:
		return 3
	}

	return 0
}

func readFractionalSeconds(r io.Reader, metadata Metadata) (int32, error) {
	packSize := fractionalSecondsPackSize(int(metadata.FractionalSecondsPrecision()))

	if packSize == 0 {
		return 0, nil
	}

	b, err := ReadBytes(r, packSize)
	if err != nil {
		return 0, err
	}

	// pad byte array so that it is 4 bytes in total
	buf := bytes.NewBuffer(padBytesBigEndian(b, 4-packSize))

	var fractionalSeconds int32
	binary.Read(buf, binary.BigEndian, &fractionalSeconds)

	return fractionalSeconds, nil
}

func removeFractionalSeconds(milliseconds uint) uint {
	return milliseconds - (milliseconds % 1000)
}

/*
DATE
====

3 bytes
Little Endian

15 bits = year
4 bits  = month
5 bits  = day

*/

func ReadDate(r io.Reader) (date.MysqlDate, error) {
	var year uint32
	var month uint32
	var day uint32

	b, err := ReadBytes(r, 3)
	if err != nil {
		return date.MysqlDate{}, err
	}

	// Pad to 4 bytes
	b = append(b, byte(0))

	value := binary.LittleEndian.Uint32(b)

	// [0-14]  1111 1111 1111 1110 0000 0000 (0xFFFE00)
	year = (value & 0xFFFE00) >> 9

	// [15-18] 0000 0000 0000 0001 1110 0000 (0x0001E0)
	month = (value & 0x0001E0) >> 5

	// [19-24] 0000 0000 0000 0000 0001 1111 (0x00001F)
	day = (value & 0x00001F)

	return date.NewMysqlDate(int(year), int(month), int(day)), nil
}

/*
TIME V2
=======

3 bytes
Big Endian

1 bit   = sign
1 bit   = reserved
10 bits = hour
6 bits  = minute
6 bits  = second

*/

func ReadTimeV2(r io.Reader) (date.MysqlTime, error) {
	// var sign int
	var hour uint32
	var minute uint32
	var second uint32

	b, err := ReadBytes(r, 3)
	if err != nil {
		return date.MysqlTime{}, err
	}

	// Pad to 4 bytes
	b = append([]byte{0}, b...)

	value := binary.BigEndian.Uint32(b)

	// if (value | 1) > 0 {
	// 	sign = 1
	// } else {
	// 	sign = -1
	// }

	// [2-11]  Mask: 0011 1111 1111 0000 0000 0000 (0x3FF000)
	hour = (value & 0x3FF000) >> 12

	// [12-17] Mask: 0000 0000 0000 1111 1100 0000 (0x000FC0)
	minute = (value & 0x000FC0) >> 6

	// [18-23] Mask: 0000 0000 0000 0000 0011 1111 (0x00003F)
	second = (value & 0x00003F)

	return date.NewMysqlTime(int(hour), int(minute), int(second)), nil
}

/*
TIMESTAMP V2
============

4 bytes + fsp bytes
Big Endian

*/

func ReadTimestampV2(r io.Reader, metadata Metadata) (time.Time, error) {
	millisecondBytes, err := ReadBytes(r, 4)
	if err != nil {
		return time.Time{}, err
	}

	millisecond := binary.BigEndian.Uint32(millisecondBytes)

	fractionalSeconds, err := readFractionalSeconds(r, metadata)
	if err != nil {
		return time.Time{}, err
	}

	fmt.Println("timestamp", time.Unix(int64(removeFractionalSeconds(uint(millisecond))), int64(fractionalSeconds)), millisecond, fractionalSeconds)

	return time.Unix(int64(removeFractionalSeconds(uint(millisecond))), int64(fractionalSeconds)), nil
}

/*
DATETIME V2
===========

5 bytes
Big Endian

1 bit   = sign
17 bits = year * 13 + month
5 bits  = day
5 bits  = hour
6 bits  = minute
6 bits  = second

NOTE: We completely ignore the sign for this type

*/

/*
func printUint64(n uint64) {
	for i := uint(0); i < 64; i++ { s := "0"
		if (n & (0x8000000000000000 >> i)) > 0 {
			s = "1"
		}

		fmt.Print(s)
	}
	fmt.Println()
}
*/

func ReadDatetimeV2(r io.Reader, metadata Metadata) (date.MysqlDatetime, error) {
	// Using uint64 for values to avoid variable truncation
	var yearMonth uint64
	var day uint64
	var hour uint64
	var minute uint64
	var second uint64

	b, err := ReadBytes(r, 5)
	if err != nil {
		return date.MysqlDatetime{}, err
	}

	// Pad to 8 bytes
	b = append([]byte{0, 0, 0}, b...)

	value := binary.BigEndian.Uint64(b)

	// [1-17] Mask:  0111 1111 1111 1111 1100 (0000 * 5) (0x7FFFC00000)
	yearMonth = (value & 0x7FFFC00000) >> 22

	// [18-22] Mask: (0000 * 4) 0011 1110 (0000 * 4) (0x00003E0000)
	day = (value & 0x00003E0000) >> 17

	// [23-27] Mask: (0000 * 5) 0001 1111 (0000 * 3) (0x000001F000)
	hour = (value & 0x000001F000) >> 12

	// [28-33] Mask: (0000 * 7) 1111 1100 0000 (0x0000000FC0)
	minute = (value & 0x0000000FC0) >> 6

	// [34-39] Mask: (0000 * 8) 0011 1111 (0x000000003F)
	second = (value & 0x000000003F)

	year := int(yearMonth / 13)
	month := int((yearMonth % 13) - 1)

	return date.NewMysqlDatetime(year, month, int(day), int(hour), int(minute), int(second)), nil
}
