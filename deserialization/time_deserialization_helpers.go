package deserialization

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"

	"github.com/granicus/mysql-binlog-go/bitset"
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
		padding[i] = NUL
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

func ReadTimeV2(r io.Reader) (time.Duration, error) {
	set, err := ReadBitset(r, 24)
	if err != nil {
		return time.Duration(0), err
	}

	var sign int
	var hour uint16
	var minute uint8
	var second uint8

	if set.Bit(0) {
		sign = 1
	} else {
		sign = -1
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(2, 12), 10)), binary.BigEndian, &hour)
	if err != nil {
		return time.Duration(0), err
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(12, 18), 6)), binary.BigEndian, &minute)
	if err != nil {
		return time.Duration(0), err
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(18, 24), 6)), binary.BigEndian, &second)
	if err != nil {
		return time.Duration(0), err
	}

	return time.Duration(sign) * ((time.Hour * time.Duration(hour)) + (time.Minute * time.Duration(minute)) + (time.Second * time.Duration(second))), nil
}

/*
TIMESTAMP V2
============

4 bytes + fsp bytes
Big Endian

*/

func ReadTimestampV2(r io.Reader, metadata Metadata) (time.Time, error) {
	millisecond, err := ReadUint32(r)
	if err != nil {
		return time.Time{}, err
	}

	fractionalSeconds, err := readFractionalSeconds(r, metadata)
	if err != nil {
		return time.Time{}, err
	}

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

func ReadDatetimeV2(r io.Reader, metadata Metadata) (time.Time, error) {
	var yearMonth uint32
	var day uint8
	var hour uint8
	var minute uint8
	var second uint8

	set, err := ReadBitset(r, 48)
	if err != nil {
		return time.Time{}, err
	}

	yearMonthBytes := expandBitsetToBytesBigEndian(set.Splice(1, 18), 17) // 3 bytes

	// pad to 4 bytes
	err = binary.Read(bytes.NewBuffer(padBytesBigEndian(yearMonthBytes, 1)), binary.BigEndian, &yearMonth)
	if err != nil {
		return time.Time{}, err
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(18, 23), 5)), binary.BigEndian, &day)
	if err != nil {
		return time.Time{}, err
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(23, 28), 5)), binary.BigEndian, &hour)
	if err != nil {
		return time.Time{}, err
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(28, 34), 5)), binary.BigEndian, &minute)
	if err != nil {
		return time.Time{}, err
	}

	err = binary.Read(bytes.NewBuffer(expandBitsetToBytesBigEndian(set.Splice(34, 40), 5)), binary.BigEndian, &second)
	if err != nil {
		return time.Time{}, err
	}

	return time.Date(int(yearMonth/13), time.Month(yearMonth%13-1), int(day), int(hour), int(minute), int(second), 0, time.UTC), nil
}
