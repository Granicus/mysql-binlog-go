package deserialization

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/granicus/mysql-binlog-go/bitset"
)

// These constants may not be necessary later
// if these aren't being used much, just yank them out
const (
	NUL byte = iota
	SOH
	STX
	ETX
	EOT
	ENQ
	ACK
	BEL
	BS
	TAB
	LF
	VT
	FF
	CR
	SO
	SI
)

/*
GENERAL PARSING INFO
====================

MySQL's binlog always stores numbers in 32-bit Little Endian and are unsigned.
(Only exception is XID, which is stored in Big Endian in some versions)

Timestamps in MySQL binlog are stored as as numbers and a UNIX epoch offsets.

PLEASE NOTE
===========

All functions in this file assume the passed reader is already seeked
to the first byte in whatever it is attempting to read. To read an entire
event header, execute them in this order:

ReadTimestamp
ReadType
ReadServerId
ReadLength
ReadNextPosition
ReadFlags
(Extended v4 fields coming soon)

*/

func checkRead(n int, err error, bytes []byte) error {
	if err != nil {
		return err
	}

	if n != len(bytes) {
		return errors.New("Read mismatch")
		// return fmt.Errorf("Read mismatch: length=%v, bytes=%v", n, bytes)
	}

	return nil
}

// Interfaces passed in must be pointers
func ReadFromBinaryBuffer(b *bytes.Buffer, i interface{}) error {
	return binary.Read(b, binary.LittleEndian, i)
}

func Uint64FromBuffer(b *bytes.Buffer) (uint64, error) {
	var value uint64
	err := ReadFromBinaryBuffer(b, &value)
	return value, err
}

func Uint32FromBuffer(b *bytes.Buffer) (uint32, error) {
	var value uint32
	err := ReadFromBinaryBuffer(b, &value)
	return value, err
}

func Uint16FromBuffer(b *bytes.Buffer) (uint16, error) {
	var value uint16
	err := ReadFromBinaryBuffer(b, &value)
	return value, err
}

func Uint8FromBuffer(b *bytes.Buffer) (uint8, error) {
	var value uint8
	err := ReadFromBinaryBuffer(b, &value)
	return value, err
}

func ReadBytes(r io.Reader, length int) ([]byte, error) {
	b := make([]byte, length)
	n, err := r.Read(b)
	return b, checkRead(n, err, b)
}

func ReadString(r io.Reader, length int) (string, error) {
	b, err := ReadBytes(r, length)
	return string(b), err
}

func ReadByte(r io.Reader) (byte, error) {
	bytes, err := ReadBytes(r, 1)
	if err != nil {
		return byte(0), err
	}

	return bytes[0], nil
}

func ReadUint64(r io.Reader) (uint64, error) {
	b, err := ReadBytes(r, 8)
	if err != nil {
		return uint64(0), err
	}

	return Uint64FromBuffer(bytes.NewBuffer(b))
}

func ReadUint32(r io.Reader) (uint32, error) {
	b, err := ReadBytes(r, 4)
	if err != nil {
		return uint32(0), err
	}

	return Uint32FromBuffer(bytes.NewBuffer(b))
}

func ReadUint16(r io.Reader) (uint16, error) {
	b, err := ReadBytes(r, 2)
	if err != nil {
		return uint16(0), err
	}

	return Uint16FromBuffer(bytes.NewBuffer(b))
}

func ReadUint8(r io.Reader) (uint8, error) {
	b, err := ReadBytes(r, 1)
	if err != nil {
		return uint8(0), err
	}

	return Uint8FromBuffer(bytes.NewBuffer(b))
}

func ReadBitset(r io.Reader, bitCount int) (bitset.Bitset, error) {
	// Shift any remainder bits over current byte block, allow for casting truncation
	packSize := int((bitCount + 7) / 8)
	b, err := ReadBytes(r, packSize)
	if err != nil {
		return make(bitset.Bitset, 0), err
	}

	return bitset.MakeFromByteArray(b, uint(bitCount)), nil
}

func ReadNullTerminatedString(r io.Reader) (string, error) {
	// Inefficient method
	buffered := []byte{}

	for {
		b, err := ReadByte(r)
		if err != nil {
			return "", err
		}

		if b == NUL {
			break
		}

		buffered = append(buffered, b)
	}

	return string(buffered), nil

	/* More effecient, but buffers a bunch after and requires reset/reread
	reader := bufio.NewReader(r)

	versionBytes, err := reader.ReadBytes(byte(0))
	if err != nil {
		return "", err
	}

	return string(versionBytes[:len(versionBytes)-1]), nil
	*/
}

// This should probably return a time interface
func ReadTimestamp(r io.Reader) (uint32, error) {
	return ReadUint32(r)
}

func ReadType(r io.Reader) (byte, error) {
	return ReadByte(r)
}

func ReadServerId(r io.Reader) (uint32, error) {
	return ReadUint32(r)
}

func ReadLength(r io.Reader) (uint32, error) {
	return ReadUint32(r)
}

func ReadNextPosition(r io.Reader) (uint32, error) {
	return ReadUint32(r)
}

func ReadFlags(r io.Reader) ([]byte, error) {
	return ReadBytes(r, 2)
}

func ReadTableId(r io.Reader) (uint64, error) {
	b, err := ReadBytes(r, 6)
	fatalErr(err)

	// Have to pass 8 byte buffer, so append 6 bytes read to end of 2 '\0' value bytes
	buf := bytes.NewBuffer(append(b, []byte{NUL, NUL}...))

	return Uint64FromBuffer(buf)
}

/*
MYSQL PACKED INTEGERS
=====================

MySQL contains a special format of packed integers
that (somehow unsurprisingly) has virtually no
documentation. After a lot of searching around
and reading other libraries/MySQL source code,
I have figured out how it works.

The number of bytes in the packed integer is variable.
To determine how long the packed integer is, we have to
read the first byte and then use it's value to determine
how long the integer is. However, if it is outside of a
certain range, it will just be used by itself. Here is
how that is determined:

 <= 250: Range is 0-250. Just use this byte and don't read anymore.
  = 251: MySQL error code (not supposed to ever be used in binlogs).
  = 252: Range is 251-0xffff. Read 2 bytes.
  = 253: Range is 0xffff-0xffffff. Read 3 bytes.
  = 254: Range is 0xffffff-0xffffffffffffffff. Read 8 bytes.

It is significantly easier with Go's typing to just default
all values to uint64. As long as you don't store the events
in an array or anything, it shouldn't cause any issues though.

*/

func ReadPackedInteger(r io.Reader) (uint64, error) {
	firstByte, err := ReadUint8(r)
	fatalErr(err)

	if firstByte <= 250 {
		return uint64(firstByte), nil
	}

	bytesToRead := 0

	switch firstByte {
	case 251:
		// MySQL error code
		// something is wrong
		log.Fatal("Packed integer invalid value:", firstByte)
	case 252:
		bytesToRead = 2
	case 253:
		bytesToRead = 3
	case 254:
		bytesToRead = 8
	case 255:
		log.Fatal("Packed integer invalid value:", firstByte)
	}

	fmt.Println("Packed Int: Reading", bytesToRead, "more bytes")

	b, err := ReadBytes(r, bytesToRead)

	if err != nil {
		return uint64(0), err
	}

	return Uint64FromBuffer(bytes.NewBuffer(b))
}
