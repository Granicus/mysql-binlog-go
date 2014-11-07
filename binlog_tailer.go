package binlog

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/aybabtme/tailf"
)

type BinlogTailer struct {
	tailReader   io.ReadCloser
	lastPosition int
}

func Tail(filepath string) (*BinlogTailer, error) {
	var err error
	tailer := new(BinlogTailer)

	tailer.tailReader, err = tailf.Follow(filepath, true)
	if err != nil {
		return nil, err
	}

	tailer.lastPosition = 4

	return tailer, tailer.readMagicBytes()
}

func (tailer *BinlogTailer) readMagicBytes() error {
	magicBytes := make([]byte, MAGIC_BYTES_LENGTH)
	n, err := tailer.tailReader.Read(magicBytes)

	if n != MAGIC_BYTES_LENGTH {
		return fmt.Errorf("Failed to read magic bytes: read length mismatch")
	}

	if err != nil {
		return fmt.Errorf("Failed to read magic bytes: %v", err.Error())
	}

	/*
		TODO: can't compare []byte to [4]byte

		if magicBytes != BINLOG_MAGIC {
			return fmt.Errorf("Failed to read magic bytes: got %v, expected %v", magicBytes, BINLOG_MAGIC)
		}
	*/

	return nil
}

func (tailer *BinlogTailer) Close() {
	tailer.tailReader.Close()
}

// recursively read remaining bytes until a certain length of bytes are read
func (tailer *BinlogTailer) readByLength(preloadedBytes []byte, length int) []byte {
	remainingBytes := make([]byte, length-len(preloadedBytes))
	n, err := tailer.tailReader.Read(remainingBytes)

	if err != nil {
		panic(fmt.Errorf("Failed to read serialized event header: %v", err))
	}

	if n != len(remainingBytes) {
		return tailer.readByLength(append(preloadedBytes, remainingBytes[:n]...), length)
	}

	return append(preloadedBytes, remainingBytes...)
}

// errors are currently non-recoverable
func (tailer *BinlogTailer) ReadSerializedEvent() []byte {
	headerBytes := tailer.readByLength([]byte{}, EVENT_HEADER_LENGTH)

	nextPosition := int(binary.LittleEndian.Uint32(headerBytes[EVENT_NEXT_OFFSET:EVENT_FLAGS_OFFSET]))
	dataLength := nextPosition - tailer.lastPosition - len(headerBytes)

	dataBytes := tailer.readByLength([]byte{}, dataLength)

	tailer.lastPosition = nextPosition
	return append(headerBytes, dataBytes...)
}
