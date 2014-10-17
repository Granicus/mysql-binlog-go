// Referenced this unmerged go source patch: https://codereview.appspot.com/2106046
package binlog

import (
	"errors"
	"io"
)

type AppendableBuffer struct {
	buf []byte
	off int
}

func NewAppendableBuffer(buf []byte) *AppendableBuffer {
	return &AppendableBuffer{
		buf: buf,
		off: 0,
	}
}

func (b *AppendableBuffer) Length() int {
	return len(b.buf)
}

func (b *AppendableBuffer) Append(p []byte) {
	b.buf = append(b.buf, p...)
}

func (b *AppendableBuffer) Read(p []byte) (int, error) {
	if b.off >= len(b.buf) {
		return 0, io.EOF
	}
	n := copy(p, b.buf[b.off:])
	b.off += n
	return n, nil
}

func (b *AppendableBuffer) Seek(offset int64, whence int) (int64, error) {
	var newPosition int64

	switch whence {
	case 0:
		newPosition = 0

	case 1:
		newPosition = int64(b.off)

	case 2:
		newPosition = int64(len(b.buf))

	default:
		return int64(b.off), errors.New("Invalid whence passed to Seek")
	}

	newPosition += offset
	if newPosition < 0 || newPosition > int64(len(b.buf)) {
		return int64(b.off), io.EOF
	}

	b.off = int(newPosition)
	return newPosition, nil
}
