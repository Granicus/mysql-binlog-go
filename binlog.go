package binlog

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/granicus/mysql-binlog-go/deserialization"
)

// Determines the binlog version from the first event
// http://dev.mysql.com/doc/internals/en/determining-binary-log-version.html
func determineLogVersion(typeCode MysqlBinlogEventType, length uint32) uint8 {
	if typeCode != START_EVENT_V3 && typeCode != FORMAT_DESCRIPTION_EVENT {
		return 3
	} else if typeCode == START_EVENT_V3 {
		if length < 75 {
			return 1
		} else {
			return 3
		}
	} else if typeCode == FORMAT_DESCRIPTION_EVENT {
		return 4
	} else {
		log.Fatal(fmt.Sprintf("Could not determine log version from: [%v, %v]", typeCode, length))
	}

	return 0
}

type Binlog struct {
	reader      io.ReadSeeker
	logVersion  uint8
	bytesLength int64
}

func OpenBinlog(filepath string) (*Binlog, error) {
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0)

	if err != nil {
		return nil, err
	}

	/*
		fmt.Println("unsigned")
		for i := 0; i < 19; i++ {
			b := make([]byte, 1)
			_, err := file.Read(b)
			fatalErr(err)
			fmt.Printf("%v =   %b - %v\n", i, uint8(b[0]), uint8(b[0]))
		}

		file.Seek(0, 0)

		log.Fatal("done")
	*/

	stat, err := file.Stat()

	if err != nil {
		return nil, err
	}

	b := &Binlog{
		reader:      file,
		logVersion:  0,
		bytesLength: stat.Size(),
	}

	b.mustFindLogVersion()

	return b, nil
}

/*
ABOUT BINLOG VERSION
====================

The binlog version can be determined by the first event.
There are a mulititude of factors in this, due to changes
throughout versions of MySQL.

The two important factors in this are the EVENT_TYPE and
EVENT_LENGTH variables. We don't deserialize the whole
event because we have not yet determined the version
to base our header deserialization on. Luckily, the
first few fields in the header are always the same,
no matter which version:

4 bytes = timestamp
1 byte  = type
4 bytes = server id
4 bytes = event size

Everything after that point is version dependent, however.

We also take this time to check the magic bytes. Every binlog,
no matter which version, starts with 4 magic bytes that are always
0xfe followed by 'b', 'i', and 'n'. This is normally ignored,
but we check it to make sure this is actually a binlog before
we try and parse things we shouldn't. This will probably be
moved soon, along with this message.

*/

// Finds log version and move reader to end of first event
// assumes reader is still at beginning of file
func (b *Binlog) mustFindLogVersion() {
	magic, err := deserialization.ReadBytes(b.reader, 4)

	if err != nil {
		log.Fatal("Something went wrong when reading magic number:", err)
	}

	if !checkBinlogMagic(magic) {
		log.Fatal("Binlog magic number was not correct. This is probably not a binlog.")
	}

	// Skip timestamp
	fatalErr(b.Skip(4))
	typeCode, err := deserialization.ReadType(b.reader)

	if err != nil {
		log.Fatal("Failed to read type_byte:", err)
	}

	fatalErr(b.SetPosition(EVENT_LEN_OFFSET + 4))
	length, err := deserialization.ReadLength(b.reader)

	if err != nil {
		log.Fatal("Failed to read event_length:", err)
	}

	b.logVersion = determineLogVersion(MysqlBinlogEventType(typeCode), length)

	// From here on out, we assume v4 events (for now)
	// this just errors out if it isn't v4
	if b.logVersion != 4 {
		log.Fatal("Sorry, this only supports v4 logs right now.", b.logVersion)
	}

	fatalErr(b.SetPosition(EVENT_NEXT_OFFSET + 4))
	nextPos, err := deserialization.ReadNextPosition(b.reader)

	if err != nil {
		log.Fatal("Failed to read event_length:", err)
	}

	fatalErr(b.SetPosition(int64(nextPos)))

	fmt.Println("Set position to ", nextPos)
}

func (b *Binlog) GetPosition() (int64, error) {
	return b.reader.Seek(0, 1)
}

func (b *Binlog) SetPosition(n int64) error {
	// does this need to be n - 1?
	_, err := b.reader.Seek(n, 0)
	return err
}

func (b *Binlog) Skip(n int64) error {
	_, err := b.reader.Seek(n, 1)
	return err
}

// TODO: error handling
func (b *Binlog) NextEvent() *Event {
	pos, err := b.GetPosition()
	fatalErr(err)

	if pos == b.bytesLength {
		return nil
	}

	return ReadEvent(b.reader)
}

// assumes you want to feed in data
func (b *Binlog) SetInputStream(r io.ReadSeeker) {
	b.reader = r
	b.bytesLength = -1
}
