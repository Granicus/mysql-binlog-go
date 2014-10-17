package binlog

import (
	"fmt"
	"io"
	"log"
	"os"

	"github.com/granicus/mysql-binlog-go/deserialization"
)

type EventDataDeserializeFunc func(*EventHeader) EventData

type Binlog struct {
	reader      io.ReadSeeker
	logVersion  uint8
	bytesLength int64
	events      []*Event
}

func NewBinlog(r io.ReadSeeker) *Binlog {
	b := &Binlog{
		reader:      r,
		bytesLength: -1,
		events:      []*Event{},
	}

	b.findLogVersion()
	b.indexEvents()

	return b
}

func OpenBinlog(filepath string) (*Binlog, error) {
	file, err := os.OpenFile(filepath, os.O_RDONLY, 0)

	if err != nil {
		return nil, err
	}

	stat, err := file.Stat()

	if err != nil {
		return nil, err
	}

	b := NewBinlog(file)
	b.bytesLength = stat.Size()
	return b, nil
}

func (b *Binlog) Events() []*Event {
	return b.events
}

func (b *Binlog) Event(i int) *Event {
	if len(b.events) > i || i < 0 {
		return nil
	}

	return b.events[i]
}

func (b *Binlog) GetPosition() (int64, error) {
	return b.reader.Seek(0, 1)
}

func (b *Binlog) Skip(n int64) error {
	_, err := b.reader.Seek(n, 1)
	return err
}

func (b *Binlog) SetPosition(newPosition int64) error {
	_, err := b.reader.Seek(newPosition, 0)
	return err

	/*
		currentPosition, err := b.GetPosition()
		fatalErr(err)

		positionDifference := newPosition - currentPosition

		// Not using math.Abs to avoid truncation when converting to float32
		absolutePositionDifference := positionDifference
		if absolutePositionDifference < 0 {
			absolutePositionDifference = -absolutePositionDifference
		}

		if newPosition > absolutePositionDifference {
			return b.Skip(positionDifference)
		}

		_, err = b.reader.Seek(newPosition, 0)
		return err
	*/
}

// assumes you want to feed in data
func (b *Binlog) SetInputStream(r io.ReadSeeker) {
	b.reader = r
	b.bytesLength = -1
}

func (b *Binlog) deserializeEventHeader(startPosition int64) *EventHeader {
	fatalErr(b.SetPosition(startPosition))

	return ReadEventHeader(b.reader)
}

func (b *Binlog) eventDataDeserializeFuncFor(eventType MysqlBinlogEventType) EventDataDeserializeFunc {
	switch eventType {
	case WRITE_ROWS_EVENTv0, UPDATE_ROWS_EVENTv0, DELETE_ROWS_EVENTv0,
		WRITE_ROWS_EVENTv1, UPDATE_ROWS_EVENTv1, DELETE_ROWS_EVENTv1,
		WRITE_ROWS_EVENTv2, UPDATE_ROWS_EVENTv2, DELETE_ROWS_EVENTv2:
		return b.DeserializeRowsEvent

	case TABLE_MAP_EVENT:
		return b.DeserializeTableMapEvent

	default:
		fmt.Println("unsupported event data deserialization:", eventType)

		return func(header *EventHeader) EventData { return &struct{}{} }
	}

	return nil
}

func (b *Binlog) deserializeEventData(startPosition int64, header *EventHeader) EventData {
	fatalErr(b.SetPosition(startPosition + 19))

	data := b.eventDataDeserializeFuncFor(header.Type)(header)
	b.SetPosition(int64(header.NextPosition))
	return data
}

func (b *Binlog) findTableMapEvent(tableId uint64) *TableMapEvent {
	for _, event := range b.events {
		if event.Type() == TABLE_MAP_EVENT && event.data == nil {
			data := event.Data().(*TableMapEvent)

			if data.TableId == tableId {
				return data
			}
		}
	}

	return nil
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

// Finds log version and move reader to end of first event
// assumes reader is still at beginning of file
func (b *Binlog) findLogVersion() {
	magic, err := deserialization.ReadBytes(b.reader, 4)

	if err != nil {
		log.Fatal("Something went wrong when reading magic number:", err)
	}

	if !checkBinlogMagic(magic) {
		log.Fatal("Binlog magic number was not correct. This is probably not a binlog.")
	}

	header := ReadEventHeader(b.reader)
	b.logVersion = determineLogVersion(header.Type, header.Length)

	// From here on out, we assume v4 events (for now)
	// this just errors out if it isn't v4
	if b.logVersion != 4 {
		panic("Sorry, this only supports v4 logs right now.")
	}

	fatalErr(b.SetPosition(int64(header.NextPosition)))
}

func (b *Binlog) indexEvent() error {
	startPosition, err := b.GetPosition()
	if err != nil {
		return err
	}

	// Skip to Type
	err = b.Skip(4)
	if err != nil {
		return err
	}

	typeByte, err := deserialization.ReadByte(b.reader)
	if err != nil {
		return err
	}

	eventType := MysqlBinlogEventType(typeByte)

	// Skip server id and length
	err = b.Skip(8)
	if err != nil {
		return err
	}

	nextPosition, err := deserialization.ReadUint32(b.reader)
	if err != nil {
		return err
	}

	// Skip the difference between where we should be and where we started, minus what we already read
	err = b.Skip((int64(nextPosition) - startPosition) - 17)
	if err != nil {
		return err
	}

	b.events = append(b.events, newIndexedEvent(b, eventType, startPosition))

	return nil
}

func (b *Binlog) indexEvents() {
	var err error
	for err != nil {
		err = b.indexEvent()
	}
}

/*

func (b *Binlog) NextEvent() *Event {
	pos, err := b.GetPosition()
	fatalErr(err)

	if pos == b.bytesLength {
		return nil
	}

	return ReadEvent(b.reader)
}

func eventTypesContains(eventTypes []MysqlBinlogEventType, value MysqlBinlogEventType) bool {
	for _, eventType := range eventTypes {
		if value == eventType {
			return true
		}
	}

	return false
}

func (b *Binlog) ReadEvents(eventTypes []MysqlBinlogEventType, callback func(*Event) bool) error {
	for {
		header := ReadEventHeader(b.reader)

		if eventTypesContains(eventTypes, header.Type) {
			event := header.DeserializeEventData(b.reader)

			if event == nil {
				return nil
			}

			if !callback(event) {
				break
			}
		} else if header.Type == TABLE_MAP_EVENT {
			// Table map events need deserialized regardless of if the client wants them
			header.DeserializeEventData(b.reader)
		} else {
			SkipEvent(b.reader, header)
		}
	}
}

func (b *Binlog) ReadEventsToPosition(position int64, eventTypes []MysqlBinlogEventType, callback func(*Event) bool) error {
	for {
		header := ReadEventHeader(b.reader)
	}
}

func (b *Binlog) ReadAllEvents(callback func(*Event) bool) error {
	allEvents = make([]MysqlBinlogEventType, 35)

	for i := 0; i < 35; i++ {
		allEvents[i] = MysqlBinlogEventType(i)
	}

	return ReadEvents(allEvents, callback)
}
*/
