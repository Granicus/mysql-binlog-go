package binlog

import (
	"fmt"
	"io"
	"log"
)

type SkipEvent struct{}

type SkipEventDeserializer struct{}

func (d *SkipEventDeserializer) Deserialize(reader io.ReadSeeker, eventInfo *Event) EventData {
	fmt.Println("Skipping by", eventInfo.DataLength)
	pos, err := reader.Seek(int64(eventInfo.DataLength), 1)

	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("Now at:", pos)

	return &SkipEvent{}
}
