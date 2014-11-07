package binlog

// TODO: find a way not to have two different references to the AppendableBuffer
type StreamingBinlog struct {
	Binlog
	filepath   string
	listening  bool
	buffer     *AppendableBuffer
	appendFunc func([]byte)
	stopChan   chan bool
	tailer     *BinlogTailer
}

func StreamBinlog(filepath string, preloadBufferStopPosition int64) *StreamingBinlog {
	var err error
	log := new(StreamingBinlog)

	log.TableMapCollection = make(map[uint64]*TableMapEvent)
	log.bytesLength = -1
	log.events = []*Event{}
	log.filepath = filepath

	log.tailer, err = Tail(filepath)
	if err != nil {
		panic(err)
	}

	filePosition := int64(MAGIC_BYTES_LENGTH)
	existingContents := BINLOG_MAGIC[:]

	for filePosition < preloadBufferStopPosition {
		serializedEvent := log.tailer.ReadSerializedEvent()
		filePosition += int64(len(serializedEvent))
		existingContents = append(existingContents, serializedEvent...)
	}

	log.buffer = NewAppendableBuffer(existingContents)
	log.reader = log.buffer // set Binlog buffer

	log.Skip(int64(MAGIC_BYTES_LENGTH))
	log.indexEvents()

	return log
}

func (log *StreamingBinlog) Close() {
	log.tailer.Close()
}

func (log *StreamingBinlog) ReadEvent() *Event {
	eventPosition := int64(log.buffer.Length())

	serializedEvent := log.tailer.ReadSerializedEvent()
	log.buffer.Append(serializedEvent)

	err := log.SetPosition(eventPosition)
	if err != nil {
		panic(err)
	}

	log.indexEvent()
	return log.events[len(log.events)-1]
}
