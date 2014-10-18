package binlog

import (
	"io/ioutil"
	"os/exec"
	"path/filepath"
)

type BinlogEventHandler func(event *Event)

// TODO: find a way not to have two different references to the AppendableBuffer
type StreamingBinlog struct {
	Binlog
	filepath     string
	listening    bool
	eventHandler BinlogEventHandler
	buffer       *AppendableBuffer
	appendFunc   func([]byte)
	chunkChan    chan *[]byte
	stopChan     chan bool
	cmd          *exec.Cmd
}

func NewStreamingBinlog(filepath string) *StreamingBinlog {
	contents, err := ioutil.ReadFile(filepath)
	if err != nil {
		panic(err)
	}

	buffer := NewAppendableBuffer(contents)

	log := &StreamingBinlog{
		Binlog:    *NewBinlog(buffer),
		filepath:  filepath,
		buffer:    buffer,
		chunkChan: make(chan *[]byte),
		stopChan:  make(chan bool),
	}

	return log
}

func (b *StreamingBinlog) SetEventHandler(handler BinlogEventHandler) {
	b.eventHandler = handler
}

func (b *StreamingBinlog) Stop() {
	b.stopChan <- true
}

// called by tail command stdout writer
func (b *StreamingBinlog) Write(p []byte) (int, error) {
	b.chunkChan <- &p
	return len(p), nil
}

func (b *StreamingBinlog) Tail() error {
	directory, filename := filepath.Split(b.filepath)

	b.cmd = exec.Command("tail", "-f", "-n", "0", filename)

	b.cmd.Dir = directory
	b.cmd.Stdout = b

	go b.listen()

	err := b.cmd.Run()

	if b.listening {
		b.Stop()
	}

	return err
}

func (b *StreamingBinlog) listen() {
	b.listening = true

	for {
		select {

		case <-b.stopChan:
			fatalErr(b.cmd.Process.Kill())
			break

		case bytesPointer := <-b.chunkChan:
			newPosition := int64(b.buffer.Length() - 1)
			b.buffer.Append(*bytesPointer)

			header := b.deserializeEventHeader(newPosition)
			data := b.deserializeEventData(newPosition, header)

			event := newDeserializedEvent(&b.Binlog, newPosition, header, data)
			b.events = append(b.events, event)

			if b.eventHandler != nil {
				b.eventHandler(event)
			}

		}
	}

	b.listening = false
}
