package connector

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
)

type Command interface {
	Body() ([]byte, error)
	PacketNumber() uint8
}

type PacketListener struct {
	conn net.Conn
}

// TODO: refactor with deserialization package once that is refactored out
func (p *PacketListener) Read() ([]byte, error) {
	lengthBytes := make([]byte, 3)

	n, err := p.conn.Read(lengthBytes)

	if err != nil {
		return []byte{}, err
	}
	if n != len(lengthBytes) {
		return []byte{}, errors.New("Length mismatch in read!")
	}

	// pad the bytes
	lengthBytes = append(lengthBytes, byte(0))

	var length uint32
	err = binary.Read(bytes.NewBuffer(lengthBytes), binary.LittleEndian, &length)

	if err != nil {
		return []byte{}, err
	}

	// skip sequence
	_, err = p.conn.Read(make([]byte, 1))

	if err != nil {
		return []byte{}, err
	}

	data := make([]byte, length)

	n, err = p.conn.Read(data)

	if err != nil {
		return []byte{}, err
	}
	if n != len(data) {
		return []byte{}, errors.New("Length mismatch in read!")
	}

	return data, nil
}

func (p *PacketListener) Write(command Command) error {
	body, err := command.Body()

	{
		fmt.Println("body:", string(body))

		for i, b := range body {
			fmt.Print(b)
			fmt.Print(" ")

			if (i+1)%8 == 0 {
				fmt.Print("\n")
			}
		}
	}

	if err != nil {
		return err
	}

	lengthBytes := make([]byte, 3)

	err = binary.Write(bytes.NewBuffer(lengthBytes), binary.LittleEndian, uint32(len(body)))

	if err != nil {
		return err
	}

	n, err := p.conn.Write(lengthBytes)

	if err != nil {
		return err
	}
	if n != len(lengthBytes) {
		return errors.New("Connection write length mismatch")
	}

	err = binary.Write(p.conn, binary.LittleEndian, command.PacketNumber())

	if err != nil {
		return err
	}

	n, err = p.conn.Write(body)

	if err != nil {
		return err
	}
	if n != len(body) {
		return errors.New("Connection write length mismatch")
	}

	return nil
}

func (p *PacketListener) authenticate(authPluginName, username, password, schema, salt string, collation uint8, capabilities uint32) error {
	authCmd := &AuthenticationCommand{
		schema:             schema,
		username:           username,
		password:           password,
		salt:               salt,
		clientCapabilities: capabilities,
		collation:          collation,
		authPluginName:     authPluginName,
	}

	err := p.Write(authCmd)

	if err != nil {
		return err
	}

	result, err := p.Read()

	if err != nil {
		return err
	}

	/*
		auth result packet contains status in first byte

		0 is ok
		255 is error
	*/

	if result[0] != 0x00 {
		if result[0] == 0xff {
			// decode error packet and handle it
			fmt.Println("error packet:", result)
			fmt.Println("error packet string:", string(result))

			return errors.New("UNIMPLEMENTED ERROR PACKET")
		}

		return errors.New("Unexpected authentication result" + string(result[0]))
	}

	return nil
}
