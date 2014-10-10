package connector

import (
	"bytes"
	"fmt"

	. "github.com/nholland94/mysql-binlog-go/deserialization"
)

type GreetingPacket struct {
	ProtocolVersion    uint8
	ServerVersion      string
	ConnectionId       uint32
	ServerCapabilities uint32
	ServerCollation    uint8
	ServerStatus       uint16
	AuthData           string
	AuthPluginName     string
}

// math helper (does including a package with one function only compile that function into your binary?)
func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

func ReadGreetingPacket(packetData []byte) (*GreetingPacket, error) {
	var err error
	packet := new(GreetingPacket)
	reader := bytes.NewBuffer(packetData)

	fmt.Println("packet:", packetData)
	fmt.Println("packet string:", string(packetData))
	fmt.Println("length:", len(packetData))

	packet.ProtocolVersion, err = ReadUint8(reader)
	if err != nil {
		return nil, err
	}

	packet.ServerVersion, err = ReadNullTerminatedString(reader)
	if err != nil {
		return nil, err
	}

	packet.ConnectionId, err = ReadUint32(reader)
	if err != nil {
		return nil, err
	}

	authDataPrefix, err := ReadString(reader, 8)
	if err != nil {
		return nil, err
	}

	_, err = ReadByte(reader) // null terminator
	if err != nil {
		return nil, err
	}

	capabilitiesLowerBytes, err := ReadUint16(reader)
	if err != nil {
		return nil, err
	}

	// If there is nothing left to read (easier than creating bufio reader?)
	if len(packetData) <= (1 + len(packet.ServerVersion) + 4 + 8 + 1 + 2) {
		fmt.Println("Shorter greeting packet than expected was recieved... is this an older version of mysql?")

		packet.AuthData = authDataPrefix
		packet.ServerCapabilities = uint32(capabilitiesLowerBytes)

		return packet, nil
	}

	packet.ServerCollation, err = ReadUint8(reader)
	if err != nil {
		return nil, err
	}

	packet.ServerStatus, err = ReadUint16(reader)
	if err != nil {
		return nil, err
	}

	capabilitiesUpperBytes, err := ReadUint16(reader)
	if err != nil {
		return nil, err
	}

	packet.ServerCapabilities = (uint32(capabilitiesUpperBytes) << 16) | uint32(capabilitiesLowerBytes)

	authDataLength := uint8(0)

	if (packet.ServerCapabilities & CAPABILITIES_PLUGIN_AUTH) > 0 {
		authDataLength, err = ReadUint8(reader)
		if err != nil {
			return nil, err
		}
	} else {
		// skip it; it should be NUL
		_, err = ReadByte(reader)
		if err != nil {
			return nil, err
		}
	}

	// reserved
	_, err = ReadBytes(reader, 10)
	if err != nil {
		return nil, err
	}

	if authDataLength > 0 && (packet.ServerCapabilities&CAPABILITIES_SECURE_CONNECTION) > 0 {
		length := max(13, int(authDataLength-8))

		authDataSuffix, err := ReadString(reader, length)
		if err != nil {
			return nil, err
		}

		packet.AuthData = authDataPrefix + authDataSuffix
	} else {
		packet.AuthData = authDataPrefix
	}

	packet.AuthPluginName, err = ReadNullTerminatedString(reader)
	if err != nil {
		return nil, err
	}

	return packet, nil
}
