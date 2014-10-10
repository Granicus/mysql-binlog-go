package connector

import (
	"fmt"
	"log"
	"net"
)

func NewConnection(uri, username, password string) (*PacketListener, error) {
	fmt.Println("dialing")

	conn, err := net.Dial("tcp", uri)

	if err != nil {
		return nil, err
	}

	listener := &PacketListener{
		conn: conn,
	}

	fmt.Println("reading greeting")

	// TODO: move over to must passing bytes
	b, err := listener.Read()

	if err != nil {
		return nil, err
	}

	fmt.Println("parsing greeting")

	greeting, err := ReadGreetingPacket(b)

	if err != nil {
		return nil, err
	}

	fmt.Println("Greeting:", greeting)

	if (greeting.ServerCapabilities & CAPABILITIES_PROTOCOL_41) > 0 {
		fmt.Println("protocol 41 supported")
		fmt.Println("authenticating")

		// TODO: research schema stuff
		err = listener.authenticate(greeting.AuthPluginName, username, password, "", greeting.AuthData, greeting.ServerCollation, greeting.ServerCapabilities)
		if err != nil {
			return nil, err
		}
	} else {
		log.Fatal("Sorry, we currently only support protocol 41")
	}

	return listener, nil
}
