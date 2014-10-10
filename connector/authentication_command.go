package connector

import (
	"bytes"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
)

type AuthenticationCommand struct {
	schema             string
	username           string
	password           string
	salt               string
	clientCapabilities uint32
	collation          uint8
	authPluginName     string
}

func (cmd *AuthenticationCommand) PacketNumber() uint8 {
	return uint8(1)
}

// Trying a different code style here
// Let me know if you think this looks alright/which way you prefer
func (cmd *AuthenticationCommand) Body() ([]byte, error) {
	var err error
	var n int
	buf := new(bytes.Buffer)
	// capabilities := cmd.clientCapabilities
	capabilities := uint32(
		CAPABILITIES_PROTOCOL_41 |
			CAPABILITIES_LONG_PASSWORD |
			CAPABILITIES_LONG_FLAG |
			CAPABILITIES_TRANSACTIONS |
			CAPABILITIES_SECURE_CONNECTION)

	// CLIENT CAPABILITIES
	{
	}
	/*
		{
			if cmd.clientCapabilities == 0 {
				capabilities = CAPABILITIES_LONG_FLAG | CAPABILITIES_PROTOCOL_41 | CAPABILITIES_SECURE_CONNECTION
			}

			if cmd.schema == "" && (capabilities & CAPABILITIES_CONNECT_WITH_DB) > 0 {
				capabilities = capabilities & ^CAPABILITIES_CONNECT_WITH_DB
			}

			fmt.Printf("%b\n", capabilities)

			if (capabilities & CAPABILITIES_CONNECT_ATTRS) > 0 {
				capabilities = capabilities & ^CAPABILITIES_CONNECT_ATTRS
			}

			fmt.Printf("%b\n", capabilities)
			fmt.Printf("%b\n", capabilities & CAPABILITIES_CONNECT_ATTRS)
			fmt.Printf("%v\n", (capabilities & CAPABILITIES_CONNECT_ATTRS) > 0)


			err = binary.Write(buf, binary.LittleEndian, capabilities)
			if err != nil {
				return []byte{}, err
			}
		}
	*/

	{
		fmt.Println("capabilities check:")

		if (capabilities & CAPABILITIES_PLUGIN_AUTH_LENENC_CLIENT_DATA) > 0 {
			fmt.Println("  length encoded authentication")
		}

		if (capabilities & CAPABILITIES_SECURE_CONNECTION) > 0 {
			fmt.Println("  secure connection")
		}

		if (capabilities & CAPABILITIES_CONNECT_WITH_DB) > 0 {
			fmt.Println("  connect with db")
		}

		if (capabilities & CAPABILITIES_PLUGIN_AUTH) > 0 {
			fmt.Println("  plugin auth")
		}

		if (capabilities & CAPABILITIES_CONNECT_ATTRS) > 0 {
			fmt.Println("  connect attrs")
		}
	}

	// MAXIMUM RESPONSE LENGTH
	{
		err = binary.Write(buf, binary.LittleEndian, uint32((16*1024*1024)-1))
		if err != nil {
			return []byte{}, err
		}
	}

	// CHARACTER SET
	{
		err = binary.Write(buf, binary.LittleEndian, &cmd.collation)
		if err != nil {
			return []byte{}, err
		}
	}

	// RESERVED
	{
		// skip these values
		for i := 0; i < 23; i++ {
			buf.WriteByte(byte(0))
		}
	}

	// USERNAME
	{
		n, err = buf.WriteString(cmd.username)
		if err != nil {
			return []byte{}, err
		}
		if n != len(cmd.username) {
			return []byte{}, errors.New("Failed to write packet username")
		}

		// null terminated
		err = buf.WriteByte(byte(0))
		if err != nil {
			return []byte{}, err
		}
	}

	// PASSWORD HASHING
	{
		passwordSha := cmd.passwordCompatibleWithMySql41()

		if (capabilities&CAPABILITIES_PLUGIN_AUTH_LENENC_CLIENT_DATA) > 0 ||
			(capabilities&CAPABILITIES_SECURE_CONNECTION) > 0 {
			// Maybe this is supposed to be a packed int for auth_lenenc?
			err = binary.Write(buf, binary.LittleEndian, uint8(len(passwordSha)))
			if err != nil {
				return []byte{}, err
			}

			n, err = buf.Write(passwordSha)
			if err != nil {
				return []byte{}, err
			}
			if n != len(passwordSha) {
				return []byte{}, errors.New("Failed to write packet password sha")
			}
		} else {
			n, err = buf.Write(passwordSha)
			if err != nil {
				return []byte{}, err
			}
			if n != len(passwordSha) {
				return []byte{}, errors.New("Failed to write packet password sha")
			}

			err = buf.WriteByte(byte(0))
			if err != nil {
				return []byte{}, err
			}
		}
	}

	// DATABASE
	{
		if (capabilities & CAPABILITIES_CONNECT_WITH_DB) > 0 {
			n, err = buf.WriteString(cmd.schema)
			if err != nil {
				return []byte{}, err
			}
			if n != len(cmd.schema) {
				return []byte{}, errors.New("Failed to write database name")
			}

			// null terminated
			err = buf.WriteByte(byte(0))
			if err != nil {
				return []byte{}, err
			}
		}
	}

	// PLUGIN NAME
	/*
		{
			if (capabilities & CAPABILITIES_PLUGIN_AUTH) > 0 {
				n, err = buf.WriteString(cmd.authPluginName)
				if err != nil {
					return []byte{}, err
				}
				if n != len(cmd.authPluginName) {
					return []byte{}, errors.New("Failed to write plugin auth name")
				}

				// null terminated
				err = buf.WriteByte(byte(0))
				if err != nil {
					return []byte{}, err
				}
			}
		}
	*/

	// CONNECT ATTRS
	{
		if (capabilities & CAPABILITIES_CONNECT_ATTRS) > 0 {
			return []byte{}, errors.New("Connect attrs not supported")
		}
	}

	return buf.Bytes(), nil
}

// password hashing/salting helpers
func (cmd *AuthenticationCommand) mustDigestSha1(b []byte) []byte {
	h := sha1.New()
	n, err := h.Write(b)

	if err != nil {
		log.Fatal("Failed to write to sha1 digester")
	}

	if n != len(b) {
		log.Fatal("Password digester write length mismatch")
	}

	return h.Sum(nil)
}

func xor(a, b []byte) []byte {
	r := make([]byte, len(a))

	for i := 0; i < len(r); i++ {
		r[i] = byte(a[i] ^ b[i])
	}

	return r
}

// Borrowed from https://github.com/ziutek/mymysql/blob/7ca4f179436c56d1bdefcb1ce9f1f74ea42a8b79/native/passwd.go#L10
//  SHA1(SHA1(SHA1(password)), scramble) XOR SHA1(password)
func (cmd *AuthenticationCommand) passwordCompatibleWithMySql41() []byte {
	crypt := sha1.New()
	crypt.Write([]byte(cmd.password))
	stg1Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write(stg1Hash)
	stg2Hash := crypt.Sum(nil)

	crypt.Reset()
	crypt.Write([]byte(cmd.salt))
	crypt.Write(stg2Hash)
	stg3Hash := crypt.Sum(nil)

	return xor(stg3Hash, stg1Hash)
}
