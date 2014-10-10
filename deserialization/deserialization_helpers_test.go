package deserialization

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func checkErr(t *testing.T, err error) {
	if err != nil {
		t.Error(err)
	}
}

func TestReadBitset(t *testing.T) {
	b := bytes.NewBuffer([]byte{
		0xff, // 11111111
		0x00, // 00000000
		0x40, // 00000010
		0x55, // 10101010
	})

	bitset, err := ReadBitset(b, 32)
	checkErr(t, err)

	// message generator
	m := func(i uint, expected bool) string {
		return fmt.Sprintf("Expected bit %v to be %v in %v", i, expected, bitset)
	}

	// TODO: dry up this quick n' dirty test
	for i := uint(0); i < 32; i++ {
		switch {
		case i < 8:
			assert.True(t, bitset.Bit(i), m(i, true))
		case i < 16:
			assert.False(t, bitset.Bit(i), m(i, false))
		case i < 24 && i != 22:
			assert.False(t, bitset.Bit(i), m(i, false))
		case i == 22:
			assert.True(t, bitset.Bit(i), m(i, true))
		default: // last byte
			if i%2 == 0 {
				assert.True(t, bitset.Bit(i), m(i, true))
			} else {
				assert.False(t, bitset.Bit(i), m(i, false))
			}
		}
	}
}
