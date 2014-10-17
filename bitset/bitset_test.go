package bitset

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBitsetSetClearBit(t *testing.T) {
	set := Make(8)

	for i := uint(0); i < uint(8); i++ {
		set.SetBit(i)
		assert.True(t, set.Bit(i))
		set.ClearBit(i)
		assert.False(t, set.Bit(i))
	}
}

func TestBitsetCount(t *testing.T) {
	set := Make(8)

	set.SetBit(0)
	set.SetBit(4)
	set.SetBit(7)

	assert.Equal(t, 3, set.Count())
}

func TestBitsetClear(t *testing.T) {
	set := Make(8)

	set.SetBit(2)
	set.SetBit(5)
	set.Clear()

	assert.False(t, set.Bit(2))
	assert.False(t, set.Bit(5))
}
