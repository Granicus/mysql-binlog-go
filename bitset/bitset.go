package bitset

// Based from https://gist.github.com/willf/965762
// For info on basic bitwise operations: http://stackoverflow.com/a/47990/3830940

import (
	"log"
	"math"
)

// Keeping the uint64 from the original for now
// this may be changed later
type Bitset []uint64

func Make(maxSize uint) Bitset {
	if maxSize%64 == 0 {
		s := make(Bitset, maxSize/64)
		s.Clear()
		return s
	}
	s := make(Bitset, maxSize/64+1)
	s.Clear()
	return s
}

func MakeFromByteArray(bytes []byte, maxSize uint) Bitset {
	if int((maxSize+7)/8) > len(bytes) {
		log.Fatal("Bitset maxSize and []byte length mismatch")
	}

	bitset := Make(maxSize)

	for i := uint(0); i < uint(len(bytes)); i++ {
		block := bytes[i]

		for j := uint(0); j < 8; j++ {
			if ((block) & (1 << (j))) > 0 {
				bitset.SetBit((i * 8) + j)
			}
		}
	}

	return bitset
}

func (set Bitset) Bit(i uint) bool {
	return ((set[i/64] & (1 << (i % 64))) != 0)
}

func (set Bitset) SetBit(i uint) {
	set[i/64] |= (1 << (i % 64))
}

func (set Bitset) ClearBit(i uint) {
	set[i/64] &= (1 << (i % 64)) ^ math.MaxUint64
}

func (set Bitset) Clear() {
	for i, _ := range set {
		set[i] = 0
	}
}

func (set Bitset) Splice(start, end uint) Bitset {
	maxSize := end - start

	if maxSize <= 0 {
		log.Fatal("Bad start/end values for bitset splicing")
	}

	splicedSet := Make(maxSize)

	for i := uint(0); i < uint(maxSize); i++ {
		if set.Bit(start + i) {
			splicedSet.SetBit(i)
		}
	}

	return splicedSet
}

// strconv doesn't force zeroes to print, so hackyness, here I come
func (set Bitset) String() string {
	s := ""

	for i := uint(0); i < uint(len(set)*64); i++ {
		if set.Bit(i) {
			s += "1"
		} else {
			s += "0"
		}

		if (i+1)%8 == 0 {
			s += " "
		}
	}

	return s
}
