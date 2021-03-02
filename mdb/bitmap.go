package mdb

import (
"fmt"
)

// A Bit provides a integer based const representation of the ONE and ZERO bit
type Bit uint8

const (
	ZERO Bit = iota
	ONE
)

// GetBitFromByte gets the ith bit of a byte where the 0th bit is the most significant bit
// and the 7th bit is the least significant bit. Some examples:
//
//   - getBit(0b10000000, 7) == ZERO
//   - getBit(0b10000000, 0) == ONE
//   - getBit(0b01000000, 1) == ONE
//   - getBit(0b00100000, 1) == ZERO
func GetBitFromByte(b byte, i int) (Bit, error) {
	if 0 > i || i >= 8 {
		return 0, fmt.Errorf("out of range")
	}
	if (b & (0x1 << uint(i))) == 0 {
		return ZERO, nil
	}
	return ONE, nil
}

// GetBitFromBytes gets the ith bit of a byte array where the 0th bit is the most significant bit of the first byte. Some
// examples:
//	- getBit(new byte[]{0b10000000, 0b00000000}, 0) == ONE
//	- getBit(new byte[]{0b01000000, 0b00000000}, 1) == ONE
//	- getBit(new byte[]{0b00000000, 0b00000001}, 15) == ONE
func GetBitFromBytes(b []byte, i int) (Bit, error) {
	if len(b) < 0 || 0 > i && i >= len(b)*8 {
		fmt.Errorf("bytes.length = %d; i = %d.", len(b), i)
	}
	return GetBitFromByte(b[i/8], i%8)
}

// SetBit sets the ith bit of a byte where the 0th bit is the most significant bit
// and the 7th bit is the least significant bit. Some examples:
//   - setBit(0b00000000, 0, ONE) == ZERO
//   - setBit(0b00000000, 1, ONE) == ONE
//   - setBit(0b00000000, 2, ONE) == ONE
func SetBit(b byte, i int, bit Bit) (byte, error) {
	if i < 0 || i >= 8 {
		return 0, fmt.Errorf("out of range")
	}
	var mask = byte(1 << uint(i))
	switch bit {
	case ZERO:
		return byte(b & ^mask), nil
	case ONE:
		return byte(b | mask), nil
	default:
		return 0, fmt.Errorf("Unreachable")
	}
}

// SetBitInBytes sets the ith bit of a byte array where the 0th bit is the most significant
// bit of the first byte read using buf.get(). The position of the buffer is
// left unchanged. An example:
//
//   var bytes = make([]byte, 16);
//   setBit(bytes, 0, ONE);
//   bytes; // [0b10000000, 0b00000000]
//   setBit(bytes, 1, ONE);
//   bytes; // [0b11000000, 0b00000000]
//   setBit(bytes, 2, ONE);
//   bytes; // [0b11100000, 0b00000000]
//   setBit(bytes, 15, ONE);
//   bytes; // [0b11100000, 0b00000001]
//
// Note that setBit uses relative positioning based on the current position
// of the byte buffer. For example:
//
//   var bytes = make([]byte, 16);
//   // Advance the position of the buffer.
//   bytes[8];
//   // This sets the 8th bit of the buffer, not the first.
//   setBit(bytes, 0, ONE);
//   bytes; // [0b00000000, 0b10000000]
func SetBitInBytes(bytes []byte, i int, bit Bit) error {
	// bytes does not have a relative single byte get and set, so we have
	// to use the relative bulk get and set. Every time we read or write anything,
	// var b = buf.getByte(buf.position() + (i / 8))
	byt := bytes[i/8]
	b, err := SetBit(byt, i%8, bit)
	if err != nil {
		return err
	}

	bytes[i/8] = b
	return nil
}
