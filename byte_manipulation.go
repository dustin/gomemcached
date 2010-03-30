package byte_manipulation

func ReadUint16(h []byte, offset int) (rv uint16) {
	rv = uint16(h[offset]) << 8
	rv |= uint16(h[offset+1])
	return
}
func ReadUint32(h []byte, offset int) (rv uint32) {
	rv = uint32(h[offset]) << 24
	rv |= uint32(h[offset+1]) << 16
	rv |= uint32(h[offset+2]) << 8
	rv |= uint32(h[offset+3])
	return
}

func ReadUint64(h []byte, offset int) (rv uint64) {
	rv = uint64(ReadUint32(h, offset)) << 32
	rv |= uint64(ReadUint32(h, offset+4))
	return
}

func WriteUint16(n uint16) (rv []byte) {
	rv = make([]byte, 2)
	rv[0] = uint8(n >> 8)
	rv[1] = uint8(n & 0xff)
	return
}

func WriteUint32(n uint32) (rv []byte) {
	rv = make([]byte, 4)
	rv[0] = uint8(n >> 24)
	rv[1] = uint8((n >> 16) & 0xff)
	rv[2] = uint8((n >> 8) & 0xff)
	rv[3] = uint8(n & 0xff)
	return
}

func WriteUint64(n uint64) (rv []byte) {
	rv = make([]byte, 8)
	rv[0] = uint8(n >> 56)
	rv[1] = uint8((n >> 48) & 0xff)
	rv[2] = uint8((n >> 40) & 0xff)
	rv[3] = uint8((n >> 32) & 0xff)
	rv[4] = uint8((n >> 24) & 0xff)
	rv[5] = uint8((n >> 16) & 0xff)
	rv[6] = uint8((n >> 8) & 0xff)
	rv[7] = uint8(n & 0xff)
	return
}
