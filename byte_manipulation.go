package byte_manipulation

func ReadInt16(h []byte, offset int) (rv uint16) {
	rv = uint16(h[offset]) << 8;
	rv |= uint16(h[offset+1]);
	return;
}
func ReadInt32(h []byte, offset int) (rv uint32) {
	rv = uint32(h[offset]) << 24;
	rv |= uint32(h[offset+1]) << 16;
	rv |= uint32(h[offset+2]) << 8;
	rv |= uint32(h[offset+3]);
	return;
}

func ReadInt64(h []byte, offset int) (rv uint64) {
	rv = uint64(ReadInt32(h, offset)) << 32;
	rv |= uint64(ReadInt32(h, offset+4));
	return;
}
