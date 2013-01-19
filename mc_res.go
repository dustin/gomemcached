package gomemcached

import (
	"encoding/binary"
	"fmt"
	"io"
)

// A memcached response
type MCResponse struct {
	// The command opcode of the command that sent the request
	Opcode CommandCode
	// The status of the response
	Status Status
	// The opaque sent in the request
	Opaque uint32
	// The CAS identifier (if applicable)
	Cas uint64
	// Extras, key, and body for this response
	Extras, Key, Body []byte
	// If true, this represents a fatal condition and we should hang up
	Fatal bool
}

// A debugging string representation of this response
func (res MCResponse) String() string {
	return fmt.Sprintf("{MCResponse status=%v keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

// Response as an error.
func (res MCResponse) Error() string {
	return fmt.Sprintf("MCResponse status=%v, msg: %s",
		res.Status, string(res.Body))
}

// True if this error represents a "not found" response.
func IsNotFound(e error) bool {
	if res, ok := e.(MCResponse); ok {
		return res.Status == KEY_ENOENT
	}
	if res, ok := e.(*MCResponse); ok {
		return res.Status == KEY_ENOENT
	}
	return false
}

// Number of bytes this response consumes on the wire.
func (res *MCResponse) Size() int {
	return HDR_LEN + len(res.Extras) + len(res.Key) + len(res.Body)
}

func (res *MCResponse) fillHeaderBytes(data []byte) int {
	pos := 0
	data[pos] = RES_MAGIC
	pos++
	data[pos] = byte(res.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2],
		uint16(len(res.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(res.Extras))
	pos++
	data[pos] = 0
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], uint16(res.Status))
	pos += 2

	// 8
	binary.BigEndian.PutUint32(data[pos:pos+4],
		uint32(len(res.Body)+len(res.Key)+len(res.Extras)))
	pos += 4

	// 12
	binary.BigEndian.PutUint32(data[pos:pos+4], res.Opaque)
	pos += 4

	// 16
	binary.BigEndian.PutUint64(data[pos:pos+8], res.Cas)
	pos += 8

	if len(res.Extras) > 0 {
		copy(data[pos:pos+len(res.Extras)], res.Extras)
		pos += len(res.Extras)
	}

	if len(res.Key) > 0 {
		copy(data[pos:pos+len(res.Key)], res.Key)
		pos += len(res.Key)
	}

	return pos
}

// Get just the header bytes for this response.
func (res *MCResponse) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(res.Extras)+len(res.Key))

	res.fillHeaderBytes(data)

	return data
}

// The actual bytes transmitted for this response.
func (res *MCResponse) Bytes() []byte {
	data := make([]byte, res.Size())

	pos := res.fillHeaderBytes(data)

	copy(data[pos:pos+len(res.Body)], res.Body)

	return data
}

// Send this response message across a writer.
func (res *MCResponse) Transmit(w io.Writer) (err error) {
	if len(res.Body) < 128 {
		_, err = w.Write(res.Bytes())
	} else {
		_, err = w.Write(res.HeaderBytes())
		if err == nil && len(res.Body) > 0 {
			_, err = w.Write(res.Body)
		}
	}
	return
}
