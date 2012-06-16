// Memcached binary protocol packet formats and constants.
package gomemcached

import (
	"encoding/binary"
	"fmt"
)

const (
	REQ_MAGIC = 0x80
	RES_MAGIC = 0x81
)

type CommandCode uint8

const (
	GET        = CommandCode(0x00)
	SET        = CommandCode(0x01)
	ADD        = CommandCode(0x02)
	REPLACE    = CommandCode(0x03)
	DELETE     = CommandCode(0x04)
	INCREMENT  = CommandCode(0x05)
	DECREMENT  = CommandCode(0x06)
	QUIT       = CommandCode(0x07)
	FLUSH      = CommandCode(0x08)
	GETQ       = CommandCode(0x09)
	NOOP       = CommandCode(0x0a)
	VERSION    = CommandCode(0x0b)
	GETK       = CommandCode(0x0c)
	GETKQ      = CommandCode(0x0d)
	APPEND     = CommandCode(0x0e)
	PREPEND    = CommandCode(0x0f)
	STAT       = CommandCode(0x10)
	SETQ       = CommandCode(0x11)
	ADDQ       = CommandCode(0x12)
	REPLACEQ   = CommandCode(0x13)
	DELETEQ    = CommandCode(0x14)
	INCREMENTQ = CommandCode(0x15)
	DECREMENTQ = CommandCode(0x16)
	QUITQ      = CommandCode(0x17)
	FLUSHQ     = CommandCode(0x18)
	APPENDQ    = CommandCode(0x19)
	PREPENDQ   = CommandCode(0x1a)
	RGET       = CommandCode(0x30)
	RSET       = CommandCode(0x31)
	RSETQ      = CommandCode(0x32)
	RAPPEND    = CommandCode(0x33)
	RAPPENDQ   = CommandCode(0x34)
	RPREPEND   = CommandCode(0x35)
	RPREPENDQ  = CommandCode(0x36)
	RDELETE    = CommandCode(0x37)
	RDELETEQ   = CommandCode(0x38)
	RINCR      = CommandCode(0x39)
	RINCRQ     = CommandCode(0x3a)
	RDECR      = CommandCode(0x3b)
	RDECRQ     = CommandCode(0x3c)

	TAP_CONNECT          = CommandCode(0x40)
	TAP_MUTATION         = CommandCode(0x41)
	TAP_DELETE           = CommandCode(0x42)
	TAP_FLUSH            = CommandCode(0x43)
	TAP_OPAQUE           = CommandCode(0x44)
	TAP_VBUCKET_SET      = CommandCode(0x45)
	TAP_CHECKPOINT_START = CommandCode(0x46)
	TAP_CHECKPOINT_END   = CommandCode(0x47)
)

type Status uint16

const (
	SUCCESS         = Status(0x00)
	KEY_ENOENT      = Status(0x01)
	KEY_EEXISTS     = Status(0x02)
	E2BIG           = Status(0x03)
	EINVAL          = Status(0x04)
	NOT_STORED      = Status(0x05)
	DELTA_BADVAL    = Status(0x06)
	NOT_MY_VBUCKET  = Status(0x07)
	UNKNOWN_COMMAND = Status(0x81)
	ENOMEM          = Status(0x82)
)

const (
	BACKFILL          = 0x01
	DUMP              = 0x02
	LIST_VBUCKETS     = 0x04
	TAKEOVER_VBUCKETS = 0x08
	SUPPORT_ACK       = 0x10
	REQUEST_KEYS_ONLY = 0x20
	CHECKPOINT        = 0x40
	REGISTERED_CLIENT = 0x80
)

// A Memcached Request
type MCRequest struct {
	// The command being issued
	Opcode CommandCode
	// The CAS (if applicable, or 0)
	Cas uint64
	// An opaque value to be returned with this request
	Opaque uint32
	// The vbucket to which this command belongs
	VBucket uint16
	// Command extras, key, and body
	Extras, Key, Body []byte
}

// The number of bytes this request requires.
func (req *MCRequest) Size() int {
	return HDR_LEN + len(req.Extras) + len(req.Key) + len(req.Body)
}

// A debugging string representation of this request
func (req MCRequest) String() string {
	return fmt.Sprintf("{MCRequest opcode=%s, bodylen=%d, key='%s'}",
		req.Opcode, len(req.Body), req.Key)
}

func (req *MCRequest) fillHeaderBytes(data []byte) int {

	pos := 0
	data[pos] = REQ_MAGIC
	pos++
	data[pos] = byte(req.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2],
		uint16(len(req.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(req.Extras))
	pos++
	// data[pos] = 0
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2], req.VBucket)
	pos += 2

	// 8
	binary.BigEndian.PutUint32(data[pos:pos+4],
		uint32(len(req.Body)+len(req.Key)+len(req.Extras)))
	pos += 4

	// 12
	binary.BigEndian.PutUint32(data[pos:pos+4], req.Opaque)
	pos += 4

	// 16
	if req.Cas != 0 {
		binary.BigEndian.PutUint64(data[pos:pos+8], req.Cas)
	}
	pos += 8

	if len(req.Extras) > 0 {
		copy(data[pos:pos+len(req.Extras)], req.Extras)
		pos += len(req.Extras)
	}

	if len(req.Key) > 0 {
		copy(data[pos:pos+len(req.Key)], req.Key)
		pos += len(req.Key)
	}
	return pos
}

// The wire representation of the header (with the extras and key)
func (req *MCRequest) HeaderBytes() []byte {
	data := make([]byte, HDR_LEN+len(req.Extras)+len(req.Key))

	req.fillHeaderBytes(data)

	return data
}

// The wire representation of this request.
func (req *MCRequest) Bytes() []byte {
	data := make([]byte, req.Size())

	pos := req.fillHeaderBytes(data)

	if len(req.Body) > 0 {
		copy(data[pos:pos+len(req.Body)], req.Body)
	}

	return data
}

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

// An internal representation of an item.
type MCItem struct {
	Cas               uint64
	Flags, Expiration uint32
	Data              []byte
}

// Number of bytes in a binary protocol header.
const HDR_LEN = 24

// Mapping of CommandCode -> name of command (not exhaustive)
var CommandNames map[CommandCode]string

var StatusNames map[Status]string

func init() {
	CommandNames = make(map[CommandCode]string)
	CommandNames[GET] = "GET"
	CommandNames[SET] = "SET"
	CommandNames[ADD] = "ADD"
	CommandNames[REPLACE] = "REPLACE"
	CommandNames[DELETE] = "DELETE"
	CommandNames[INCREMENT] = "INCREMENT"
	CommandNames[DECREMENT] = "DECREMENT"
	CommandNames[QUIT] = "QUIT"
	CommandNames[FLUSH] = "FLUSH"
	CommandNames[GETQ] = "GETQ"
	CommandNames[NOOP] = "NOOP"
	CommandNames[VERSION] = "VERSION"
	CommandNames[GETK] = "GETK"
	CommandNames[GETKQ] = "GETKQ"
	CommandNames[APPEND] = "APPEND"
	CommandNames[PREPEND] = "PREPEND"
	CommandNames[STAT] = "STAT"
	CommandNames[SETQ] = "SETQ"
	CommandNames[ADDQ] = "ADDQ"
	CommandNames[REPLACEQ] = "REPLACEQ"
	CommandNames[DELETEQ] = "DELETEQ"
	CommandNames[INCREMENTQ] = "INCREMENTQ"
	CommandNames[DECREMENTQ] = "DECREMENTQ"
	CommandNames[QUITQ] = "QUITQ"
	CommandNames[FLUSHQ] = "FLUSHQ"
	CommandNames[APPENDQ] = "APPENDQ"
	CommandNames[PREPENDQ] = "PREPENDQ"
	CommandNames[RGET] = "RGET"
	CommandNames[RSET] = "RSET"
	CommandNames[RSETQ] = "RSETQ"
	CommandNames[RAPPEND] = "RAPPEND"
	CommandNames[RAPPENDQ] = "RAPPENDQ"
	CommandNames[RPREPEND] = "RPREPEND"
	CommandNames[RPREPENDQ] = "RPREPENDQ"
	CommandNames[RDELETE] = "RDELETE"
	CommandNames[RDELETEQ] = "RDELETEQ"
	CommandNames[RINCR] = "RINCR"
	CommandNames[RINCRQ] = "RINCRQ"
	CommandNames[RDECR] = "RDECR"
	CommandNames[RDECRQ] = "RDECRQ"

	CommandNames[TAP_CONNECT] = "TAP_CONNECT"
	CommandNames[TAP_MUTATION] = "TAP_MUTATION"
	CommandNames[TAP_DELETE] = "TAP_DELETE"
	CommandNames[TAP_FLUSH] = "TAP_FLUSH"
	CommandNames[TAP_OPAQUE] = "TAP_OPAQUE"
	CommandNames[TAP_VBUCKET_SET] = "TAP_VBUCKET_SET"
	CommandNames[TAP_CHECKPOINT_START] = "TAP_CHECKPOINT_START"
	CommandNames[TAP_CHECKPOINT_END] = "TAP_CHECKPOINT_END"

	StatusNames = make(map[Status]string)
	StatusNames[SUCCESS] = "SUCCESS"
	StatusNames[KEY_ENOENT] = "KEY_ENOENT"
	StatusNames[KEY_EEXISTS] = "KEY_EEXISTS"
	StatusNames[E2BIG] = "E2BIG"
	StatusNames[EINVAL] = "EINVAL"
	StatusNames[NOT_STORED] = "NOT_STORED"
	StatusNames[DELTA_BADVAL] = "DELTA_BADVAL"
	StatusNames[NOT_MY_VBUCKET] = "NOT_MY_VBUCKET"
	StatusNames[UNKNOWN_COMMAND] = "UNKNOWN_COMMAND"
	StatusNames[ENOMEM] = "ENOMEM"

}

// String an op code.
func (o CommandCode) String() (rv string) {
	rv = CommandNames[o]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(o))
	}
	return rv
}

// String an op code.
func (s Status) String() (rv string) {
	rv = StatusNames[s]
	if rv == "" {
		rv = fmt.Sprintf("0x%02x", int(s))
	}
	return rv
}
