package gomemcached

import "fmt"

type HeaderMagic int

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

type ResponseStatus int

const (
	SUCCESS         = 0x00
	KEY_ENOENT      = 0x01
	KEY_EEXISTS     = 0x02
	E2BIG           = 0x03
	EINVAL          = 0x04
	NOT_STORED      = 0x05
	DELTA_BADVAL    = 0x06
	NOT_MY_VBUCKET  = 0x07
	UNKNOWN_COMMAND = 0x81
	ENOMEM          = 0x82
)

type TapFlags uint32

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

type MCRequest struct {
	Opcode            CommandCode
	Cas               uint64
	Opaque            uint32
	VBucket           uint16
	Extras, Key, Body []byte
	ResponseChannel   chan MCResponse
}

func (req MCRequest) String() string {
	return fmt.Sprintf("{MCRequest opcode=%x, key='%s'}",
		req.Opcode, req.Key)
}

type MCResponse struct {
	Status            uint16
	Cas               uint64
	Extras, Key, Body []byte
	Fatal             bool
}

func (res MCResponse) String() string {
	return fmt.Sprintf("{MCResponse status=%x keylen=%d, extralen=%d, bodylen=%d}",
		res.Status, len(res.Key), len(res.Extras), len(res.Body))
}

func (res MCResponse) Error() string {
	return fmt.Sprintf("MCResponse status=%x, msg: %s",
		res.Status, string(res.Body))
}

type MCItem struct {
	Cas               uint64
	Flags, Expiration uint32
	Data              []byte
}

const HDR_LEN = 24
