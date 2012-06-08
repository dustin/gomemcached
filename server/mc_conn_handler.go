// Useful functions for building your own memcached server.
package memcached

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dustin/go-humanize"
	"github.com/dustin/gomemcached"
)

// The maximum reasonable body length to expect.
// Anything larger than this will result in an error.
var MaxBodyLen = uint32(1 * 1e6)

// Error returned when a packet doesn't start with proper magic.
type BadMagic struct {
	was uint8
}

func (b BadMagic) Error() string {
	return fmt.Sprintf("Bad magic:  0x%02x", b.was)
}

func HandleIO(s io.ReadWriteCloser, reqChannel chan gomemcached.MCRequest) {
	defer s.Close()
	for handleMessage(s, s, reqChannel) {
	}
}

func handleMessage(r io.Reader, w io.Writer, reqChannel chan gomemcached.MCRequest) (ret bool) {
	req, err := ReadPacket(r)
	if err != nil {
		return
	}

	req.ResponseChannel = make(chan gomemcached.MCResponse)
	reqChannel <- req
	res := <-req.ResponseChannel
	ret = !res.Fatal
	if ret {
		res.Opcode = req.Opcode
		res.Opaque = req.Opaque
		transmitResponse(w, res)
	}

	return
}

func ReadPacket(r io.Reader) (rv gomemcached.MCRequest, err error) {
	hdrBytes := make([]byte, gomemcached.HDR_LEN)
	bytesRead, err := io.ReadFull(r, hdrBytes)
	if err != nil {
		return
	}
	if bytesRead != gomemcached.HDR_LEN {
		panic("Expected to read full and didn't")
	}

	rv, err = grokHeader(hdrBytes)
	if err != nil {
		return
	}

	err = readContents(r, &rv)
	return
}

func readContents(s io.Reader, req *gomemcached.MCRequest) (err error) {
	err = readOb(s, req.Extras)
	if err != nil {
		return err
	}
	err = readOb(s, req.Key)
	if err != nil {
		return err
	}
	return readOb(s, req.Body)
}

func transmitResponse(s io.Writer, res gomemcached.MCResponse) error {
	_, err := s.Write(res.Bytes())
	return err
}

func readOb(s io.Reader, buf []byte) error {
	x, err := io.ReadFull(s, buf)
	if err == nil && x != len(buf) {
		panic("Read full didn't")
	}
	return err
}

func grokHeader(hdrBytes []byte) (rv gomemcached.MCRequest, err error) {
	if hdrBytes[0] != gomemcached.REQ_MAGIC {
		return rv, &BadMagic{was: hdrBytes[0]}
	}
	rv.Opcode = gomemcached.CommandCode(hdrBytes[1])
	rv.Key = make([]byte, binary.BigEndian.Uint16(hdrBytes[2:]))
	rv.Extras = make([]byte, hdrBytes[4])
	// Vbucket at 6:7
	rv.VBucket = binary.BigEndian.Uint16(hdrBytes[6:])
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:]) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	if bodyLen > MaxBodyLen {
		return rv, errors.New(fmt.Sprintf("%d is too big (max %s)",
			bodyLen, humanize.Bytes(uint64(MaxBodyLen))))
	}
	rv.Body = make([]byte, bodyLen)
	rv.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	rv.Cas = binary.BigEndian.Uint64(hdrBytes[16:])
	return rv, nil
}
