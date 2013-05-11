package memcached

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/dustin/gomemcached"
)

var noConn = errors.New("No connection")

// If the error is a memcached response, declare the error to be nil
// so a client can handle the status without worrying about whether it
// indicates success or failure.
func UnwrapMemcachedError(rv *gomemcached.MCResponse,
	err error) (*gomemcached.MCResponse, error) {

	if rv == err {
		return rv, nil
	}
	return rv, err
}

func getResponse(s io.Reader, hdrBytes []byte) (rv *gomemcached.MCResponse, err error) {
	if s == nil {
		return nil, noConn
	}
	_, err = io.ReadFull(s, hdrBytes)
	if err != nil {
		return rv, err
	}

	if hdrBytes[0] != gomemcached.RES_MAGIC && hdrBytes[0] != gomemcached.REQ_MAGIC {
		return rv, fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
	}

	klen := int(binary.BigEndian.Uint16(hdrBytes[2:4]))
	elen := int(hdrBytes[4])

	rv = &gomemcached.MCResponse{
		Opcode: gomemcached.CommandCode(hdrBytes[1]),
		Status: gomemcached.Status(binary.BigEndian.Uint16(hdrBytes[6:8])),
		Opaque: binary.BigEndian.Uint32(hdrBytes[12:16]),
		Cas:    binary.BigEndian.Uint64(hdrBytes[16:24]),
	}

	bodyLen := int(binary.BigEndian.Uint32(hdrBytes[8:12])) - (klen + elen)

	buf := make([]byte, klen+elen+bodyLen)
	_, err = io.ReadFull(s, buf)
	if err == nil {
		rv.Extras = buf[0:elen]
		rv.Key = buf[elen : klen+elen]
		rv.Body = buf[klen+elen:]
	}

	if err == nil && rv.Status != gomemcached.SUCCESS {
		err = rv
	}
	return rv, err
}

func transmitRequest(o io.Writer, req *gomemcached.MCRequest) (err error) {
	if o == nil {
		return noConn
	}
	return req.Transmit(o)
}
