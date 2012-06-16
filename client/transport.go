package memcached

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/dustin/gomemcached"
)

func getResponse(s io.Reader, buf []byte) (rv *gomemcached.MCResponse, err error) {
	_, err = io.ReadFull(s, buf)
	if err != nil {
		return rv, err
	}
	rv, err = grokHeader(buf)
	if err != nil {
		return rv, err
	}
	err = readContents(s, rv)
	return rv, err
}

func readContents(s io.Reader, res *gomemcached.MCResponse) error {
	if len(res.Extras) > 0 {
		_, err := io.ReadFull(s, res.Extras)
		if err != nil {
			return err
		}
	}
	if len(res.Key) > 0 {
		_, err := io.ReadFull(s, res.Key)
		if err != nil {
			return err
		}
	}
	_, err := io.ReadFull(s, res.Body)
	return err
}

func grokHeader(hdrBytes []byte) (rv *gomemcached.MCResponse, err error) {
	if hdrBytes[0] != gomemcached.RES_MAGIC {
		return rv, fmt.Errorf("Bad magic: 0x%02x", hdrBytes[0])
	}
	rv = &gomemcached.MCResponse{
		Opcode: gomemcached.CommandCode(hdrBytes[1]),
		Key:    make([]byte, binary.BigEndian.Uint16(hdrBytes[2:4])),
		Extras: make([]byte, hdrBytes[4]),
		Status: gomemcached.Status(binary.BigEndian.Uint16(hdrBytes[6:8])),
		Opaque: binary.BigEndian.Uint32(hdrBytes[12:16]),
		Cas:    binary.BigEndian.Uint64(hdrBytes[16:24]),
	}
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:12]) -
		uint32(len(rv.Key)+len(rv.Extras))
	rv.Body = make([]byte, bodyLen)

	return
}

func transmitRequest(o io.Writer, req *gomemcached.MCRequest) (err error) {
	if len(req.Body) < 128 {
		_, err = o.Write(req.Bytes())
	} else {
		_, err = o.Write(req.HeaderBytes())
		if err == nil && len(req.Body) > 0 {
			_, err = o.Write(req.Body)
		}
	}
	return
}
