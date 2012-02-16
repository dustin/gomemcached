package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"github.com/dustin/gomemcached"
	"io"
	"log"
	"runtime"
)

func HandleIO(s io.ReadWriteCloser, reqChannel chan gomemcached.MCRequest) {
	log.Print("Processing input from %s", s)
	defer hangup(s)
	for handleMessage(s, s, reqChannel) {
	}
}

func hangup(s io.ReadCloser) {
	s.Close()
	log.Print("Hung up on a connection")
}

func handleMessage(r io.Reader, w io.Writer, reqChannel chan gomemcached.MCRequest) (ret bool) {
	log.Printf("Handling a message...")

	log.Printf("Reading header...")
	req, err := ReadPacket(r)
	if err != nil {
		return
	}

	log.Print("Processing message %s", req)
	req.ResponseChannel = make(chan gomemcached.MCResponse)
	reqChannel <- req
	res := <-req.ResponseChannel
	ret = !res.Fatal
	if ret {
		log.Printf("Got response %s", res)
		transmitResponse(w, req, res)
	} else {
		log.Print("Something went wrong, hanging up...")
	}

	return
}

func ReadPacket(r io.Reader) (rv gomemcached.MCRequest, err error) {
	hdrBytes := make([]byte, gomemcached.HDR_LEN)
	bytesRead, err := io.ReadFull(r, hdrBytes)
	if err != nil {
		log.Print("Error reading message: %s (%d bytes)", err, bytesRead)
		return
	}
	if bytesRead != gomemcached.HDR_LEN {
		return rv, errors.New("Short read.")
	}

	rv = grokHeader(hdrBytes)

	readContents(r, &rv)
	return
}

func readContents(s io.Reader, req *gomemcached.MCRequest) {
	readOb(s, req.Extras)
	readOb(s, req.Key)
	readOb(s, req.Body)
}

func transmitResponse(s io.Writer, req gomemcached.MCRequest, res gomemcached.MCResponse) {
	o := bufio.NewWriter(s)
	writeByte(o, gomemcached.RES_MAGIC)
	writeByte(o, byte(req.Opcode))
	writeUint16(o, uint16(len(res.Key)))
	writeByte(o, uint8(len(res.Extras)))
	writeByte(o, 0)
	writeUint16(o, res.Status)
	writeUint32(o, uint32(len(res.Body))+
		uint32(len(res.Key))+
		uint32(len(res.Extras)))
	writeUint32(o, req.Opaque)
	writeUint64(o, res.Cas)
	writeBytes(o, res.Extras)
	writeBytes(o, res.Key)
	writeBytes(o, res.Body)
	o.Flush()
	return
}

func writeBytes(s *bufio.Writer, data []byte) {
	if len(data) > 0 {
		written, err := s.Write(data)
		if err != nil || written != len(data) {
			log.Printf("Error writing bytes:  %s", err)
			runtime.Goexit()
		}
	}
	return

}

func writeByte(s *bufio.Writer, b byte) {
	data := make([]byte, 1)
	data[0] = b
	writeBytes(s, data)
}

func writeUint16(s *bufio.Writer, n uint16) {
	data := []byte{0, 0}
	binary.BigEndian.PutUint16(data, n)
	writeBytes(s, data)
}

func writeUint32(s *bufio.Writer, n uint32) {
	data := []byte{0, 0, 0, 0}
	binary.BigEndian.PutUint32(data, n)
	writeBytes(s, data)
}

func writeUint64(s *bufio.Writer, n uint64) {
	data := []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(data, n)
	writeBytes(s, data)
}

func readOb(s io.Reader, buf []byte) {
	x, err := io.ReadFull(s, buf)
	if err != nil || x != len(buf) {
		log.Printf("Error reading part: %s", err)
		runtime.Goexit()
	}
}

func grokHeader(hdrBytes []byte) (rv gomemcached.MCRequest) {
	if hdrBytes[0] != gomemcached.REQ_MAGIC {
		log.Printf("Bad magic: %x", hdrBytes[0])
		runtime.Goexit()
	}
	rv.Opcode = gomemcached.CommandCode(hdrBytes[1])
	rv.Key = make([]byte, binary.BigEndian.Uint16(hdrBytes[2:]))
	rv.Extras = make([]byte, hdrBytes[4])
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:]) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	rv.Body = make([]byte, bodyLen)
	rv.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	rv.Cas = binary.BigEndian.Uint64(hdrBytes[16:])
	return
}
