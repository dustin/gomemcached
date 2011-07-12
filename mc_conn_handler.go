package mc_conn_handler

import . "./mc_constants"

import (
	"log";
	"net";
	"io";
	"bufio";
	"runtime";
	"encoding/binary"
)

func HandleIO(s net.Conn, reqChannel chan MCRequest) {
	log.Print("Processing input from %s", s)
	defer hangup(s)
	for handleMessage(s, reqChannel) {
	}
}

func hangup(s net.Conn) {
	s.Close()
	log.Print("Hung up on a connection")
}

func handleMessage(s net.Conn, reqChannel chan MCRequest) (ret bool) {
	log.Printf("Handling a message...")
	hdrBytes := make([]byte, HDR_LEN)
	ret = false

	log.Printf("Reading header...")
	bytesRead, err := io.ReadFull(s, hdrBytes)
	if err != nil || bytesRead != HDR_LEN {
		log.Print("Error reading message: %s (%d bytes)", err, bytesRead)
		return
	}

	req := grokHeader(hdrBytes)

	readContents(s, req)

	log.Print("Processing message %s", req)
	req.ResponseChannel = make(chan MCResponse)
	reqChannel <- req
	res := <-req.ResponseChannel
	ret = !res.Fatal
	if ret {
		log.Printf("Got response %s", res)
		transmitResponse(s, req, res)
	} else {
		log.Print("Something went wrong, hanging up...")
	}

	return
}

func readContents(s net.Conn, req MCRequest) {
	readOb(s, req.Extras)
	readOb(s, req.Key)
	readOb(s, req.Body)
}

func transmitResponse(s net.Conn, req MCRequest, res MCResponse) {
	o := bufio.NewWriter(s)
	writeByte(o, RES_MAGIC)
	writeByte(o, req.Opcode)
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

func readOb(s net.Conn, buf []byte) {
	x, err := io.ReadFull(s, buf)
	if err != nil || x != len(buf) {
		log.Printf("Error reading part: %s", err)
		runtime.Goexit()
	}
}

func grokHeader(hdrBytes []byte) (rv MCRequest) {
	if hdrBytes[0] != REQ_MAGIC {
		log.Printf("Bad magic: %x", hdrBytes[0])
		runtime.Goexit()
	}
	rv.Opcode = hdrBytes[1]
	rv.Key = make([]byte, binary.BigEndian.Uint16(hdrBytes[2:]))
	rv.Extras = make([]byte, hdrBytes[4])
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:]) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	rv.Body = make([]byte, bodyLen)
	rv.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	rv.Cas = binary.BigEndian.Uint64(hdrBytes[16:])
	return
}
