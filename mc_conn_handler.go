package mc_conn_handler

import . "./mc_constants"
import . "./byte_manipulation"

import (
	"log";
	"net";
	"io";
	"bufio";
)

func HandleIO(s net.Conn, reqChannel chan MCRequest) {
	log.Stdout("Processing input from %s", s);
	for handleMessage(s, reqChannel) {
	}
	s.Close();
	log.Stdout("Hung up on a connection");
}

func handleMessage(s net.Conn, reqChannel chan MCRequest) (ret bool) {
	log.Stdoutf("Handling a message...");
	hdrBytes := make([]byte, HDR_LEN);
	ret = false;

	log.Stdoutf("Reading header...");
	bytesRead, err := io.ReadFull(s, hdrBytes);
	if err != nil || bytesRead != HDR_LEN {
		log.Stderr("Error reading message: %s (%d bytes)", err, bytesRead);
		return;
	}

	req, ok := grokHeader(hdrBytes);
	if !ok {
		return
	}

	if !readContents(s, req) {
		return
	}

	log.Stdout("Processing message %s", req);
	req.ResponseChannel = make(chan MCResponse);
	reqChannel <- req;
	res := <-req.ResponseChannel;
	ret = !res.Fatal;
	if ret {
		log.Stdoutf("Got response %s", res);
		ret = transmitResponse(s, req, res);
	} else {
		log.Stderr("Something went wrong, hanging up...")
	}

	return;
}

func readContents(s net.Conn, req MCRequest) (rv bool) {
	if !readOb(s, req.Extras) {
		return
	}

	if !readOb(s, req.Key) {
		return
	}

	if readOb(s, req.Body) {
		rv = true
	}
	return;
}

func transmitResponse(s net.Conn, req MCRequest, res MCResponse) (rv bool) {
	rv = true;
	o := bufio.NewWriter(s);
	rv = writeByte(o, RES_MAGIC, rv);
	rv = writeByte(o, req.Opcode, rv);
	rv = writeUint16(o, uint16(len(res.Key)), rv);
	rv = writeByte(o, uint8(len(res.Extras)), rv);
	rv = writeByte(o, 0, rv);
	rv = writeUint16(o, res.Status, rv);
	rv = writeUint32(o, uint32(len(res.Body))+
		uint32(len(res.Key))+
		uint32(len(res.Extras)),
		rv);
	rv = writeUint32(o, req.Opaque, rv);
	rv = writeUint64(o, res.Cas, rv);
	rv = writeBytes(o, res.Extras, rv);
	rv = writeBytes(o, res.Key, rv);
	rv = writeBytes(o, res.Body, rv);
	o.Flush();
	return;
}

func writeBytes(s *bufio.Writer, data []byte, ok bool) (rv bool) {
	rv = ok;
	if ok && len(data) > 0 {
		written, err := s.Write(data);
		if err != nil || written != len(data) {
			log.Stderrf("Error writing bytes:  %s", err);
			rv = false;
		}
	}
	return;

}

func writeByte(s *bufio.Writer, b byte, ok bool) bool {
	var data [1]byte;
	data[0] = b;
	return writeBytes(s, &data, ok);
}

func writeUint16(s *bufio.Writer, n uint16, ok bool) bool {
	data := WriteUint16(n);
	return writeBytes(s, data, ok);
}

func writeUint32(s *bufio.Writer, n uint32, ok bool) bool {
	data := WriteUint32(n);
	return writeBytes(s, data, ok);
}

func writeUint64(s *bufio.Writer, n uint64, ok bool) bool {
	data := WriteUint64(n);
	return writeBytes(s, data, ok);
}

func readOb(s net.Conn, buf []byte) (rv bool) {
	rv = true;
	x, err := io.ReadFull(s, buf);
	if err != nil || x != len(buf) {
		log.Stderrf("Error reading part: %s", err);
		rv = false;
	}
	return;
}

func grokHeader(hdrBytes []byte) (rv MCRequest, ok bool) {
	ok = true;
	if hdrBytes[0] != REQ_MAGIC {
		log.Stderrf("Bad magic: %x", hdrBytes[0]);
		ok = false;
		return;
	}
	rv.Opcode = hdrBytes[1];
	rv.Key = make([]byte, ReadUint16(hdrBytes, 2));
	rv.Extras = make([]byte, hdrBytes[4]);
	bodyLen := ReadUint32(hdrBytes, 8) - uint32(len(rv.Key)) - uint32(len(rv.Extras));
	rv.Body = make([]byte, bodyLen);
	rv.Opaque = ReadUint32(hdrBytes, 12);
	rv.Cas = ReadUint64(hdrBytes, 16);

	return;
}
