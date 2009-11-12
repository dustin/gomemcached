package mc_conn_handler

import . "./mc_constants"

import (
	"log";
	"net";
	"io";
)

func HandleIO(s *net.TCPConn, reqChannel chan MCRequest) {
	log.Stdout("Processing input from %s", s);
	for handleMessage(s, reqChannel) {
	}
	s.Close();
	log.Stdout("Hung up on a connection");
}

func handleMessage(s *net.TCPConn, reqChannel chan MCRequest) (ret bool) {
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

func readContents(s *net.TCPConn, req MCRequest) (rv bool) {
	rv = true;
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

func transmitResponse(s *net.TCPConn, req MCRequest, res MCResponse) (rv bool) {
	rv = true;
	rv = writeByte(s, RES_MAGIC, rv);
	rv = writeByte(s, req.Opcode, rv);
	rv = writeUint16(s, uint16(len(res.Key)), rv);
	rv = writeByte(s, uint8(len(res.Extras)), rv);
	rv = writeByte(s, 0, rv);
	rv = writeUint16(s, res.Status, rv);
	rv = writeUint32(s, uint32(len(res.Body))+
		uint32(len(res.Key))+
		uint32(len(res.Extras)),
		rv);
	rv = writeUint32(s, req.Opaque, rv);
	rv = writeUint64(s, res.Cas, rv);
	rv = writeBytes(s, res.Extras, rv);
	rv = writeBytes(s, res.Key, rv);
	rv = writeBytes(s, res.Body, rv);
	return;
}

func writeBytes(s *net.TCPConn, data []byte, ok bool) (rv bool) {
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

func writeByte(s *net.TCPConn, b byte, ok bool) (rv bool) {
	var data [1]byte;
	data[0] = b;
	rv = writeBytes(s, &data, ok);
	return;
}

func writeUint16(s *net.TCPConn, n uint16, ok bool) (rv bool) {
	var data [2]byte;
	data[0] = uint8(n >> 8);
	data[1] = uint8(n & 0xff);
	rv = writeBytes(s, &data, ok);
	return;
}

func writeUint32(s *net.TCPConn, n uint32, ok bool) (rv bool) {
	var data [4]byte;
	data[0] = uint8(n >> 24);
	data[1] = uint8((n >> 16) & 0xff);
	data[2] = uint8((n >> 8) & 0xff);
	data[3] = uint8(n & 0xff);
	rv = writeBytes(s, &data, ok);
	/*
		rv = writeUint16(s, uint16(n>>16), ok);
		rv = writeUint16(s, uint16(n&0xffff), ok);
	*/
	return;
}

func writeUint64(s *net.TCPConn, n uint64, ok bool) (rv bool) {
	rv = writeUint32(s, uint32(n>>32), ok);
	rv = writeUint32(s, uint32(n&0xffffffff), ok);
	return;
}

func readOb(s *net.TCPConn, buf []byte) (rv bool) {
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
	rv.Key = make([]byte, readInt16(hdrBytes, 2));
	rv.Extras = make([]byte, hdrBytes[4]);
	bodyLen := readInt32(hdrBytes, 8) - uint32(len(rv.Key)) - uint32(len(rv.Extras));
	rv.Body = make([]byte, bodyLen);
	rv.Opaque = readInt32(hdrBytes, 12);
	rv.Cas = readInt64(hdrBytes, 16);

	return;
}

func readInt16(h []byte, offset int) (rv uint16) {
	rv = uint16(h[offset]) << 8;
	rv |= uint16(h[offset+1]);
	return;
}
func readInt32(h []byte, offset int) (rv uint32) {
	rv = uint32(h[offset]) << 24;
	rv |= uint32(h[offset+1]) << 16;
	rv |= uint32(h[offset+2]) << 8;
	rv |= uint32(h[offset+3]);
	return;
}

func readInt64(h []byte, offset int) (rv uint64) {
	rv = uint64(readInt32(h, offset)) << 32;
	rv |= uint64(readInt32(h, offset+4));
	return;
}
