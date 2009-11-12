package mc_conn_handler

import . "./mc_constants"

import (
	"log";
	// "os";
	"net";
	"io";
)

func HandleIO(s *net.TCPConn) {
	log.Stdout("Processing input from %s", s);
	for handleMessage(s) {
	}
	s.Close();
}

func handleMessage(s *net.TCPConn) (ret bool) {
	hdrBytes := make([]byte, HDR_LEN);
	ret = false;

	bytesRead, err := io.ReadFull(s, hdrBytes);
	if err != nil || bytesRead != HDR_LEN {
		log.Stderr("Error reading message: %s (%d bytes)", err, bytesRead);
		return;
	}

	req, ok := grokHeader(hdrBytes);
	if !ok {
		return
	}

	if !readOb(s, req.Extras) {
		return
	}

	if !readOb(s, req.Key) {
		return
	}

	if !readOb(s, req.Body) {
		return
	}

	log.Stdout("Processing message %s", req);
	res, ret := processRequest(req);
	if ret {
		log.Stdout("Got response %s", res);
		ret = transmitResponse(s, req, res);
	} else {
		log.Stderr("Something went wrong, hanging up...")
	}

	return;
}

func transmitResponse(s *net.TCPConn, req MCRequest, res MCResponse) (rv bool) {
	rv = writeByte(s, RES_MAGIC, rv);
	rv = writeByte(s, req.Opcode, rv);
	rv = writeUint16(s, uint16(len(res.Key)), rv);
	rv = writeByte(s, uint8(len(res.Extras)), rv);
	rv = writeByte(s, 0, rv);
	rv = writeUint16(s, res.Status, rv);
	rv = writeUint32(s, uint32(len(res.Body)), rv);
	rv = writeUint32(s, req.Opaque, rv);
	rv = writeUint64(s, res.Cas, rv);
	rv = writeBytes(s, res.Extras, rv);
	rv = writeBytes(s, res.Key, rv);
	rv = writeBytes(s, res.Body, rv);
	return;
}

func writeBytes(s *net.TCPConn, data []byte, failed bool) (rv bool) {
	rv = failed;
	if !failed {
		written, err := s.Write(data);
		if err != nil || written != len(data) {
			log.Stderrf("Error writing bytes:  %s", err);
			rv = false;
		}
	}
	return;

}

func writeByte(s *net.TCPConn, b byte, failed bool) (rv bool) {
	var data [1]byte;
	data[0] = b;
	rv = writeBytes(s, &data, failed);
	return;
}

func writeUint16(s *net.TCPConn, n uint16, failed bool) (rv bool) {
	var data [2]byte;
	data[0] = uint8(n >> 8);
	data[1] = uint8(n & 0xff);
	rv = writeBytes(s, &data, failed);
	return;
}

func writeUint32(s *net.TCPConn, n uint32, failed bool) (rv bool) {
	rv = writeUint16(s, uint16(n>>16), failed);
	rv = writeUint16(s, uint16(n&0xffff), failed);
	return;
}

func writeUint64(s *net.TCPConn, n uint64, failed bool) (rv bool) {
	rv = writeUint32(s, uint32(n>>32), failed);
	rv = writeUint32(s, uint32(n&0xffffffff), failed);
	return;
}

func processRequest(req MCRequest) (res MCResponse, rc bool) {
	rc = true;
	res.Status = UNKNOWN_COMMAND;
	res.Cas = 0;
	return;
}

func readOb(s *net.TCPConn, buf []byte) (rv bool) {
	rv = true;
	_, err := io.ReadFull(s, buf);
	if err != nil {
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
