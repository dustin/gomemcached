// A memcached binary protocol client.
package memcached

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/dustin/gomemcached"
)

const bufsize = 1024

// The Client itself.
type Client struct {
	conn   net.Conn
	writer *bufio.Writer

	hdrBuf []byte
}

// Connect to a memcached server.
func Connect(prot, dest string) (rv *Client, err error) {
	conn, err := net.Dial(prot, dest)
	if err != nil {
		return nil, err
	}
	return &Client{
		conn:   conn,
		writer: bufio.NewWriterSize(conn, bufsize),
		hdrBuf: make([]byte, gomemcached.HDR_LEN),
	}, nil
}

// Close the connection when you're done.
func (c *Client) Close() {
	c.conn.Close()
}

// Send a custom request and get the response.
func (client *Client) Send(req *gomemcached.MCRequest) (rv gomemcached.MCResponse, err error) {
	err = transmitRequest(client.writer, req)
	if err != nil {
		return
	}
	return client.getResponse()
}

// Send a request, but do not wait for a response.
func (client *Client) Transmit(req *gomemcached.MCRequest) {
	transmitRequest(client.writer, req)
}

// Receive a response
func (client *Client) Receive() (gomemcached.MCResponse, error) {
	return client.getResponse()
}

// Get the value for a key.
func (client *Client) Get(vb uint16, key string) (gomemcached.MCResponse, error) {
	var req gomemcached.MCRequest
	req.Opcode = gomemcached.GET
	req.VBucket = vb
	req.Key = []byte(key)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = []byte{}
	req.Body = []byte{}
	return client.Send(&req)
}

// Delete a key.
func (client *Client) Del(vb uint16, key string) (gomemcached.MCResponse, error) {
	var req gomemcached.MCRequest
	req.Opcode = gomemcached.DELETE
	req.VBucket = vb
	req.Key = []byte(key)
	req.Cas = 0
	req.Opaque = 0
	req.Extras = []byte{}
	req.Body = []byte{}
	return client.Send(&req)
}

func (client *Client) store(opcode gomemcached.CommandCode, vb uint16,
	key string, flags int, exp int, body []byte) (gomemcached.MCResponse, error) {

	var req gomemcached.MCRequest
	req.Opcode = opcode
	req.VBucket = vb
	req.Cas = 0
	req.Opaque = 0
	req.Key = []byte(key)
	req.Extras = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	req.Body = body
	return client.Send(&req)
}

// Add a value for a key (store if not exists).
func (client *Client) Add(vb uint16, key string, flags int, exp int,
	body []byte) (gomemcached.MCResponse, error) {
	return client.store(gomemcached.ADD, vb, key, flags, exp, body)
}

// Set the value for a key.
func (client *Client) Set(vb uint16, key string, flags int, exp int,
	body []byte) (gomemcached.MCResponse, error) {
	return client.store(gomemcached.SET, vb, key, flags, exp, body)
}

// Stats returns a slice of these.
type StatValue struct {
	// The stat key
	Key string
	// The stat value
	Val string
}

// Get stats from the server
// use "" as the stat key for toplevel stats.
func (client *Client) Stats(key string) ([]StatValue, error) {
	rv := []StatValue{}

	var req gomemcached.MCRequest
	req.Opcode = gomemcached.STAT
	req.VBucket = 0
	req.Cas = 0
	req.Opaque = 918494
	req.Key = []byte(key)
	req.Extras = []byte{}
	req.Body = []byte{}

	err := transmitRequest(client.writer, &req)
	if err != nil {
		return rv, err
	}

	for {
		res, err := client.getResponse()
		if err != nil {
			return rv, err
		}
		k := string(res.Key)
		if k == "" {
			break
		}
		rv = append(rv, StatValue{
			Key: k,
			Val: string(res.Body),
		})
	}

	return rv, nil
}

// Get the stats from the server as a map
func (client *Client) StatsMap(key string) (map[string]string, error) {
	rv := make(map[string]string)
	st, err := client.Stats(key)
	if err != nil {
		return rv, err
	}
	for _, sv := range st {
		rv[sv.Key] = sv.Val
	}
	return rv, nil
}

func (client *Client) getResponse() (rv gomemcached.MCResponse, err error) {
	_, err = io.ReadFull(client.conn, client.hdrBuf)
	if err != nil {
		return rv, err
	}
	rv, err = grokHeader(client.hdrBuf)
	if err != nil {
		return rv, err
	}
	err = readContents(client.conn, &rv)
	return rv, err
}

func readContents(s net.Conn, res *gomemcached.MCResponse) error {
	err := readOb(s, res.Extras)
	if err != nil {
		return err
	}
	err = readOb(s, res.Key)
	if err != nil {
		return err
	}
	return readOb(s, res.Body)
}

func grokHeader(hdrBytes []byte) (rv gomemcached.MCResponse, err error) {
	if hdrBytes[0] != gomemcached.RES_MAGIC {
		return rv, errors.New(fmt.Sprintf("Bad magic: 0x%02x", hdrBytes[0]))
	}
	// rv.Opcode = hdrBytes[1]
	rv.Key = make([]byte, binary.BigEndian.Uint16(hdrBytes[2:]))
	rv.Extras = make([]byte, hdrBytes[4])
	rv.Status = uint16(hdrBytes[7])
	bodyLen := binary.BigEndian.Uint32(hdrBytes[8:]) - uint32(len(rv.Key)) - uint32(len(rv.Extras))
	rv.Body = make([]byte, bodyLen)
	// rv.Opaque = binary.BigEndian.Uint32(hdrBytes[12:])
	rv.Cas = binary.BigEndian.Uint64(hdrBytes[16:])
	return
}

func transmitRequest(o io.Writer, req *gomemcached.MCRequest) (err error) {
	data := make([]byte, req.Size())

	pos := 0
	data[pos] = gomemcached.REQ_MAGIC
	pos++
	data[pos] = byte(req.Opcode)
	pos++
	binary.BigEndian.PutUint16(data[pos:pos+2],
		uint16(len(req.Key)))
	pos += 2

	// 4
	data[pos] = byte(len(req.Extras))
	pos++
	data[pos] = 0
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
	binary.BigEndian.PutUint64(data[pos:pos+8], req.Cas)
	pos += 8

	copy(data[pos:pos+len(req.Extras)], req.Extras)
	pos += len(req.Extras)

	copy(data[pos:pos+len(req.Key)], req.Key)
	pos += len(req.Key)

	copy(data[pos:pos+len(req.Body)], req.Body)
	pos += len(req.Body)

	n, err := o.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("Invalid write")
	}
	return nil
}

func readOb(s net.Conn, buf []byte) error {
	_, err := io.ReadFull(s, buf)
	if err != nil {
		return err
	}
	return nil
}
