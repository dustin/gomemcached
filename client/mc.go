// A memcached binary protocol client.
package memcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/dustin/gomemcached"
)

const bufsize = 1024

// The Client itself.
type Client struct {
	conn io.ReadWriteCloser

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
		hdrBuf: make([]byte, gomemcached.HDR_LEN),
	}, nil
}

// Close the connection when you're done.
func (c *Client) Close() {
	c.conn.Close()
}

// Send a custom request and get the response.
func (client *Client) Send(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error) {
	err = transmitRequest(client.conn, req)
	if err != nil {
		return
	}
	return getResponse(client.conn, client.hdrBuf)
}

// Send a request, but do not wait for a response.
func (client *Client) Transmit(req *gomemcached.MCRequest) error {
	return transmitRequest(client.conn, req)
}

// Receive a response
func (client *Client) Receive() (*gomemcached.MCResponse, error) {
	return getResponse(client.conn, client.hdrBuf)
}

// Get the value for a key.
func (client *Client) Get(vb uint16, key string) (*gomemcached.MCResponse, error) {
	return client.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.GET,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{},
		Body:    []byte{}})
}

// Delete a key.
func (client *Client) Del(vb uint16, key string) (*gomemcached.MCResponse, error) {
	return client.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.DELETE,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{},
		Body:    []byte{}})
}

func (client *Client) store(opcode gomemcached.CommandCode, vb uint16,
	key string, flags int, exp int, body []byte) (*gomemcached.MCResponse, error) {

	req := &gomemcached.MCRequest{
		Opcode:  opcode,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
		Body:    body}

	binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
	return client.Send(req)
}

// Increment a value.
func (client *Client) Incr(vb uint16, key string,
	amt, def uint64, exp int) (uint64, error) {

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.INCREMENT,
		VBucket: vb,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  0,
		Extras:  make([]byte, 8+8+4),
		Body:    []byte{}}
	binary.BigEndian.PutUint64(req.Extras[:8], amt)
	binary.BigEndian.PutUint64(req.Extras[8:16], def)
	binary.BigEndian.PutUint32(req.Extras[16:20], uint32(exp))

	resp, err := client.Send(req)
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(resp.Body), nil
}

// Add a value for a key (store if not exists).
func (client *Client) Add(vb uint16, key string, flags int, exp int,
	body []byte) (*gomemcached.MCResponse, error) {
	return client.store(gomemcached.ADD, vb, key, flags, exp, body)
}

// Set the value for a key.
func (client *Client) Set(vb uint16, key string, flags int, exp int,
	body []byte) (*gomemcached.MCResponse, error) {
	return client.store(gomemcached.SET, vb, key, flags, exp, body)
}

// Get keys in bulk
func (client *Client) GetBulk(vb uint16, keys []string) (map[string]*gomemcached.MCResponse, error) {
	terminalOpaque := uint32(len(keys) + 5)
	rv := map[string]*gomemcached.MCResponse{}
	wg := sync.WaitGroup{}
	going := true

	defer func() {
		going = false
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for going {
			res, err := client.Receive()
			if err != nil {
				return
			}
			if res.Opaque == terminalOpaque {
				return
			}
			if res.Opcode != gomemcached.GETQ {
				log.Panicf("Unexpected opcode in GETQ response: %+v",
					res)
			}
			rv[keys[res.Opaque]] = res
		}
	}()

	for i, k := range keys {
		err := client.Transmit(&gomemcached.MCRequest{
			Opcode:  gomemcached.GETQ,
			VBucket: vb,
			Key:     []byte(k),
			Cas:     0,
			Opaque:  uint32(i),
			Extras:  []byte{},
			Body:    []byte{}})
		if err != nil {
			return rv, err
		}
	}

	err := client.Transmit(&gomemcached.MCRequest{
		Opcode: gomemcached.NOOP,
		Key:    []byte{},
		Cas:    0,
		Extras: []byte{},
		Body:   []byte{},
		Opaque: terminalOpaque})
	if err != nil {
		return rv, err
	}

	wg.Wait()

	return rv, nil
}

// A function to perform a CAS transform
type CasFunc func(current []byte) []byte

// Perform a CAS transform with the given function.
//
// If the value does not exist, an empty byte string will be sent to f
func (client *Client) CAS(vb uint16, k string, f CasFunc,
	initexp int) (rv *gomemcached.MCResponse, err error) {

	flags := 0
	exp := 0

	for {
		orig, err := client.Get(vb, k)
		if err != nil && orig != nil && orig.Status != gomemcached.KEY_ENOENT {
			return rv, err
		}

		if orig.Status == gomemcached.KEY_ENOENT {
			init := f([]byte{})
			// If it doesn't exist, add it
			resp, err := client.Add(vb, k, 0, initexp, init)
			if err == nil && resp.Status != gomemcached.KEY_EEXISTS {
				return rv, err
			}
			// Copy the body into this response.
			resp.Body = init
			return resp, err
		} else {
			req := &gomemcached.MCRequest{
				Opcode:  gomemcached.SET,
				VBucket: vb,
				Key:     []byte(k),
				Cas:     orig.Cas,
				Opaque:  0,
				Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
				Body:    f(orig.Body)}

			binary.BigEndian.PutUint64(req.Extras, uint64(flags)<<32|uint64(exp))
			resp, err := client.Send(req)
			if err == nil {
				return resp, nil
			}
		}
	}
	panic("Unreachable")
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
	rv := make([]StatValue, 0, 128)

	req := &gomemcached.MCRequest{
		Opcode:  gomemcached.STAT,
		VBucket: 0,
		Key:     []byte(key),
		Cas:     0,
		Opaque:  918494,
		Extras:  []byte{}}

	err := transmitRequest(client.conn, req)
	if err != nil {
		return rv, err
	}

	for {
		res, err := getResponse(client.conn, client.hdrBuf)
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
