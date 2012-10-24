// A memcached binary protocol client.
package memcached

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"

	"github.com/dustin/gomemcached"
)

const bufsize = 1024

// The Client itself.
type Client struct {
	conn    io.ReadWriteCloser
	healthy bool

	hdrBuf []byte
}

// Connect to a memcached server.
func Connect(prot, dest string) (rv *Client, err error) {
	conn, err := net.Dial(prot, dest)
	if err != nil {
		return nil, err
	}
	return Wrap(conn)
}

// Wrap an existing transport.
func Wrap(rwc io.ReadWriteCloser) (rv *Client, err error) {
	return &Client{
		conn:    rwc,
		healthy: true,
		hdrBuf:  make([]byte, gomemcached.HDR_LEN),
	}, nil
}

// Close the connection when you're done.
func (c *Client) Close() {
	c.conn.Close()
}

// Return false if this client has had issues communicating.
//
// This is useful for connection pools where we want to
// non-destructively determine that a connection may be reused.
func (c Client) IsHealthy() bool {
	return c.healthy
}

// Send a custom request and get the response.
func (client *Client) Send(req *gomemcached.MCRequest) (rv *gomemcached.MCResponse, err error) {
	err = transmitRequest(client.conn, req)
	if err != nil {
		client.healthy = false
		return
	}
	resp, err := getResponse(client.conn, client.hdrBuf)
	if err != nil {
		client.healthy = false
	}
	return resp, err
}

// Send a request, but do not wait for a response.
func (client *Client) Transmit(req *gomemcached.MCRequest) error {
	err := transmitRequest(client.conn, req)
	if err != nil {
		client.healthy = false
	}
	return err
}

// Receive a response
func (client *Client) Receive() (*gomemcached.MCResponse, error) {
	resp, err := getResponse(client.conn, client.hdrBuf)
	if err != nil {
		client.healthy = false
	}
	return resp, err
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

// List auth mechanisms
func (client *Client) AuthList() (*gomemcached.MCResponse, error) {
	return client.Send(&gomemcached.MCRequest{
		Opcode:  gomemcached.SASL_LIST_MECHS,
		VBucket: 0,
		Key:     []byte{},
		Cas:     0,
		Opaque:  0,
		Extras:  []byte{},
		Body:    []byte{}})
}

func (client *Client) Auth(user, pass string) (*gomemcached.MCResponse, error) {
	res, err := client.AuthList()

	if err != nil {
		return res, err
	}

	authMech := string(res.Body)
	if strings.Index(authMech, "PLAIN") != -1 {
		return client.Send(&gomemcached.MCRequest{
			Opcode:  gomemcached.SASL_AUTH,
			VBucket: 0,
			Key:     []byte("PLAIN"),
			Cas:     0,
			Opaque:  0,
			Extras:  []byte{},
			Body:    []byte(fmt.Sprintf("\x00%s\x00%s", user, pass))})
	}
	return res, fmt.Errorf("Auth mechanism PLAIN not supported")
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

// Operation to perform on this CAS loop.
type CasOp uint8

const (
	// Store the new value normally
	CASStore = CasOp(iota)
	// Stop attempting to CAS, leave value untouched
	CASQuit
	// Delete the current value
	CASDelete
)

// User specified termination is returned as an error.
func (c CasOp) Error() string {
	switch c {
	case CASStore:
		return "CAS store"
	case CASQuit:
		return "CAS quit"
	case CASDelete:
		return "CAS delete"
	}
	panic("Unhandled value")
}

// A function to perform a CAS transform
type CasFunc func(current []byte) ([]byte, CasOp)

// Perform a CAS transform with the given function.
//
// If the value does not exist, an empty byte string will be sent to f
func (client *Client) CAS(vb uint16, k string, f CasFunc,
	initexp int) (rv *gomemcached.MCResponse, err error) {

	flags := 0
	exp := 0

	for {
		orig, err := client.Get(vb, k)
		if err != nil && (orig == nil || orig.Status != gomemcached.KEY_ENOENT) {
			return rv, err
		}

		if orig.Status == gomemcached.KEY_ENOENT {
			init, operation := f([]byte{})
			if operation == CASQuit || operation == CASDelete {
				return nil, operation
			}
			// If it doesn't exist, add it
			resp, err := client.Add(vb, k, 0, initexp, init)
			if err == nil && resp.Status != gomemcached.KEY_EEXISTS {
				return rv, err
			}
		} else {
			var req *gomemcached.MCRequest
			newValue, operation := f(orig.Body)

			switch operation {
			case CASQuit:
				return nil, operation
			case CASStore:
				req = &gomemcached.MCRequest{
					Opcode:  gomemcached.SET,
					VBucket: vb,
					Key:     []byte(k),
					Cas:     orig.Cas,
					Opaque:  0,
					Extras:  []byte{0, 0, 0, 0, 0, 0, 0, 0},
					Body:    newValue}

				binary.BigEndian.PutUint64(req.Extras,
					uint64(flags)<<32|uint64(exp))
			case CASDelete:
				req = &gomemcached.MCRequest{
					Opcode:  gomemcached.DELETE,
					VBucket: vb,
					Key:     []byte(k),
					Cas:     orig.Cas}
			}
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
