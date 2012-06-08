package memcached

import (
	"bufio"
	"bytes"
	"reflect"
	"testing"

	"github.com/dustin/gomemcached"
)

func TestTransmit(t *testing.T) {
	b := bytes.NewBuffer([]byte{})
	buf := bufio.NewWriter(b)

	req := gomemcached.MCRequest{
		Opcode:          gomemcached.SET,
		Cas:             938424885,
		Opaque:          7242,
		VBucket:         824,
		Extras:          []byte{},
		Key:             []byte("somekey"),
		Body:            []byte("somevalue"),
		ResponseChannel: make(chan gomemcached.MCResponse),
	}

	err := transmitRequest(buf, &req)
	if err != nil {
		t.Fatalf("Error transmitting request: %v", err)
	}

	buf.Flush()

	expected := []byte{
		gomemcached.REQ_MAGIC, byte(gomemcached.SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(b.Bytes()) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", req.Size(),
			len(b.Bytes()))
	}

	if !reflect.DeepEqual(b.Bytes(), expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, b.Bytes())
	}
}

func BenchmarkTransmit(b *testing.B) {
	bout := bytes.NewBuffer([]byte{})

	req := gomemcached.MCRequest{
		Opcode:          gomemcached.SET,
		Cas:             938424885,
		Opaque:          7242,
		VBucket:         824,
		Extras:          []byte{},
		Key:             []byte("somekey"),
		Body:            []byte("somevalue"),
		ResponseChannel: make(chan gomemcached.MCResponse),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		bout.Reset()
		buf := bufio.NewWriter(bout)
		err := transmitRequest(buf, &req)
		if err != nil {
			b.Fatalf("Error transmitting request: %v", err)
		}
	}
}
