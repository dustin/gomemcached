package gomemcached

import (
	"reflect"
	"testing"
)

func TestEncodingRequest(t *testing.T) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	got := req.Bytes()

	expected := []byte{
		REQ_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x3, 0x38, // vbucket
		0x0, 0x0, 0x0, 0x10, // Length of value
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != req.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func BenchmarkEncodingRequest(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     938424885,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest0CAS(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingRequest1Extra(b *testing.B) {
	req := MCRequest{
		Opcode:  SET,
		Cas:     0,
		Opaque:  7242,
		VBucket: 824,
		Extras:  []byte{1},
		Key:     []byte("somekey"),
		Body:    []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}
