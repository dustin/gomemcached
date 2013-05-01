package gomemcached

import (
	"bytes"
	"errors"
	"reflect"
	"testing"
)

func TestEncodingResponse(t *testing.T) {
	req := MCResponse{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	got := req.Bytes()

	expected := []byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x0,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
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

func TestEncodingResponseWithExtras(t *testing.T) {
	res := MCResponse{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Extras: []byte{1, 2, 3, 4},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	buf := &bytes.Buffer{}
	res.Transmit(buf)
	got := buf.Bytes()

	expected := []byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
		0x0, 0x0, 0x0, 0x14, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
		's', 'o', 'm', 'e', 'v', 'a', 'l', 'u', 'e'}

	if len(got) != res.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func TestEncodingResponseWithLargeBody(t *testing.T) {
	res := MCResponse{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Extras: []byte{1, 2, 3, 4},
		Key:    []byte("somekey"),
		Body:   make([]byte, 256),
	}

	buf := &bytes.Buffer{}
	res.Transmit(buf)
	got := buf.Bytes()

	expected := append([]byte{
		RES_MAGIC, byte(SET),
		0x0, 0x7, // length of key
		0x4,       // extra length
		0x0,       // reserved
		0x6, 0x2e, // status
		0x0, 0x0, 0x1, 0xb, // Length of remainder
		0x0, 0x0, 0x1c, 0x4a, // opaque
		0x0, 0x0, 0x0, 0x0, 0x37, 0xef, 0x3a, 0x35, // CAS
		1, 2, 3, 4, // extras
		's', 'o', 'm', 'e', 'k', 'e', 'y',
	}, make([]byte, 256)...)

	if len(got) != res.Size() {
		t.Fatalf("Expected %v bytes, got %v", got,
			len(got))
	}

	if !reflect.DeepEqual(got, expected) {
		t.Fatalf("Expected:\n%#v\n  -- got -- \n%#v",
			expected, got)
	}
}

func BenchmarkEncodingResponse(b *testing.B) {
	req := MCResponse{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   []byte("somevalue"),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func BenchmarkEncodingResponseLarge(b *testing.B) {
	req := MCResponse{
		Opcode: SET,
		Status: 1582,
		Opaque: 7242,
		Cas:    938424885,
		Extras: []byte{},
		Key:    []byte("somekey"),
		Body:   make([]byte, 24*1024),
	}

	b.SetBytes(int64(req.Size()))

	for i := 0; i < b.N; i++ {
		req.Bytes()
	}
}

func TestIsNotFound(t *testing.T) {
	tests := []struct {
		e  error
		is bool
	}{
		{nil, false},
		{errors.New("something"), false},
		{MCResponse{}, false},
		{&MCResponse{}, false},
		{MCResponse{Status: KEY_ENOENT}, true},
		{&MCResponse{Status: KEY_ENOENT}, true},
	}

	for i, x := range tests {
		if IsNotFound(x.e) != x.is {
			t.Errorf("Expected %v for %#v (%v)", x.is, x.e, i)
		}
	}
}
