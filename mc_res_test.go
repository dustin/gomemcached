package gomemcached

import (
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
		Extras: []byte{},
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
