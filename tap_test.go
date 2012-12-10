package gomemcached

import (
	"bytes"
	"io"
	"io/ioutil"
	"reflect"
	"testing"
)

func TestTapConnectFlagNameString(t *testing.T) {
	if DUMP.String() != "DUMP" {
		t.Fatalf("Expected \"DUMP\" for DUMP, got \"%v\"",
			DUMP.String())
	}

	f := TapConnectFlag(0x3)
	exp := "BACKFILL|DUMP"
	if f.String() != exp {
		t.Fatalf("Expected %q for 0x3, got %q", exp, f.String())
	}

	f = TapConnectFlag(0x212)
	exp = "DUMP|SUPPORT_ACK|0x200"
	if f.String() != exp {
		t.Fatalf("Expected %q for 0x212, got %q", exp, f.String())
	}

	f = TapConnectFlag(0xffffffff)
	f.String() // would hang if I were stupid
}

func TestTapParsers(t *testing.T) {
	tests := []struct {
		f  TapItemParser
		in []byte

		exp       interface{}
		errs      bool
		remaining int64
	}{
		// 64 bit
		{TapParseUint64, []byte{0, 0, 0, 0}, uint64(0), true, 0},
		{TapParseUint64, []byte{0, 0, 0, 0, 0, 0, 0, 0}, uint64(0), false, 0},
		{TapParseUint64, []byte{0, 0, 0, 0, 0, 0, 0, 5}, uint64(5), false, 0},
		{TapParseUint64, []byte{0, 0, 0, 0, 0, 0, 0, 5, 6, 7}, uint64(5), false, 2},
		// 16 bit
		{TapParseUint16, []byte{0}, uint16(0), true, 0},
		{TapParseUint16, []byte{0, 0}, uint16(0), false, 0},
		{TapParseUint16, []byte{0, 5}, uint16(5), false, 0},
		{TapParseUint16, []byte{0, 5, 6, 7}, uint16(5), false, 2},
		// noop
		{TapParseBool, []byte{4, 5}, true, false, 2},
		// vbucket list
		{TapParseVBList, []byte{0}, nil, true, 0},
		{TapParseVBList, []byte{0, 0}, []uint16{}, false, 0},
		{TapParseVBList, []byte{0, 0, 0}, []uint16{}, false, 1},
		{TapParseVBList, []byte{0, 0, 0, 0}, []uint16{}, false, 2},
		{TapParseVBList, []byte{0, 1, 1, 0}, []uint16{256}, false, 0},
		{TapParseVBList, []byte{0, 2, 0, 0}, nil, true, 0},
		{TapParseVBList, []byte{0, 2, 1, 0, 0, 16}, []uint16{256, 16}, false, 0},
	}

	for _, x := range tests {
		r := bytes.NewReader(x.in)
		got, err := x.f(r)

		if (err != nil) == x.errs {
			if !reflect.DeepEqual(got, x.exp) {
				t.Errorf("Expected %v, got %v for %v",
					x.exp, got, x)
			}
			n, _ := io.Copy(ioutil.Discard, r)
			if n != x.remaining {
				t.Errorf("Expected %v remaining, got %v for %v",
					x.remaining, n, x)
			}
		} else {
			t.Errorf("Error fail, got %v on %v", err, x)
		}
	}
}
