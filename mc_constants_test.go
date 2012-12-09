package gomemcached

import (
	"testing"
)

func TestCommandCodeStringin(t *testing.T) {
	if GET.String() != "GET" {
		t.Fatalf("Expected \"GET\" for GET, got \"%v\"", GET.String())
	}

	cc := CommandCode(0x80)
	if cc.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", cc.String())
	}
}

func TestStatusNameString(t *testing.T) {
	if SUCCESS.String() != "SUCCESS" {
		t.Fatalf("Expected \"SUCCESS\" for SUCCESS, got \"%v\"",
			SUCCESS.String())
	}

	s := Status(0x80)
	if s.String() != "0x80" {
		t.Fatalf("Expected \"0x80\" for 0x80, got \"%v\"", s.String())
	}
}

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
