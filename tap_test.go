package gomemcached

import (
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
