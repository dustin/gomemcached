package gomemcached

import (
	"fmt"
	"strings"
)

type TapConnectFlag uint32

const (
	BACKFILL           = TapConnectFlag(0x01)
	DUMP               = TapConnectFlag(0x02)
	LIST_VBUCKETS      = TapConnectFlag(0x04)
	TAKEOVER_VBUCKETS  = TapConnectFlag(0x08)
	SUPPORT_ACK        = TapConnectFlag(0x10)
	REQUEST_KEYS_ONLY  = TapConnectFlag(0x20)
	CHECKPOINT         = TapConnectFlag(0x40)
	REGISTERED_CLIENT  = TapConnectFlag(0x80)
	FIX_FLAG_BYTEORDER = TapConnectFlag(0x100)
)

var TapConnectFlagNames = map[TapConnectFlag]string{
	BACKFILL:           "BACKFILL",
	DUMP:               "DUMP",
	LIST_VBUCKETS:      "LIST_VBUCKETS",
	TAKEOVER_VBUCKETS:  "TAKEOVER_VBUCKETS",
	SUPPORT_ACK:        "SUPPORT_ACK",
	REQUEST_KEYS_ONLY:  "REQUEST_KEYS_ONLY",
	CHECKPOINT:         "CHECKPOINT",
	REGISTERED_CLIENT:  "REGISTERED_CLIENT",
	FIX_FLAG_BYTEORDER: "FIX_FLAG_BYTEORDER",
}

func (f TapConnectFlag) String() string {
	parts := []string{}
	for i := uint32(1); f != 0; i = i << 1 {
		if uint32(f)&i == i {
			p := TapConnectFlagNames[TapConnectFlag(i)]
			if p == "" {
				p = fmt.Sprintf("0x%x", i)
			}
			parts = append(parts, p)
		}
		f = TapConnectFlag(uint32(f) & (^i))
	}
	return strings.Join(parts, "|")
}
