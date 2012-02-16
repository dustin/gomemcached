package main

import (
	"flag"
	"fmt"
	"log"
	"net"

	"github.com/dustin/gomemcached"
	"github.com/dustin/gomemcached/server"
)

var port *int = flag.Int("port", 11212, "Port on which to listen")

func waitForConnections(ls net.Listener) {
	reqChannel := make(chan gomemcached.MCRequest)
	go RunServer(reqChannel)
	log.Printf("Listening on port %d", *port)
	for {
		s, e := ls.Accept()
		if e == nil {
			log.Print("Got a connection %s", s)
			go memcached.HandleIO(s, reqChannel)
		} else {
			log.Printf("Error accepting from %s", ls)
		}
	}
}

func main() {
	flag.Parse()
	ls, e := net.Listen("tcp", fmt.Sprintf(":%d", *port))
	if e != nil {
		log.Fatalf("Got an error:  %s", e)
	}

	waitForConnections(ls)
}
