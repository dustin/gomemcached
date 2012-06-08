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

type chanReq struct {
	req *gomemcached.MCRequest
	res chan *gomemcached.MCResponse
}

type reqHandler struct {
	ch chan chanReq
}

func (rh *reqHandler) HandleMessage(req *gomemcached.MCRequest) *gomemcached.MCResponse {
	cr := chanReq{
		req,
		make(chan *gomemcached.MCResponse),
	}

	rh.ch <- cr
	return <-cr.res
}

func waitForConnections(ls net.Listener) {
	reqChannel := make(chan chanReq)

	go RunServer(reqChannel)
	handler := &reqHandler{reqChannel}

	log.Printf("Listening on port %d", *port)
	for {
		s, e := ls.Accept()
		if e == nil {
			log.Print("Got a connection %s", s)
			go memcached.HandleIO(s, handler)
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
