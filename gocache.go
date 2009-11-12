package main

import . "./mc_constants"

import (
	"./mc_conn_handler";
	"./mc_storage";
	"log";
	"net";
	"fmt";
	"flag";
)

var port *int = flag.Int("port", 11212, "Port on which to listen")

func waitForConnections(ls net.Listener) {
	reqChannel := make(chan MCRequest);
	go mc_storage.RunServer(reqChannel);
	log.Stdoutf("Listening on port %d", *port);
	for {
		s, e := ls.Accept();
		if e == nil {
			log.Stdout("Got a connection %s", s);
			go mc_conn_handler.HandleIO(s, reqChannel);
		} else {
			log.Stderr("Error accepting from %s", ls)
		}
	}
}

func main() {
	flag.Parse();
	ls, e := net.Listen("tcp", fmt.Sprintf(":%d", *port));
	if e != nil {
		log.Exitf("Got an error:  %s", e)
	}

	waitForConnections(ls);
}
