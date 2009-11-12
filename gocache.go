package main

import . "./mc_constants"

import (
	"./mc_conn_handler";
	"./mc_storage";
	"log";
	"net";
)

func waitForConnections(ls net.Listener) {
	reqChannel := make(chan MCRequest);
	go mc_storage.RunServer(reqChannel);
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
	ls, e := net.Listen("tcp", ":11212");
	if e != nil {
		log.Exitf("Got an error:  %s", e)
	}

	waitForConnections(ls);
}
