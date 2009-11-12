package main

import . "./mc_constants"

import (
	"./mc_conn_handler";
	"log";
	"net";
	"os";
)

func dataServer(input chan MCRequest) {
	for {
		req := <-input;
		log.Stderrf("Got a request: %s", req);
		var response MCResponse;
		response.Status = UNKNOWN_COMMAND;
		response.Cas = 0;
		response.Fatal = false;
		req.ResponseChannel <- response;
	}
}

func waitForConnections(ls *net.TCPListener) {
	reqChannel := make(chan MCRequest);
	go dataServer(reqChannel);
	for {
		s, e := ls.AcceptTCP();
		if e == nil {
			log.Stdout("Got a connection %s", s);
			go mc_conn_handler.HandleIO(s, reqChannel);
		} else {
			log.Stderr("Error accepting from %s", ls)
		}
	}
}

func main() {
	var la *net.TCPAddr;
	var err os.Error;
	if la, err = net.ResolveTCPAddr(":11212"); err != nil {
		log.Exitf("Error resolving address: %s", err)
	}
	ls, e := net.ListenTCP("tcp", la);
	if e != nil {
		log.Exitf("Got an error:  %s", e)
	}

	waitForConnections(ls);
}
