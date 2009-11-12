package main

import (
	"./mc_conn_handler";
	"log";
	"net";
	"os";
)

func waitForConnections(ls *net.TCPListener) {
	for {
		s, e := ls.AcceptTCP();
		if e == nil {
			log.Stdout("Got a connection %s", s);
			go mc_conn_handler.HandleIO(s);
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
