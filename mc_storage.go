package mc_storage

import . "./mc_constants"

import (
	"log";
)

func RunServer(input chan MCRequest) {
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
