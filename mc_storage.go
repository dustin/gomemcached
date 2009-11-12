package mc_storage

import . "./mc_constants"

import (
	"log";
)

type itemMap map[string]MCItem

func RunServer(input chan MCRequest) {
	content := make(itemMap);
	for {
		req := <-input;
		log.Stderrf("Got a request: %s", req);
		req.ResponseChannel <- dispatch(req, content);
	}
}

func dispatch(req MCRequest, c itemMap) (rv MCResponse) {
	switch req.Opcode {
	default:
		rv = notFound(req, c)
	}
	return;
}

func notFound(req MCRequest, c itemMap) MCResponse {
	var response MCResponse;
	response.Status = UNKNOWN_COMMAND;
	response.Cas = 0;
	response.Fatal = false;
	return response;
}
