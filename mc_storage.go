package mc_storage

import . "./mc_constants"
import . "./byte_manipulation"

import (
	"log";
)

type storage struct {
	data	map[string]MCItem;
	cas	uint64;
}

type handler func(req MCRequest, s *storage) MCResponse

var handlers = map[uint8]handler{
	SET: handleSet,
	GET: handleGet,
	FLUSH: handleFlush,
}

func RunServer(input chan MCRequest) {
	var s storage;
	s.cas = 0;
	s.data = make(map[string]MCItem);
	for {
		req := <-input;
		log.Stderrf("Got a request: %s", req);
		req.ResponseChannel <- dispatch(req, &s);
	}
}

func dispatch(req MCRequest, s *storage) (rv MCResponse) {
	if h, ok := handlers[req.Opcode]; ok {
		rv = h(req, s)
	} else {
		notFound(req, s)
	}
	return;
}

func notFound(req MCRequest, s *storage) MCResponse {
	var response MCResponse;
	response.Status = UNKNOWN_COMMAND;
	response.Cas = 0;
	response.Fatal = false;
	return response;
}

func handleSet(req MCRequest, s *storage) (ret MCResponse) {
	var item MCItem;

	item.Flags = ReadInt32(req.Extras, 0);
	item.Expiration = ReadInt32(req.Extras, 4);
	item.Data = req.Body;
	ret.Status = SUCCESS;
	s.cas += 1;
	item.Cas = s.cas;
	ret.Cas = s.cas;

	s.data[string(req.Key)] = item;
	return;
}

func handleGet(req MCRequest, s *storage) (ret MCResponse) {
	if item, ok := s.data[string(req.Key)]; ok {
		ret.Status = SUCCESS;
		ret.Extras = WriteUint32(item.Flags);
		ret.Body = item.Data;
	} else {
		ret.Status = KEY_ENOENT
	}
	return;
}

func handleFlush(req MCRequest, s *storage) (ret MCResponse) {
	delay := ReadInt32(req.Extras, 0);
	if delay > 0 {
		log.Stderrf("Delay not supported (got %d)", delay);
	}
	s.data = make(map[string]MCItem);
	return;
}