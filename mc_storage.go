package mc_storage

import . "./mc_constants"

import (
	"log"
	"encoding/binary"
)

type storage struct {
	data map[string]MCItem
	cas  uint64
}

type handler func(req MCRequest, s *storage) MCResponse

var handlers = map[uint8]handler{
	SET:    handleSet,
	GET:    handleGet,
	DELETE: handleDelete,
	FLUSH:  handleFlush,
}

func RunServer(input chan MCRequest) {
	var s storage
	s.data = make(map[string]MCItem)
	for {
		req := <-input
		log.Printf("Got a request: %s", req)
		req.ResponseChannel <- dispatch(req, &s)
	}
}

func dispatch(req MCRequest, s *storage) (rv MCResponse) {
	if h, ok := handlers[req.Opcode]; ok {
		rv = h(req, s)
	} else {
		notFound(req, s)
	}
	return
}

func notFound(req MCRequest, s *storage) MCResponse {
	var response MCResponse
	response.Status = UNKNOWN_COMMAND
	return response
}

func handleSet(req MCRequest, s *storage) (ret MCResponse) {
	var item MCItem

	item.Flags = binary.BigEndian.Uint32(req.Extras)
	item.Expiration = binary.BigEndian.Uint32(req.Extras[4:])
	item.Data = req.Body
	ret.Status = SUCCESS
	s.cas += 1
	item.Cas = s.cas
	ret.Cas = s.cas

	s.data[string(req.Key)] = item
	return
}

func handleGet(req MCRequest, s *storage) (ret MCResponse) {
	if item, ok := s.data[string(req.Key)]; ok {
		ret.Status = SUCCESS
		binary.BigEndian.PutUint32(ret.Extras, item.Flags)
		ret.Cas = item.Cas
		ret.Body = item.Data
	} else {
		ret.Status = KEY_ENOENT
	}
	return
}

func handleFlush(req MCRequest, s *storage) (ret MCResponse) {
	delay := binary.BigEndian.Uint32(req.Extras)
	if delay > 0 {
		log.Printf("Delay not supported (got %d)", delay)
	}
	s.data = make(map[string]MCItem)
	return
}

func handleDelete(req MCRequest, s *storage) (ret MCResponse) {
	delete(s.data, string(req.Key))
	return
}
