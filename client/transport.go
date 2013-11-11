package memcached

import (
	"errors"
	"io"

	"github.com/dustin/gomemcached"
)

var errNoConn = errors.New("no connection")

// UnwrapMemcachedError converts memcached errors to normal responses.
//
// If the error is a memcached response, declare the error to be nil
// so a client can handle the status without worrying about whether it
// indicates success or failure.
func UnwrapMemcachedError(rv *gomemcached.MCResponse,
	err error) (*gomemcached.MCResponse, error) {

	if rv == err {
		return rv, nil
	}
	return rv, err
}

func getResponse(s io.Reader, hdrBytes []byte) (rv *gomemcached.MCResponse, err error) {
	if s == nil {
		return nil, errNoConn
	}

	rv = &gomemcached.MCResponse{}
	err = rv.Receive(s, hdrBytes)

	if err == nil && rv.Status != gomemcached.SUCCESS {
		err = rv
	}
	return rv, err
}

func transmitRequest(o io.Writer, req *gomemcached.MCRequest) (err error) {
	if o == nil {
		return errNoConn
	}
	return req.Transmit(o)
}
