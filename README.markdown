# gomemcached

This is a memcached server in [go][go].

It's currently exceedingly simple, but I'll make it do something
useful over time as I play with the language more.

## Design

The basic design can be seen in [gocache].  A [storage
server][storage] is run as a goroutine that receives a `MCRequest` on
a channel, and then issues an `MCResponse` to a channel contained
within the request.

Each connection is a separate goroutine, of course, and is responsible
for all IO for that connection until the connection drops or the
`dataServer` decides it's stupid and sends a fatal response back over
the channel.

There is currently no work at all in making the thing perform (there
are specific areas I know need work).  This is just my attempt to
learn the language somewhat.

[go]: http://golang.org/
[gocache]: gomemcached/blob/master/gocache.go
[storage]: gomemcached/blob/master/mc_storage.go
