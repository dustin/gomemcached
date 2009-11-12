.SUFFIXES: .go .6

OBJS=mc_constants.6 mc_conn_handler.6 gocache.6

gocache: $(OBJS)
	6l -o gocache $(OBJS)

clean:
	rm -f $(OBJS) gocache

.go.6:
	6g $<
