include $(GOROOT)/src/Make.$(GOARCH)

.SUFFIXES: .go .$O

OBJS=mc_constants.$O \
		 byte_manipulation.$O \
		 mc_storage.$O \
		 mc_conn_handler.$O \
		 gocache.$O

gocache: $(OBJS)
	$(LD) -o gocache $(OBJS)

clean:
	rm -f $(OBJS) gocache

.go.$O:
	$(GC) $<
