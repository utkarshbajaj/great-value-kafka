PKGNAME = greatvaluekafka

# generate documentation for the package of interest
docs:
	go doc -C src/$(PKGNAME) -u -all > $(PKGNAME)-doc.txt
