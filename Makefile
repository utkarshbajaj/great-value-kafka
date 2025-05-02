PKGNAME = greatvaluekafka

build:
	go build -C src/$(PKGNAME)

final: build
	go test -C src/$(PKGNAME) -run Final

# generate documentation for the package of interest
docs:
	go doc -C src/$(PKGNAME) -u -all > $(PKGNAME)-doc.txt
