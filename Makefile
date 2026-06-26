BINARY=jin-kv
GO=go

build:
	$(GO) build -o bin/server ./cmd/server
	$(GO) build -o bin/client ./cmd/client

test:
	$(GO) test -v -race ./...

run-cluster:
	@echo "cluster start not yet implemented"

clean:
	rm -rf bin/

.PHONY: build test run-cluster clean
