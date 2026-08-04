.PHONY: build test validate tidy pipeline-test

BINARY := bin/edgestream

build:
	go build -o $(BINARY) ./cmd/edgestream

test:
	go test ./...

tidy:
	go mod tidy

validate: build
	./$(BINARY) validate --config testdata/pipelines/linear.yaml

pipeline-test: build
	./$(BINARY) test --dir testdata/tests
