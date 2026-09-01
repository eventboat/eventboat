.PHONY: build test validate tidy pipeline-test

BINARY := bin/riverpod

build:
	go build -o $(BINARY) ./cmd/riverpod

test:
	go test ./...

tidy:
	go mod tidy

validate: build
	./$(BINARY) validate --config testdata/pipelines/linear.yaml

pipeline-test: build
	./$(BINARY) test --dir testdata/tests
