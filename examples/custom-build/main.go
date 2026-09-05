// Command custom-build is the reference custom Eventboat build: the whole
// binary is a blank import of one compiled-in plugin plus a delegation to
// RunCLI. From this directory:
//
//	go build -o my-eventboat .
//	./my-eventboat verify --config myecho.pipeline.yaml
//	./my-eventboat run --config myecho.pipeline.yaml
//
// See README.md.
package main

import (
	"os"

	"github.com/eventboat/eventboat"
	_ "github.com/eventboat/example-plugins/custom-build/myecho"
)

func main() {
	os.Exit(eventboat.RunCLI(os.Args[1:]))
}
