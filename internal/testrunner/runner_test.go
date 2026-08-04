package testrunner_test

import (
	"path/filepath"
	"testing"

	"github.com/edgesets/edgestream/internal/testrunner"
	_ "github.com/edgesets/edgestream/plugins/all"
)

func TestMapFilterSuite(t *testing.T) {
	path := filepath.Join("..", "..", "testdata", "tests", "map_filter.yaml")
	results, err := testrunner.RunFile(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !testrunner.AllPassed(results) {
		t.Fatalf("%s", testrunner.FormatResults(results))
	}
}
