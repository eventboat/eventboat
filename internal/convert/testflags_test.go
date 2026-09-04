package convert

import (
	"flag"
	"os"
	"testing"

	"github.com/eventboat/eventboat/internal/lang/starhost"
)

var updateGoldens = flag.Bool("update", false, "update golden files")

func TestMain(m *testing.M) {
	flag.Parse()
	os.Exit(m.Run())
}

// compileForTest runs the real Starlark compile gate used by Convert.
func compileForTest(script string) (any, error) {
	return starhost.Compile("convert-test", script, starhost.DefaultOptions())
}
