// The custom-build acceptance gate: examples/custom-build is a separate Go
// module that uses only the root package's RunCLI and pkg/plugin. This test
// builds it the way an outside user would and drives the binary through the
// plugin catalog, the static verify gate and a real engine run (the run
// verb is a long-lived daemon, so the test polls the sink's output file and
// stops the process once the events arrived).
package eventboat_test

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const customBuildDir = "examples/custom-build"

func TestCustomBuildAcceptance(t *testing.T) {
	bin := filepath.Join(t.TempDir(), "my-eventboat")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	build := exec.Command("go", "build", "-o", bin, ".")
	build.Dir = customBuildDir
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build custom-build: %v\n%s", err, out)
	}

	work := t.TempDir()
	input := filepath.Join(work, "orders.jsonl")
	if err := os.WriteFile(input, []byte("{\"id\":\"o-1\",\"qty\":2}\n{\"id\":\"o-2\",\"qty\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(work, "echoed.out.jsonl")
	pipeline := filepath.Join(work, "myecho.pipeline.yaml")
	yaml := fmt.Sprintf(`
apiVersion: eventboat/v3
kind: Pipeline
metadata: { name: custom-build-acceptance }
sources:
  ingest:
    decoder: json
    file:
      path: %s
sinks:
  echoed:
    from: [ingest]
    encoder: json
    myecho:
      path: %s
`, filepath.ToSlash(input), filepath.ToSlash(output))
	if err := os.WriteFile(pipeline, []byte(yaml), 0o644); err != nil {
		t.Fatal(err)
	}

	run := func(args ...string) string {
		cmd := exec.Command(bin, args...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s %v: %v\n%s", bin, args, err, out)
		}
		return string(out)
	}

	// The registered plugin is a first-class catalog member, like any
	// built-in.
	if catalog := run("--json", "plugin", "catalog"); !strings.Contains(catalog, `"myecho"`) {
		t.Errorf("plugin catalog misses myecho:\n%s", catalog)
	}

	// The static gate validates the plugin block against the schema
	// generated from the plugin's config struct.
	run("verify", "--config", pipeline)

	// A real run through the plugin sink.
	data := filepath.Join(work, "data")
	cmd := exec.Command(bin, "run", "--config", pipeline, "--ephemeral", "--data-dir", data)
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_, _ = cmd.Process.Wait()
	}()

	deadline := time.Now().Add(30 * time.Second)
	for {
		b, err := os.ReadFile(output)
		if err == nil && strings.Count(string(b), "\n") >= 2 {
			first := strings.SplitN(string(b), "\n", 2)[0]
			var event map[string]any
			if err := json.Unmarshal([]byte(first), &event); err != nil {
				t.Fatalf("sink output is not JSON lines: %v\n%s", err, b)
			}
			if event["id"] != "o-1" {
				t.Errorf("first event id = %v, want o-1", event["id"])
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("sink output never reached 2 lines; have:\n%s", b)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
