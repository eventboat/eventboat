package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/eventboat/eventboat/internal/lang/celhost"
	"github.com/eventboat/eventboat/internal/lang/starhost"
)

// cmdRepl implements `eventboat repl` (redesign-v3.md §3.6/§4.4): evaluate
// CEL predicates and Starlark scripts against one sample message without
// running a pipeline. One-shot modes (--cel / --script) serve CI and
// agents; the interactive loop re-executes the accumulated script against
// the original sample each line — deterministic session semantics, the same
// guarantee test/explain/replay give (§4.3).
func cmdRepl(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("repl", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	message := fs.String("message", "", "sample message JSON file (default: {})")
	cel := fs.String("cel", "", "evaluate one CEL predicate against the message and exit")
	script := fs.String("script", "", "run one Starlark script file against the message and exit")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: eventboat repl [--message sample.json] [--cel 'expr' | --script f.star]")
		fmt.Fprintln(os.Stderr, "  no --cel/--script: interactive session (lines are Starlark; `cel:` prefix evaluates CEL; :show/:reset/:quit)")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		fs.Usage()
		return 2
	}

	var payload any = map[string]any{}
	if *message != "" {
		data, err := os.ReadFile(*message)
		if err != nil {
			fmt.Fprintf(os.Stderr, "repl: %v\n", err)
			return 2
		}
		if err := json.Unmarshal(data, &payload); err != nil {
			fmt.Fprintf(os.Stderr, "repl: --message: %v\n", err)
			return 2
		}
	}
	meta := map[string]any{"message_id": "repl-sample"}

	switch {
	case *cel != "":
		ok, errStr := evalCel(payload, meta, *cel)
		if jsonOut {
			writeJSON(map[string]any{"expression": *cel, "result": ok, "error": errStr})
		} else if errStr != "" {
			fmt.Fprintf(os.Stderr, "error: %s\n", errStr)
		} else {
			fmt.Printf("%v\n", ok)
		}
		if errStr != "" {
			return 1
		}
		return 0
	case *script != "":
		src, err := os.ReadFile(*script)
		if err != nil {
			fmt.Fprintf(os.Stderr, "repl: %v\n", err)
			return 2
		}
		out, errStr := runStarlark(string(src), payload, meta)
		if jsonOut {
			writeJSON(map[string]any{"script": *script, "payload": out, "error": errStr})
		} else if errStr != "" {
			fmt.Fprintf(os.Stderr, "error: %s\n", errStr)
		} else {
			b, _ := json.MarshalIndent(out, "", "  ")
			fmt.Println(string(b))
		}
		if errStr != "" {
			return 1
		}
		return 0
	}
	if jsonOut {
		fmt.Fprintln(os.Stderr, "repl: --json applies to --cel/--script one-shot modes")
		return 2
	}
	return replInteractive(payload, meta)
}

func evalCel(payload any, meta map[string]any, expr string) (bool, string) {
	env, err := celhost.NewEnv(nil, nil)
	if err != nil {
		return false, err.Error()
	}
	pred, err := env.Compile(expr)
	if err != nil {
		return false, err.Error()
	}
	ok, evalErr := pred.Eval(payload, meta)
	if evalErr != nil {
		return false, evalErr.Error()
	}
	return ok, ""
}

// runStarlark executes src against the sample; the returned payload is the
// post-script value (writes applied).
func runStarlark(src string, payload any, meta map[string]any) (any, string) {
	prog, err := starhost.Compile("repl", src, starhost.DefaultOptions())
	if err != nil {
		return nil, err.Error()
	}
	ps := starhost.NewMsgState("payload", payload)
	ms := starhost.NewMsgState("meta", meta)
	constants := starhost.FreezeConstants(nil)
	if serr := prog.Run(ps, ms, constants); serr != nil {
		return nil, serr.Error()
	}
	return ps.GoValue(), ""
}

func replInteractive(payload any, meta map[string]any) int {
	fmt.Println("eventboat repl — Starlark lines apply to the sample; prefix `cel:` for CEL predicates; :show :reset :quit")
	var script strings.Builder
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("eb> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println()
			return 0 // EOF
		}
		line = strings.TrimSpace(line)
		switch line {
		case "":
			continue
		case ":quit", ":q", ":exit":
			return 0
		case ":reset":
			script.Reset()
			fmt.Println("(script buffer reset; payload restarts from the sample)")
			continue
		case ":show":
			out, errStr := runStarlark(script.String(), payload, meta)
			if errStr != "" {
				fmt.Printf("error: %s\n", errStr)
				continue
			}
			b, _ := json.MarshalIndent(out, "", "  ")
			fmt.Println(string(b))
			continue
		}
		if after, ok := strings.CutPrefix(line, "cel:"); ok {
			ok, errStr := evalCel(currentPayload(script.String(), payload, meta), meta, strings.TrimSpace(after))
			if errStr != "" {
				fmt.Printf("error: %s\n", errStr)
				continue
			}
			fmt.Printf("%v\n", ok)
			continue
		}
		candidate := script.String() + line + "\n"
		if _, errStr := runStarlark(candidate, payload, meta); errStr != "" {
			fmt.Printf("error: %s\n", errStr)
			continue
		}
		script.WriteString(line + "\n")
	}
}

// currentPayload replays the session script to give CEL the same view a
// downstream edge predicate would have.
func currentPayload(script string, payload any, meta map[string]any) any {
	out, errStr := runStarlark(script, payload, meta)
	if errStr != "" {
		return payload
	}
	return out
}

func writeJSON(v any) {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
