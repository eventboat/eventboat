package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/eventboat/eventboat/internal/convert"
)

// cmdConvert implements `eventboat convert <v2-config> [-o out.yaml]
// [--report report.md] [--from v2] [--to v3]` (redesign-v3.md §7.3/§3.6).
// The output must pass `eventboat verify` — convert exits 1 when the
// generated file does not.
func cmdConvert(args []string, jsonOut bool) int {
	fs := flag.NewFlagSet("convert", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	outPath := fs.String("o", "", "write the converted v3 config here (default: stdout)")
	reportPath := fs.String("report", "", "write the markdown migration report here (default: appended to stdout)")
	from := fs.String("from", "v2", "source dialect (v2 only)")
	to := fs.String("to", "v3", "target dialect (v3 only)")
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, "usage: eventboat convert <v2-config> [-o out.yaml] [--report report.md]")
		fs.PrintDefaults()
	}
	if err := fs.Parse(args); err != nil {
		return 2
	}
	// Go's flag package stops at the first positional; keep re-parsing so
	// `convert file.yaml -o out.yaml` works like `convert -o out.yaml file.yaml`.
	var rest []string
	for {
		if fs.NArg() == 0 {
			break
		}
		rest = append(rest, fs.Arg(0))
		if err := fs.Parse(fs.Args()[1:]); err != nil {
			return 2
		}
	}
	if *from != "v2" || *to != "v3" {
		fmt.Fprintf(os.Stderr, "convert: only --from v2 --to v3 exists\n")
		return 2
	}
	if len(rest) != 1 {
		fs.Usage()
		return 2
	}
	data, err := os.ReadFile(rest[0])
	if err != nil {
		fmt.Fprintf(os.Stderr, "convert: %v\n", err)
		return 2
	}
	res, err := convert.Convert(rest[0], data)
	if err != nil {
		fmt.Fprintf(os.Stderr, "convert: %v\n", err)
		return 2
	}

	if *outPath != "" {
		if err := os.WriteFile(*outPath, res.YAML, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "convert: %v\n", err)
			return 2
		}
	} else {
		os.Stdout.Write(res.YAML)
	}
	if *reportPath != "" {
		if err := os.WriteFile(*reportPath, []byte(res.Report.Markdown()), 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "convert: %v\n", err)
			return 2
		}
		fmt.Fprintf(os.Stderr, "converted %s → %s (report: %s; verify: %s)\n",
			rest[0], orStdout(*outPath), *reportPath, verdict(res.Report.VerifyOK))
	} else if *outPath != "" {
		fmt.Fprintf(os.Stderr, "converted %s → %s (verify: %s; pass --report for the itemized migration report)\n",
			rest[0], *outPath, verdict(res.Report.VerifyOK))
	} else {
		fmt.Fprintf(os.Stderr, "--- migration report (%s) ---\n%s", verdict(res.Report.VerifyOK), res.Report.Markdown())
	}
	if !res.Report.VerifyOK {
		return 1
	}
	return 0
}

func orStdout(p string) string {
	if p == "" {
		return "<stdout>"
	}
	return p
}

func verdict(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}
