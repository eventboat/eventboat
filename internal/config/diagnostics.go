package config

import "strings"

// Diagnostics is one verify pass's ordered findings (load + build). It is a
// first-class value (candidate 05): severity policy lives here, once, instead
// of in eight hand-rolled scans.
type Diagnostics []Diagnostic

// Errors returns the error-severity findings, in order.
func (d Diagnostics) Errors() Diagnostics {
	out := make(Diagnostics, 0, len(d))
	for _, diag := range d {
		if diag.Severity == "error" {
			out = append(out, diag)
		}
	}
	return out
}

// Warnings returns the warning-severity findings, in order.
func (d Diagnostics) Warnings() Diagnostics {
	out := make(Diagnostics, 0, len(d))
	for _, diag := range d {
		if diag.Severity == "warning" {
			out = append(out, diag)
		}
	}
	return out
}

// HasErrors reports whether any finding is an error (errors abort the build).
func (d Diagnostics) HasErrors() bool {
	for _, diag := range d {
		if diag.Severity == "error" {
			return true
		}
	}
	return false
}

// HasWarnings reports whether any finding is a warning.
func (d Diagnostics) HasWarnings() bool {
	for _, diag := range d {
		if diag.Severity == "warning" {
			return true
		}
	}
	return false
}

// FirstError returns the first error-severity finding.
func (d Diagnostics) FirstError() (Diagnostic, bool) {
	for _, diag := range d {
		if diag.Severity == "error" {
			return diag, true
		}
	}
	return Diagnostic{}, false
}

// FirstErrorText renders the first error; when no error exists it falls back
// to the first finding (a warning-only abort path) and finally to "unknown
// error". It is the operator-facing one-line summary of a failed verify.
func (d Diagnostics) FirstErrorText() string {
	if diag, ok := d.FirstError(); ok {
		return diag.Error()
	}
	if len(d) > 0 {
		return d[0].Error()
	}
	return "unknown error"
}

// StrictOK is the one strict verdict: a configuration is OK when it has no
// errors, and — under --strict — no warnings either. Apply it once, at the
// composition boundary.
func (d Diagnostics) StrictOK(strict bool) bool {
	if d.HasErrors() {
		return false
	}
	return !strict || !d.HasWarnings()
}

// ErrorLines renders the error findings one per line, indented two spaces
// (the deploy-rejection shape).
func (d Diagnostics) ErrorLines() string {
	var b strings.Builder
	for _, diag := range d.Errors() {
		b.WriteString("  ")
		b.WriteString(diag.Error())
		b.WriteString("\n")
	}
	return b.String()
}
