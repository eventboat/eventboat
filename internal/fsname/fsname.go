// Package fsname is the single source of the file-name rules shared by every
// module that turns a pipeline name into a file base name: config's
// metadata.name validation (<data-dir>/pipelines/<name>.yaml) and the store
// owner's per-pipeline database (<data-dir>/stores/<name>.db). A leaf
// package: no internal dependencies, no state, so the two callers cannot
// drift on either rule.
package fsname

import "strings"

// Sanitize maps a name onto a conservative file base name: alphanumerics
// plus '-' and '_' survive, everything else becomes '_'. Pipeline names may
// contain '.', which must not create a second path component or an
// extension, so dots are substituted too. The mapping is total: it never
// fails, and an empty input is the only input that maps to "".
func Sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		default:
			return '_'
		}
	}, name)
}

// WindowsReservedName reports whether name collides with a Windows reserved
// device name (CON, PRN, AUX, NUL, COM1-9, LPT1-9): Windows resolves such
// file names to the devices themselves, so os.WriteFile silently writes the
// device instead of creating a file. Names derived from user input end up as
// file base names here — the deployed pipeline (<name>.yaml) and the
// per-pipeline store file (<name>.db) — so both gate on this check.
// Case-insensitive on the name's first dot-component (the would-be base
// stem): "con" and "con.yaml" are reserved, while "console" and "acon" are
// ordinary names.
func WindowsReservedName(name string) bool {
	stem := name
	if i := strings.IndexByte(name, '.'); i >= 0 {
		stem = name[:i]
	}
	switch strings.ToUpper(stem) {
	case "CON", "PRN", "AUX", "NUL",
		"COM1", "COM2", "COM3", "COM4", "COM5", "COM6", "COM7", "COM8", "COM9",
		"LPT1", "LPT2", "LPT3", "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9":
		return true
	}
	return false
}
