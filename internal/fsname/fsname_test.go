package fsname

import "testing"

func TestSanitize(t *testing.T) {
	cases := []struct{ in, want string }{
		{"orders", "orders"},
		{"orders-sync_2", "orders-sync_2"},
		{"a.b", "a_b"},          // dots never survive: no extension, no second component
		{"a/b\\c:d", "a_b_c_d"}, // separators and drive colons
		{"é", "_"},              // non-ASCII
		{"", ""},
	}
	for _, tc := range cases {
		if got := Sanitize(tc.in); got != tc.want {
			t.Errorf("Sanitize(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestWindowsReservedName(t *testing.T) {
	reserved := []string{"CON", "con", "Con.yaml", "PRN", "aux", "NUL", "com1", "LPT9", "LPT9.db", "nul.jsonl"}
	for _, name := range reserved {
		if !WindowsReservedName(name) {
			t.Errorf("WindowsReservedName(%q) = false, want true", name)
		}
	}
	ordinary := []string{"console", "acon", "con2", "com0", "lpt10", "orders", ""}
	for _, name := range ordinary {
		if WindowsReservedName(name) {
			t.Errorf("WindowsReservedName(%q) = true, want false", name)
		}
	}
}
