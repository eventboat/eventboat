//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly && !windows

package builtin

import "os"

// openReadShared falls back to os.Open: no platform-specific share mode is
// available here.
func openReadShared(path string) (*os.File, error) { return os.Open(path) }

// fileIdentity falls back to the path, which is weaker than the Unix
// (device, inode) and Windows (volume, file index) identities: a rename plus
// recreate at the same path is indistinguishable from appends, so rotation
// cannot be detected on this platform. Documented in docs/collector.md.
func fileIdentity(f *os.File) (string, bool) { return "path:" + f.Name(), true }
