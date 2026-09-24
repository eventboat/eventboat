//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly && !windows

package store

import (
	"fmt"
	"os"
	"runtime"
)

// lockFileExclusive is unsupported on this platform: the store lease cannot
// be taken, and a run must refuse loudly rather than silently skip the
// single-writer guarantee. Supported targets are the Unix flock family and
// Windows (LockFileEx).
func lockFileExclusive(*os.File) error {
	return fmt.Errorf("file locking is unsupported on %s", runtime.GOOS)
}
