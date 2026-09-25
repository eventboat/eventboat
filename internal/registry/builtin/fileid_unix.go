//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package builtin

import (
	"fmt"
	"os"
	"syscall"
)

// openReadShared opens path for reading. On the Unix family the plain open
// already lets an external writer rename or delete the file while this
// descriptor is held (the inode survives), which is what rotation and
// logrotate's create mode rely on.
func openReadShared(path string) (*os.File, error) { return os.Open(path) }

// fileIdentity is the Unix (device, inode) pair rendered as an opaque state
// key. It survives rename+recreate (a new file gets a new inode) and is the
// same identity two hard links to one file share. The fields are formatted
// through uint64 so the integer-width differences between the BSDs and Linux
// do not matter.
func fileIdentity(f *os.File) (string, bool) {
	fi, err := f.Stat()
	if err != nil {
		return "", false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return "", false
	}
	return fmt.Sprintf("dev=%d,ino=%d", uint64(st.Dev), uint64(st.Ino)), true
}
