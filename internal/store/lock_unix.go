//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package store

import (
	"errors"
	"fmt"
	"os"
	"syscall"
)

// lockFileExclusive takes a non-blocking exclusive flock on f. flock locks
// belong to the open file description, so a second open of the same file
// conflicts even inside one process, and the kernel drops the lock when the
// process dies.
func lockFileExclusive(f *os.File) error {
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return errLockBusy
		}
		return fmt.Errorf("flock: %w", err)
	}
	return nil
}
