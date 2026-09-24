//go:build windows

package store

import (
	"fmt"
	"os"
	"syscall"
	"unsafe"
)

// Windows byte-range locks are taken through LockFileEx (Go's syscall
// package exposes the DLL loader and OVERLAPPED, not the call itself). The
// lock is owned by the file handle, so a second open of the same file
// conflicts even inside one process, and the kernel releases it when the
// process dies. Failure is immediate: LOCKFILE_FAIL_IMMEDIATELY turns the
// wait into ERROR_LOCK_VIOLATION.
const (
	lockfileExclusiveLock   = 0x00000002
	lockfileFailImmediately = 0x00000001
	// ERROR_LOCK_VIOLATION (33) and ERROR_SHARING_VIOLATION (32); syscall
	// does not export them.
	errnoSharingViolation = syscall.Errno(32)
	errnoLockViolation    = syscall.Errno(33)
)

var (
	kernel32       = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx = kernel32.NewProc("LockFileEx")
)

func lockFileExclusive(f *os.File) error {
	var ol syscall.Overlapped
	r1, _, callErr := procLockFileEx.Call(
		f.Fd(),
		lockfileExclusiveLock|lockfileFailImmediately,
		0,
		1, // lock one byte from offset 0: any whole-file lock conflicts
		0,
		uintptr(unsafe.Pointer(&ol)),
	)
	if r1 != 0 {
		return nil
	}
	if errno, ok := callErr.(syscall.Errno); ok && (errno == errnoLockViolation || errno == errnoSharingViolation) {
		return errLockBusy
	}
	if callErr == nil || callErr == syscall.Errno(0) {
		callErr = syscall.EINVAL
	}
	return fmt.Errorf("LockFileEx: %w", callErr)
}
