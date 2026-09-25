//go:build windows

package builtin

import (
	"fmt"
	"os"
	"syscall"
)

// openReadShared opens path for reading through CreateFile with
// FILE_SHARE_DELETE. Go's own os.Open omits that flag, and a handle without it
// makes Windows refuse every rename or delete of the file — a tailer holding
// such a handle would break logrotate's create mode and copytruncate's
// rename-then-recreate for the whole rotation window. With the flag the
// descriptor keeps the data readable while the name disappears, matching the
// Unix inode semantics the source relies on.
func openReadShared(path string) (*os.File, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ|syscall.FILE_SHARE_WRITE|syscall.FILE_SHARE_DELETE,
		nil, syscall.OPEN_EXISTING, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		return nil, &os.PathError{Op: "open", Path: path, Err: err}
	}
	return os.NewFile(uintptr(h), path), nil
}

// fileIdentity is the Windows volume serial number plus the 64-bit file index
// (nFileIndexHigh<<32 | nFileIndexLow) rendered as an opaque state key. The
// index is per-volume and stable across renames; a new file at the same path
// gets a new index, which is what rotation detection compares.
func fileIdentity(f *os.File) (string, bool) {
	var info syscall.ByHandleFileInformation
	if err := syscall.GetFileInformationByHandle(syscall.Handle(f.Fd()), &info); err != nil {
		return "", false
	}
	index := uint64(info.FileIndexHigh)<<32 | uint64(info.FileIndexLow)
	return fmt.Sprintf("vol=%d,idx=%d", info.VolumeSerialNumber, index), true
}
