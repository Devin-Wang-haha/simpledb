package simpledb

import (
	"os"
	"syscall"
	"unsafe"
)

func mmap(f *os.File, mmapSize int64) ([]byte, error) {
	p, err := syscall.Mmap(int(f.Fd()), 0, int(mmapSize), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	return p, err
}

// enlarge the size of a file
func grow(f *os.File, fileSize int64) error {
	stat, err := f.Stat()
	if err != nil {
		return err
	}
	if stat.Size() >= fileSize {
		return nil
	}
	return f.Truncate(fileSize)
}

func munmap(data []byte) error {
	return syscall.Munmap(data)
}

func madviceRandom(data []byte) error {
	_, _, errno := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&data[0])), uintptr(len(data)), uintptr(syscall.MADV_RANDOM))
	if errno != 0 {
		return errno
	}
	return nil
}
