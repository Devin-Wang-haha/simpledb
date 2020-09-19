package simpledb

import (
	"errors"
	"os"
)

const initialMmapSize  = 128 * (1 << 20)  // 128MB

type MmapFile struct {
	*os.File
	data []byte
	fileSize int64
}

// Todo: add @flag, @perm
func OpenMmapFile(name string, mmapSize int64) (*MmapFile, error) {
	f, err := os.OpenFile(name, os.O_CREATE | os.O_RDWR, os.FileMode(0640))
	if err != nil {
		return nil, err
	}
	data, err := mmap(f, mmapSize)
	if err!=nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}
	return &MmapFile{f, data, stat.Size()}, nil

}

// append the data at the offset of m.dataSize
// called when writing datalog
func (m *MmapFile) Append (bs []byte) {
	//m.data = append(m.data, bs...)
	copy(m.data[m.fileSize:m.fileSize+int64(len(bs))], bs)
	m.fileSize += int64(len(bs))
}

// write the data at the specific offset
// called when writing buckets
func (m *MmapFile) WriteAt(offset int64, bs []byte) {
	//m.data = append(m.data, bs...)
	copy(m.data[offset:offset+int64(len(bs))], bs)
	// here, the fileSize also counts the padding
	//m.fileSize = offset+int64(len(bs))
}

// called by datalog
func (m * MmapFile) Read(offset, len int64) ([]byte, error) {
	if offset+len > m.fileSize {
		return nil, errors.New("read is out of range")
	}
	return m.data[offset: offset+len], nil
}

// called by buckets
func (m * MmapFile) ReadRandom(offset, len int64) ([]byte, error) {
	stat, _ := m.File.Stat()

	if offset+len > stat.Size() {
		err := m.Grow(offset+len)
		if err != nil {
			return nil, err
		}
		m.fileSize = offset+len
	}

	return m.data[offset: offset+len], nil
}

func (m *MmapFile) Grow(fileSize int64) error {
	err := grow(m.File, fileSize)
	if err != nil {
		return err
	}
	// unmap it
	err = munmap(m.data)
	if err !=nil {
		return err
	}

	data, err := mmap(m.File, fileSize)
	if err != nil {
		return err
	}
	m.data = data
	return nil
}

func (m *MmapFile) FileSize() int64 {
	return m.fileSize
}