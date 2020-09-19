package simpledb

import (
	"encoding/binary"
	"fmt"
	"math"
)

const  (
	maxSegmentSize  = math.MaxUint32
	segmentExt = ".sea"
)

type segment struct {
	*MmapFile
	id 		uint16
	full 	bool
}

func segmentName(id uint16) string {
	return fmt.Sprintf("%05d%s", id, segmentExt)
}

type record struct {
	key       []byte
	value     []byte
}

// [keyLen, key, valueLen, value]
// keyLen: 2 bytes, valueLen: 4 bytes
func (r *record) encode() []byte {
	keyLen := len(r.key)
	valueLen := len(r.value)
	data := make([]byte, 2+keyLen+4+valueLen)
	binary.LittleEndian.PutUint16(data[:2], uint16(keyLen))
	copy(data[2:2+keyLen], r.key)
	binary.LittleEndian.PutUint32(data[2+keyLen:2+keyLen+4], uint32(valueLen))
	copy(data[2+keyLen+4:2+keyLen+4+valueLen], r.value)
	return data
}