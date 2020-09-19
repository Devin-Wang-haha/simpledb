package simpledb

import (
	"encoding/binary"
)

const (
	slotsCountPerBucket = 31
	bucketSize     = 512 // approximately equals 31*(4+2+2+4+4)+1+8=505
)

// slot corresponds to a single item in the hash table.
type slot struct {
	hash      uint32
	segmentID uint16
	keySize   uint16
	valueSize uint32
	offset    uint32 // Segment offset.
}

type bucket struct {
	slots 		[slotsCountPerBucket]slot
	next 		int64 	// Offset of overflow bucket
}

// bucketHandle is used to link the bucket in disk with bucket in memory
type bucketHandle struct {
	*bucket
	*MmapFile
	offset int64
}

func (b *bucket) MarshalBinary() ([]byte, error) {
	buf := make([]byte, bucketSize)
	data := buf
	for i := 0; i < slotsCountPerBucket; i++ {
		sl := b.slots[i]
		binary.LittleEndian.PutUint32(buf[:4], sl.hash)
		binary.LittleEndian.PutUint16(buf[4:6], sl.segmentID)
		binary.LittleEndian.PutUint16(buf[6:8], sl.keySize)
		binary.LittleEndian.PutUint32(buf[8:12], sl.valueSize)
		binary.LittleEndian.PutUint32(buf[12:16], sl.offset)
		buf = buf[16:]
	}
	binary.LittleEndian.PutUint64(buf[:8], uint64(b.next))
	return data, nil
}

func (b *bucket) UnmarshalBinary(data []byte) error {
	for i := 0; i < slotsCountPerBucket; i++ {
		_ = data[16] // bounds check hint to compiler; see golang.org/issue/14808
		b.slots[i].hash = binary.LittleEndian.Uint32(data[:4])
		b.slots[i].segmentID = binary.LittleEndian.Uint16(data[4:6])
		b.slots[i].keySize = binary.LittleEndian.Uint16(data[6:8])
		b.slots[i].valueSize = binary.LittleEndian.Uint32(data[8:12])
		b.slots[i].offset = binary.LittleEndian.Uint32(data[12:16])
		data = data[16:]
	}
	b.next = int64(binary.LittleEndian.Uint64(data[:8]))
	return nil
}

// read a bucket data from disk to memory
func (b *bucketHandle) read() error {
	buf, err := b.MmapFile.ReadRandom(b.offset, bucketSize)
	if err != nil {
		return err
	}
	if buf == nil {
		return nil
	}
	return b.UnmarshalBinary(buf)
}

// write a bucket data from memory to disk
func (b *bucketHandle) write() error {
	buf, err := b.MarshalBinary()
	if err != nil {
		return err
	}
	b.MmapFile.WriteAt(b.offset, buf)
	return nil
}

// Iterate the slots in a bucket for a matched hash. It will stop at
// 1) the position of a matched slot (found); 2) the next position of the last slot (not found)
func (b * bucket) iterateSlots(hash uint32) (*slot, int) {
	i := 0
	for ; i< slotsCountPerBucket; i++ {
		if b.slots[i].hash == hash { // found
			return &b.slots[i], i
		} else if b.slots[i].offset == 0 {
			break
		}
	}
	return nil, i
}

func (sl *slot) kvSize() uint32 {
	return uint32(sl.keySize) + sl.valueSize
}

func (b *bucket) insert(sl *slot, pos int) {
	b.slots[pos] = *sl
}