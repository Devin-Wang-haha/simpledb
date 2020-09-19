package simpledb

import (
	"math"
)

const maxSegments  = math.MaxInt16

type datalog struct {
	curSeg			*segment
	segments		[maxSegments]*segment
}

func (dl *datalog) readKeyValue(sl *slot) ([]byte, []byte, error) {
	seg := dl.segments[sl.segmentID]
	off := sl.offset
	keyStart := off+2		// 2 is the length of keySize
	key, err := seg.Read(int64(keyStart), int64(sl.keySize))
	if err != nil {
		return nil, nil, err
	}
	valueStart := keyStart+uint32(sl.keySize)+4		// 4 is the length of valueSize
	value, err := seg.Read(int64(valueStart), int64(sl.valueSize))
	if err != nil {
		return nil, nil, err
	}
	return key, value, nil
}

// If the current segment is full, create a new segment and set it as the current segment
func (dl * datalog) swapSegment() error {
	curSegId := dl.curSeg.id
	newSegId := curSegId + 1
	newMmapFile, err := OpenMmapFile(segmentName(newSegId), 1)
	if err != nil {
		return err
	}
	dl.segments[newSegId] = &segment{
		MmapFile: newMmapFile,
		id: newSegId,
	}
	dl.curSeg = dl.segments[newSegId]
	return nil
}

// @return: segmentID, offset
func (dl *datalog) writeRecord(data []byte) (uint16, int64, error) {
	curSize := int(dl.curSeg.FileSize())
	if curSize + len(data) > maxSegmentSize {
		dl.curSeg.full = true
	}
	if dl.curSeg.full {
		err := dl.swapSegment()
		if err!=nil {
			return 0, -1, err
		}
	}

	newFileSize := dl.curSeg.FileSize() + int64(len(data))
	err := dl.curSeg.MmapFile.Grow(newFileSize)
	if err != nil {
		return 0, -1, err
	}

	dl.curSeg.Append(data)

	return dl.curSeg.id, int64(curSize), nil
}