package simpledb

import (
)

const (
	BucketLen = 1024
	indexName = "idx.pix"
)

type index struct {
	*MmapFile
	//buckets			[]*bucket
	freeBucketOffs	[]int64
	numBucket		int64		// count of buckets already used, including the free buckets
}


func (idx *index) bucketIndex(hash uint32) uint32 {
	return hash & (BucketLen-1)
}

func bucketOffset(bidx uint32) int64 {
	return int64(bucketSize) * int64(bidx)
}

func (idx *index) createOverflowBucket() (*bucketHandle, error) {
	// Todo: if may corrected as for
	if bucketSize*(idx.numBucket+1) > idx.MmapFile.FileSize() {
		newFileSize := idx.MmapFile.FileSize()*2
		err := idx.MmapFile.Grow(newFileSize)
		if err!=nil {
			return nil, err
		}
	}

	bh := &bucketHandle{
		MmapFile: idx.MmapFile,
		offset:bucketSize*idx.numBucket,
		bucket: &bucket{},
	}
	if err := bh.read(); err != nil {
		return nil, err
	}
	idx.numBucket++
	return bh, nil
}

func (idx *index) fillBucket(off int64) (*bucketHandle, error) {
	bh := &bucketHandle{
		MmapFile: idx.MmapFile,
		offset: off,
		bucket: &bucket{},
	}
	err := bh.read()
	if err != nil {
		return nil, err
	}
	return bh, nil
}

func (idx *index) get(hash uint32) (*slot, error) {
	bidx := idx.bucketIndex(hash)
	off := bucketOffset(bidx)
	bh, err := idx.fillBucket(off)
	if err != nil {
		return nil, err
	}

	buc := bh.bucket
	if slot, _ := buc.iterateSlots(hash); slot != nil {
		return slot, nil
	}

	// search in the next bucket
	off = buc.next
	for ; off!=0; off = buc.next {
		bh, err = idx.fillBucket(off)
		if err != nil {
			return nil, err
		}
		buc = bh.bucket
		if slot, _ := buc.iterateSlots(hash); slot != nil {
			return slot, nil
		}
	}
	return nil, nil
}

func (idx *index) put (sl *slot) error {
	bidx := idx.bucketIndex(sl.hash)
	off := bucketOffset(bidx)
	bh, err := idx.fillBucket(off)
	if err != nil {
		return err
	}
	buc := bh.bucket

	pos := slotsCountPerBucket
	var slot *slot
	// replace the existing slot if existed
	if slot, pos = buc.iterateSlots(sl.hash); slot!=nil {
		buc.slots[pos] = *sl
		return bh.write()
	}
	off = buc.next
	for ; off!=0; off = buc.next {
		bh, err = idx.fillBucket(off)
		if err != nil {
			return err
		}
		buc = bh.bucket
		if slot, pos = buc.iterateSlots(sl.hash); slot != nil {
			buc.slots[pos] = *sl
			return bh.write()
		}
	}

	// add a new slot
	if pos == slotsCountPerBucket {		// The current bucket is full
		var newBucket *bucket
		var newBucketOff int64
		var newBucketHandle *bucketHandle
		if len(idx.freeBucketOffs) >0 {	// fetch a new bucket from the free buckets
			newBucketOff = idx.freeBucketOffs[0]
			idx.freeBucketOffs = idx.freeBucketOffs[1:]
			newBucketHandle, err = idx.fillBucket(newBucketOff)
			if err != nil {
				return err
			}
			newBucket = newBucketHandle.bucket
		} else {		// extend the buckets
			newBucketHandle, err = idx.createOverflowBucket()
			if err != nil {
				return err
			}
			newBucket = newBucketHandle.bucket
			newBucketOff = newBucketHandle.offset
		}
		buc.next = newBucketOff
		// write the previous bucket
		if err := bh.write(); err != nil {
			return err
		}
		newBucket.insert(sl, 0)
		bh = newBucketHandle
	} else {
		buc.insert(sl, pos)
	}
	return bh.write()
}

