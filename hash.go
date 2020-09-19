package simpledb

import "hash/fnv"

func hash(b []byte) uint32 {
	h := fnv.New32a()
	h.Write(b)
	return h.Sum32()
}
