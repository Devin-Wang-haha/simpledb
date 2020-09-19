package simpledb

import (
	"bytes"
	"errors"
	"sync"
)

type DB struct {
	mu                	sync.RWMutex
	index				*index
	datalog 			*datalog
}

// Get returns the value for the given key stored in the DB or nil if the key doesn't exist.
func (db *DB) Get(key []byte) ([]byte, error) {
	hash := hash(key)
	slot, err := db.index.get(hash)
	if err != nil {
		return nil, err
	}
	if slot == nil {
		return nil, nil
	}
	keyRead, value, err := db.datalog.readKeyValue(slot)
	if err != nil {
		return nil, err
	}
	if bytes.Equal(key, keyRead) {
		return value, nil
	} else {
		return nil, errors.New("key stored in segment is not consistent "+
			"with the index")
	}
}

// Put sets the value for the given key. It updates the value for the existing key.
func (db *DB) Put(key []byte, value []byte) error {
	hash := hash(key)

	// write the record to the segment
	record := record{
		key: 	key,
		value: 	value,
	}
	segmentID, offset, err := db.datalog.writeRecord(record.encode())

	if err != nil {
		return err
	}

	// update the index
	slot := &slot{
		hash: hash,
		segmentID: segmentID,
		keySize: uint16(len(key)),
		valueSize: uint32(len(value)),
		offset: uint32(offset),
	}
	db.index.put(slot)

	return nil
}

// Delete deletes the given key from the DB.
func (db *DB) Delete(key []byte) error {
	return nil
}

