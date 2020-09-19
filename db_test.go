package simpledb

import (
	"bytes"
	"fmt"
	"testing"
	"time"
)

// initialize DB
func initializeDB() (*DB, error) {
	newMmapFile, err := OpenMmapFile(segmentName(0), 1)
	if err != nil {
		return nil, err
	}
	indexMmapFile, err := OpenMmapFile(indexName, 512 * (1 << 10))
	if err != nil {
		return nil, err
	}

	db := &DB{
		index: &index{
			MmapFile: indexMmapFile,
			numBucket: BucketLen,
		},
		datalog: &datalog{
			curSeg: &segment{
				MmapFile: newMmapFile,
				id: 0,
			},
		},
	}

	db.datalog.segments[0] = db.datalog.curSeg

	return db, nil
}

// Test if the query results matches the data inserted
func TestSimple(t *testing.T) {
	// initialize DB
	db, err := initializeDB()
	if err != nil {
		t.Fatal(err)
	}

	db.Put([]byte("1"), []byte("simple1"))
	db.Put([]byte("2"), []byte("这是simple2"))
	db.Put([]byte("3"), []byte("玩的愉快"))

	getFirst, err := db.Get([]byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(getFirst, []byte("simple1")) {
		t.Fatal("Test for '1'")
	}

	getSecond, _ := db.Get([]byte("2"))
	if !bytes.Equal(getSecond, []byte("这是simple2")) {
		t.Fatal("Test for '2'")
	}

	getThird, _ := db.Get([]byte("3"))
	if !bytes.Equal(getThird, []byte("玩的愉快")) {
		t.Fatal("Test for '3'")
	}

}

// Test if the query for an inexistent data works as expected
func TestInexistence(t *testing.T) {
	// initialize DB
	db, err := initializeDB()
	if err != nil {
		t.Fatal(err)
	}

	getData, _ := db.Get([]byte("0"))
	if getData != nil {
		t.Fatal("get an inexistent value, but it exists")
	}

}

// Test if the segments and buckets are extended correctly,
// when the data is massive
// Watch if the 'Count of buckets' and 'Count of segments' increase as expected
func TestMassiveData(t *testing.T) {
	// initialize DB
	db, err := initializeDB()
	if err != nil {
		t.Fatal(err)
	}

	// suffix is used to enlarge the value,
	// so as to test the creation of new segment more quickly
	suffix := ""
	for i:=0; i<10; i++ {
		suffix += "fdhsjkhfjkshfjksahjkfshajkfhaskjfhkjsafjksahjkfajkfjnvnnasm"
	}

	for j:=0; j<1000; j++ {
		for i := 0; i < 5000; i++ {
			key := []byte(string(j*5000+i))
			value := []byte(string(j*5000+i)+suffix)
			db.Put(key, value)
		}

		fmt.Printf("Count of buckets: %d\n", db.index.numBucket)
		fmt.Printf("Count of segments: %d\n", db.datalog.curSeg.id+1)
		fmt.Printf("Size of curSeg: %d\n\n", db.datalog.curSeg.FileSize())

	}

	start := time.Now()
	getData, _ := db.Get([]byte(string(10000)))
	fmt.Printf("Elapsed time: %v\n", time.Now().Sub(start).String())
	if !bytes.Equal(getData, []byte(string(10000)+suffix)) {
		t.Fatal("Test for '10000'")
	}

}

// Test if the data is updated correctly
func TestUpdateData(t *testing.T) {
	// initialize DB
	db, err := initializeDB()
	if err != nil {
		t.Fatal(err)
	}

	db.Put([]byte("1"), []byte("祝你早安"))
	db.Put([]byte("1"), []byte("祝你晚安"))

	getFirst, _ := db.Get([]byte("1"))
	if bytes.Equal(getFirst, []byte("祝你早安")) {
		t.Fatal("Update data")
	}
	if !bytes.Equal(getFirst, []byte("祝你晚安")) {
		t.Fatal("Update data")
	}

}