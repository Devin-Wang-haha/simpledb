package simpledb

import (
	"fmt"
	"testing"
)

func TestWrite(t *testing.T) {
	mmapfile, err := OpenMmapFile("mmap.sea", 1)
	if err != nil {
		t.Fatal(err)
	}
	stat, _ := mmapfile.Stat()
	fmt.Println(stat.Size())
	mmapfile.Write([]byte("haha"))
}
