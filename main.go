package main

import (
	"fmt"
	"os"

	"github.com/thobbiz/thobbixDB/helpers"
)

func Open(filepath string) (*KVStore, error) {
	file, fileID, err := NewFile(filepath)
	if err != nil {
		return nil, err
	}

	KVStore := &KVStore{
		file: file,
		keyTable: KeyTable{
			keyOffsetMap: make(map[string]int64),
		},
		DataSegments: &DataSegments{
			activeDS: &DataSegment{
				file:   file,
				fileId: fileID,
			},
			inactiveDS:     make(map[uint64]*DataSegment),
			maxDSSizeBytes: MaxFileSize,
		},
	}

	if err := KVStore.buildIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to rebuild index: %w", err)
	}

	return KVStore, nil
}

func NewFile(filepath string) (*os.File, uint64, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}

	fileID, err := helpers.GenerateRandomID()
	if err != nil {
		return nil, 0, fmt.Errorf("Error occured while generating file ID: %v", err)
	}

	return file, fileID, nil
}

func (kv *KVStore) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.file != nil {
		return kv.file.Close()
	}
	return nil
}

func main() {
	kvStore, _ := Open("store.db")
	defer kvStore.Close()
	fmt.Println("Opened db")

	// Put data
	_ = kvStore.Put([]byte("God"), []byte("Greatest"))
	fmt.Println("Put data")
	_ = kvStore.Put([]byte("Me"), []byte("Sad"))

	// Retrieve data
	value, _ := kvStore.Get([]byte("God"))
	fmt.Printf("God => %s\n", value)

	value, _ = kvStore.Get([]byte("Me"))
	fmt.Printf("Me => %s\n", value)

	// update value
	_ = kvStore.Put([]byte("Me"), []byte("Wild"))
	value, _ = kvStore.Get([]byte("Me"))
	fmt.Printf("Me => %s\n", value)

	// delete data
	_ = kvStore.Delete([]byte("Tojumi"))
	value, err := kvStore.Get([]byte("Tojumi"))
	fmt.Print(err)
}
