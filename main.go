package main

import (
	"fmt"
	"os"
)

func Open(filename string) (*KVStore, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	KVStore := &KVStore{
		filename: filename,
		file:     file,
		index:    make(map[string]int64),
	}

	if err := KVStore.buildIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to rebuild index: %w", err)
	}

	return KVStore, nil
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

	// Put data
	_ = kvStore.Put([]byte("God"), []byte("Greatest"))
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
}
