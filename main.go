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
