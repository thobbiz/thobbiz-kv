package main

import (
	"os"
	"sync"
)

type KVStore struct {
	filename string
	file     *os.File
	mu       sync.RWMutex
	index    map[string]int64
}

type Record struct {
	Key       []byte
	Value     []byte
	Timestamp uint32
	ExpiresAt uint64
	TombStone bool
}
