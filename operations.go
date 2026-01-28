package main

import (
	"fmt"
	"time"
)

func (kv *KVStore) Put(key, value []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	record := &Record{
		Key:       key,
		Value:     value,
		Timestamp: uint32(time.Now().Unix()),
	}

	offset, err := kv.writeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	kv.index[string(key)] = offset
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	kv.mu.Lock()
	defer kv.mu.RUnlock()

	offset, exists := kv.index[string(key)]
	if !exists || offset == tombstoneOffset {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	record, err := kv.readRecord(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	return record.Value, nil
}
