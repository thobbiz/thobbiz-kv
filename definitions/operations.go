package definitions

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
		FileId:    kv.dataSegments.activeDS.fileId,
		Key:       key,
		Value:     value,
		Timestamp: uint32(time.Now().Unix()),
	}

	offset, err := kv.writeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	kv.keyTable.keyOffsetMap[string(key)] = offset
	fmt.Println("Worked pa")
	return nil
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	offset, exists := kv.keyTable.keyOffsetMap[string(key)]
	if !exists || offset == TombStoneOffset {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	record, err := kv.readRecord(offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	return record.Value, nil
}

func (kv *KVStore) Delete(key []byte) error {
	if len(key) == 0 {
		return fmt.Errorf("key cannot be empty")
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	record := Record{
		Key:       key,
		TombStone: true,
		Timestamp: uint32(time.Now().Unix()),
	}

	_, err := kv.writeRecord(&record)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	delete(kv.keyTable.keyOffsetMap, string(key))
	// kv.index[string(key)] = tombStoneOffset
	return nil
}
