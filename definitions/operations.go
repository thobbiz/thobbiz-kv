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

	appendRecordResponse, err := kv.writeRecord(record)
	if err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	kv.keyTable.keyOffsetMap[string(key)] = *appendRecordResponse
	return nil
}

// Get takes the Key passed to it to retrieve the offset from the
//
// in-memory map then use it to retrieve the value from the data file
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key cannot be empty")
	}

	kv.mu.RLock()
	defer kv.mu.RUnlock()

	appendRecordResponse, exists := kv.keyTable.keyOffsetMap[string(key)]
	if !exists || appendRecordResponse.Offset == TombStoneOffset {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	record, err := kv.readRecord(appendRecordResponse.Offset, appendRecordResponse.FileId)
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	return record.Value, nil
}

// Delete takes the Key passed to it to insert a tombstone offset to the
//
// in-memory map it then use it to retrieve the value from the data file
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
	kv.keyTable.keyOffsetMap[string(key)] = AppendRecordResponse{Offset: TombStoneOffset}
	return nil
}
