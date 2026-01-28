package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

func (kv *KVStore) buildIndex() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var offset int64
	for {
		record, err := kv.readRecord(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %v", err)
		}

		if record.Tombstone {
			kv.index[string(record.Key)] = tombStoneOffset
		} else {
			kv.index[string(record.Key)] = offset
		}

		offset += int64(headerSize + len(record.Key) + len(record.Value))
	}

	return nil
}

func (kv *KVStore) writeRecord(record *Record) (int64, error) {
	totalSize := headerSize + len(record.Key) + len(record.Value)
	buf := make([]byte, totalSize)

	binary.BigEndian.PutUint32(buf[0:4], record.Timestamp)

	binary.BigEndian.PutUint32(buf[4:8], uint32(len(record.Key)))

	binary.BigEndian.PutUint32(buf[8:12], uint32(len(record.Value)))

	if record.TombStone {
		buf[12] = 1
	} else {
		buf[12] = 0
	}

	copy(buf[headerSize:headerSize+len(record.Key)], record.Key)
	copy(buf[headerSize+len(record.Key):], record.Value)

	offset, err := kv.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	if _, err := kv.file.Write(buf); err != nil {
		return 0, err
	}

	return offset, nil
}

func (kv *KVStore) readRecord(offset int64) (*Record, error) {
	if _, err := kv.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failes to seek: %w", err)
	}

	record := &Record{}

	// read the timestamp (4 bytes)
	timestampBuf := make([]byte, 4)
	if _, err := kv.file.Read(timestampBuf); err != nil {
		return nil, err
	}
	record.Timestamp = binary.BigEndian.Uint32(timestampBuf)

	// read the key len (4 bytes)
	keyLenBuf := make([]byte, 4)
	if _, err := kv.file.Read(keyLenBuf); err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	// read the value len (4 bytes)
	valueLenBuf := make([]byte, 4)
	if _, err := kv.file.Read(valueLenBuf); err != nil {
		return nil, err
	}
	valueLen := binary.BigEndian.Uint32(valueLenBuf)

	// read the tombstone (1 byte)
	tombStoneBuf := make([]byte, 1)
	if _, err := kv.file.Read(tombStoneBuf); err != nil {
		return nil, err
	}
	record.TombStone = tombStoneBuf[0] == 1

	// read key data
	key := make([]byte, keyLen)
	if _, err := kv.file.Read(key); err != nil {
		return nil, err
	}
	record.Key = key

	// read value data
	value := make([]byte, valueLen)
	if _, err := kv.file.Read(value); err != nil {
		return nil, err
	}
	record.Value = value

	return record, nil
}
