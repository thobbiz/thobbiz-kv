package store

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type Record struct {
	FileId    uint64
	Key       []byte
	Value     []byte
	Timestamp uint32
	TombStone bool
}

// writeRecord inserts a record into the dataSegments active datafile,
//
// inserts the offset to the keyTable map and returns it
func (kv *KVStore) writeRecord(record *Record) (*AppendRecordResponse, error) {
	totalSize := HeaderSize + len(record.Key) + len(record.Value)
	buf := make([]byte, totalSize)

	// put fileID - 8 bytes
	binary.BigEndian.PutUint64(buf[0:8], record.FileId)

	// put timestamp - 4 bytes
	binary.BigEndian.PutUint32(buf[8:12], record.Timestamp)

	// put key length - 4 bytes
	binary.BigEndian.PutUint32(buf[12:16], uint32(len(record.Key)))

	// put value length - 4 bytes
	binary.BigEndian.PutUint32(buf[16:20], uint32(len(record.Value)))

	// put tombstone - 1 byte
	if record.TombStone {
		buf[20] = 1
	} else {
		buf[20] = 0
	}

	copy(buf[HeaderSize:HeaderSize+len(record.Key)], record.Key)
	copy(buf[HeaderSize+len(record.Key):], record.Value)

	appendRecordResponse, err := kv.dataSegments.append(buf)

	return appendRecordResponse, err
}

func (kv *KVStore) readRecord(offset int64, dsFileID uint64) (*Record, error) {
	dsFile, err := kv.findDataSegment(dsFileID)
	if err != nil {
		return nil, err
	}

	info, err := os.Stat(dsFile.Name())
	if err != nil {
		return nil, fmt.Errorf("Couldn't check file stats: %v", err)
	}

	if offset == info.Size() {
		return nil, io.EOF
	}

	headerBuf := make([]byte, HeaderSize)
	if _, err := dsFile.ReadAt(headerBuf, offset); err != nil {
		return nil, fmt.Errorf("failed to read record header: %w", err)
	}

	record := &Record{}
	// read file ID (8 bytes)
	record.FileId = uint64(binary.BigEndian.Uint64(headerBuf[0:8]))
	// read the timestamp (4 bytes)
	record.Timestamp = binary.BigEndian.Uint32(headerBuf[8:12])
	// read the key len (4 bytes)
	keyLen := binary.BigEndian.Uint32(headerBuf[12:16])
	// read the value len (4 bytes)
	valueLen := binary.BigEndian.Uint32(headerBuf[16:20])
	// read the tombstone (1 byte)
	record.TombStone = headerBuf[20] == 1

	bodyBuf := make([]byte, keyLen+valueLen)
	if _, err := dsFile.ReadAt(bodyBuf, offset+HeaderSize); err != nil {
		return nil, fmt.Errorf("failed to read record body: %w", err)
	}

	record.Key = bodyBuf[:keyLen]
	record.Value = bodyBuf[keyLen:]

	return record, nil
}
