package definitions

import (
	"os"
	"sync"
)

const (
	// Using binary MB (1024 * 1024)
	MB              = 1 << 20
	MaxFileSize     = 500 * MB
	HeaderSize      = 21
	TombStoneOffset = -1
)

type KVStore struct {
	file         *os.File
	mu           sync.RWMutex
	keyTable     KeyTable
	dataSegments *DataSegments
}

type KeyTable struct {
	keyOffsetMap map[string]int64
}

type DataSegments struct {
	activeDS       *DataSegment
	inactiveDS     map[uint64]*DataSegment
	maxDSSizeBytes uint64
}

type DataSegment struct {
	file   *os.File
	fileId uint64
}

type AppendEntryResponse struct {
	FileId      uint64
	Offset      int64
	EntryLength uint32
}

type Record struct {
	FileId    uint64
	Key       []byte
	Value     []byte
	Timestamp uint32
	TombStone bool
}

func NewStore(file *os.File, fileID uint64) *KVStore {
	KVStore := &KVStore{
		file: file,
		keyTable: KeyTable{
			keyOffsetMap: make(map[string]int64),
		},
		dataSegments: &DataSegments{
			activeDS: &DataSegment{
				file:   file,
				fileId: fileID,
			},
			inactiveDS:     make(map[uint64]*DataSegment),
			maxDSSizeBytes: MaxFileSize,
		},
	}

	return KVStore
}

func (kv *KVStore) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.file != nil {
		return kv.file.Close()
	}
	return nil
}
