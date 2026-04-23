package store

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/thobbiz/thobbixDB/internal/fileutil"
)

const (
	// Using binary MB (1024 * 1024)
	MB              = 1 << 20
	MaxFileSize     = 500 * MB
	HeaderSize      = 21
	TombStoneOffset = -1
)

var DataDIR string

type KVStore struct {
	mu           sync.RWMutex
	keyTable     KeyTable
	dataSegments *DataSegments
}

func Open(dataDir string) (*KVStore, error) {
	kv := NewStore()

	DataDIR = dataDir
	files, err := os.ReadDir(DataDIR)
	if err != nil {
		return nil, fmt.Errorf("failed to read data directory: %w", err)
	}

	var fileNames []string
	for _, f := range files {
		if !f.IsDir() && strings.HasSuffix(f.Name(), ".db") {
			fileNames = append(fileNames, f.Name())
		}
	}

	sort.Strings(fileNames)

	if len(fileNames) == 0 {
		name := fileutil.GenerateFileName(1)
		filePath := filepath.Join(DataDIR, name)
		file, fileID, err := fileutil.NewFile(filePath)
		if err != nil {
			return nil, err
		}
		kv.dataSegments.activeDS.file = file
		kv.dataSegments.activeDS.fileId = fileID
	} else {
		// Load existing files
		for i, name := range fileNames {
			filePath := filepath.Join(dataDir, name)

			// Use os.OpenFile to avoid overwriting
			file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
			if err != nil {
				return nil, err
			}

			// Extract ID from filename
			id, err := fileutil.GenerateRandomID()
			if err != nil {
				return nil, fmt.Errorf("failed to generate file id: %v", err)
			}
			ds := &DataSegment{file: file, fileId: id}

			// If it's the last file, it's the active one
			if i == len(fileNames)-1 {
				kv.dataSegments.activeDS = ds
			} else {
				// Otherwise, it's inactive
				kv.dataSegments.inactiveDS[id] = ds
			}
		}

		// Build Index
		if err := kv.BuildIndex(); err != nil {
			return nil, err
		}
	}

	return kv, nil
}

func (kv *KVStore) Close() error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	var firstErr error

	for _, ds := range kv.dataSegments.inactiveDS {
		if err := ds.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	if kv.dataSegments.activeDS != nil {
		if err := kv.dataSegments.activeDS.file.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

func NewStore() *KVStore {
	KVStore := &KVStore{
		keyTable: KeyTable{
			keyOffsetMap: make(map[string]AppendRecordResponse),
		},
		dataSegments: &DataSegments{
			activeDS:       &DataSegment{},
			inactiveDS:     make(map[uint64]*DataSegment),
			maxDSSizeBytes: MaxFileSize,
		},
	}

	return KVStore
}
