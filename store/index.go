package store

import (
	"fmt"
	"io"
	"log"
)

type KeyTable struct {
	keyOffsetMap map[string]AppendRecordResponse
}

func (kv *KVStore) BuildIndex() error {
	log.Println("-- Building Index")
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// build index for inactive Data segments
	for _, ds := range kv.dataSegments.inactiveDS {
		var offset int64
		for {
			record, err := kv.readRecord(offset, ds.fileId)
			if err == io.EOF {
				break
			}
			if err != nil {
				return fmt.Errorf("failed to build index for segment %d: %v", ds.fileId, err)
			}

			if record.TombStone {
				kv.keyTable.keyOffsetMap[string(record.Key)] = AppendRecordResponse{FileId: ds.fileId, Offset: TombStoneOffset}
			} else {
				kv.keyTable.keyOffsetMap[string(record.Key)] = AppendRecordResponse{FileId: ds.fileId, Offset: offset}
			}

			offset += int64(HeaderSize + len(record.Key) + len(record.Value))
		}
	}

	var offset int64 = 0
	activeDS := kv.dataSegments.activeDS
	for {
		record, err := kv.readRecord(offset, activeDS.fileId)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to build index for active data segment: %v", err)
		}

		if record.TombStone {
			kv.keyTable.keyOffsetMap[string(record.Key)] = AppendRecordResponse{FileId: activeDS.fileId, Offset: TombStoneOffset}
		} else {
			kv.keyTable.keyOffsetMap[string(record.Key)] = AppendRecordResponse{FileId: activeDS.fileId, Offset: offset}
		}

		offset += int64(HeaderSize + len(record.Key) + len(record.Value))
	}

	return nil
}
