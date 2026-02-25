package definitions

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/thobbiz/thobbixDB/helpers"
)

func (kv *KVStore) BuildIndex() error {
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

		if record.TombStone {
			kv.keyTable.keyOffsetMap[string(record.Key)] = TombStoneOffset
		} else {
			kv.keyTable.keyOffsetMap[string(record.Key)] = offset
		}

		offset += int64(HeaderSize + len(record.Key) + len(record.Value))
	}

	return nil
}

// writeRecord inserts a record into the dataSegments active datafile,
//
// inserts the offset to the keyTable map and returns it
func (kv *KVStore) writeRecord(record *Record) (int64, error) {
	totalSize := HeaderSize + len(record.Key) + len(record.Value)
	buf := make([]byte, totalSize)

	// put fileID - 8 bytes
	binary.BigEndian.PutUint32(buf[0:8], uint32(record.FileId))

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

	offset, err := kv.dataSegments.append(buf)

	return offset, err
}

func (kv *KVStore) readRecord(offset int64) (*Record, error) {
	if _, err := kv.dataSegments.activeDS.file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("failed to seek: %w \n", err)
	}

	fileInfo, err := os.Stat(kv.dataSegments.activeDS.file.Name())
	if err != nil {
		return nil, fmt.Errorf("Couldn't check file stats: %v \n", err)
	}
	if (offset) > fileInfo.Size() {
		return nil, fmt.Errorf("File is too small, offset is %d and file size is %d \n", offset, fileInfo.Size())
	}

	record := &Record{}

	// read file ID (8 bytes)
	fileID := make([]byte, 8)
	if _, err := kv.dataSegments.activeDS.file.Read(fileID); err != nil {
		return nil, err
	}
	record.FileId = uint64(binary.BigEndian.Uint64(fileID))

	// read the timestamp (4 bytes)
	timestampBuf := make([]byte, 4)
	if _, err := kv.dataSegments.activeDS.file.Read(timestampBuf); err != nil {
		return nil, err
	}
	record.Timestamp = binary.BigEndian.Uint32(timestampBuf)

	// read the key len (4 bytes)
	keyLenBuf := make([]byte, 4)
	if _, err := kv.dataSegments.activeDS.file.Read(keyLenBuf); err != nil {
		return nil, err
	}
	keyLen := binary.BigEndian.Uint32(keyLenBuf)

	// read the value len (4 bytes)
	valueLenBuf := make([]byte, 4)
	if _, err := kv.dataSegments.activeDS.file.Read(valueLenBuf); err != nil {
		return nil, err
	}
	valueLen := binary.BigEndian.Uint32(valueLenBuf)

	// read the tombstone (1 byte)
	tombStoneBuf := make([]byte, 1)
	if _, err := kv.dataSegments.activeDS.file.Read(tombStoneBuf); err != nil {
		return nil, err
	}
	record.TombStone = tombStoneBuf[0] == 1

	// read key data
	key := make([]byte, keyLen)
	if _, err := kv.dataSegments.activeDS.file.Read(key); err != nil {
		return nil, err
	}
	record.Key = key

	// read value data
	value := make([]byte, valueLen)
	if _, err := kv.dataSegments.activeDS.file.Read(value); err != nil {
		return nil, err
	}
	record.Value = value

	return record, nil
}

// append rollovers a dataSegment if it has reached its limit and appends a buffer to the active dataSegment
func (dataSegments *DataSegments) append(buf []byte) (int64, error) {
	maxSizeReached, err := dataSegments.checkIfRolloverActiveSegment(buf)
	if err != nil {
		if maxSizeReached {
			// Archive old file
			dataSegments.inactiveDS[dataSegments.activeDS.fileId] = dataSegments.activeDS
			// Open a new file
			currentNo := len(dataSegments.inactiveDS) + 1
			fileName := helpers.GenerateFileName(currentNo)
			newFile, newFileId, err := helpers.NewFile(fileName)
			if err != nil {
				return 0, err
			}

			// create new datasegment and replace active DS
			activeDs := DataSegment{
				file:   newFile,
				fileId: newFileId,
			}
			dataSegments.activeDS = &activeDs

			// append buf to new File
			return dataSegments.activeDS.append(buf)
		}
		return 0, err
	}

	return dataSegments.activeDS.append(buf)
}

// append a buffer byte to a dataSegment file
func (dataSegment *DataSegment) append(buf []byte) (int64, error) {
	offset, err := dataSegment.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("Couldn't get file offset: %v", err)
	}

	bytesWritten, err := dataSegment.file.Write(buf)
	if err != nil {
		return 0, fmt.Errorf("Failed to write to file: %v", err)
	}

	if bytesWritten < len(buf) {
		return 0, fmt.Errorf("Could not append %v bytes", len(buf))
	}

	return offset, nil
}

// checkIfRolloverActiveSegment check if a dataSegments active dataSegments has reached
// its limits
func (dataSegments *DataSegments) checkIfRolloverActiveSegment(buf []byte) (bool, error) {
	info, err := os.Stat(dataSegments.activeDS.file.Name())
	if err != nil {
		return false, fmt.Errorf("Couldn't check file stats: %v", err)
	}
	if (info.Size() + int64(len(buf))) >= int64(dataSegments.maxDSSizeBytes) {
		return true, fmt.Errorf("Maximum file size reached: %v", err)
	}

	return true, nil
}
