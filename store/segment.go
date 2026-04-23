package store

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/thobbiz/thobbixDB/internal/fileutil"
)

type DataSegments struct {
	activeDS       *DataSegment
	inactiveDS     map[uint64]*DataSegment
	maxDSSizeBytes uint64
}

type DataSegment struct {
	file   *os.File
	fileId uint64
}

type AppendRecordResponse struct {
	FileId uint64
	Offset int64
}

func (dataSegments *DataSegments) append(buf []byte) (*AppendRecordResponse, error) {
	maxSizeReached, err := dataSegments.checkIfRolloverActiveSegment(buf)
	if err != nil {
		if maxSizeReached {
			// Archive old file
			dataSegments.inactiveDS[dataSegments.activeDS.fileId] = dataSegments.activeDS
			// Open a new file
			currentNo := len(dataSegments.inactiveDS) + 1
			name := fileutil.GenerateFileName(currentNo)
			filePath := filepath.Join(DataDIR, name)
			newFile, newFileId, err := fileutil.NewFile(filePath)
			if err != nil {
				return nil, err
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
		return nil, err
	}

	return dataSegments.activeDS.append(buf)
}

// appends a buffer byte to a data segment file
func (dataSegment *DataSegment) append(buf []byte) (*AppendRecordResponse, error) {
	offset, err := dataSegment.file.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("failed to get file offset: %v", err)
	}

	bytesWritten, err := dataSegment.file.Write(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to write to file: %v", err)
	}

	if bytesWritten < len(buf) {
		return nil, fmt.Errorf("could not append %v bytes", len(buf))
	}

	result := AppendRecordResponse{
		FileId: dataSegment.fileId,
		Offset: offset,
	}

	return &result, nil
}

// checkIfRolloverActiveSegment checks if the active data segment has reached
// its limits
func (dataSegments *DataSegments) checkIfRolloverActiveSegment(buf []byte) (bool, error) {
	info, err := os.Stat(dataSegments.activeDS.file.Name())
	if err != nil {
		return false, fmt.Errorf("Couldn't check file stats: %v", err)
	}
	if (info.Size() + int64(len(buf))) >= int64(dataSegments.maxDSSizeBytes) {
		return true, fmt.Errorf("Maximum file size reached: %v", err)
	}

	return false, nil
}

// finds and returns the data segment with the fileID
func (kv *KVStore) findDataSegment(fileID uint64) (*os.File, error) {
	// check if file is the active dataSegment
	if kv.dataSegments.activeDS.fileId == fileID {
		return kv.dataSegments.activeDS.file, nil
	}

	// check file is in the inactive DataSegments
	for _, value := range kv.dataSegments.inactiveDS {
		if value.fileId == fileID {
			return value.file, nil
		}
	}

	return nil, errors.New("data segment couldn't be found")
}
