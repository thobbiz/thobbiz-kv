package main

import (
	"encoding/binary"
	"fmt"
	"io"
)

func (s *Store) buildIndex() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var offset int64
	for {
		record, err := s.readRecord(offset)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to read record: %v", err)
		}

		if record.Tombstone {
			s.index[string(record.Key)] = tombStoneOffset
		} else {
			s.index[string(record.Key)] = offset
		}

		offset += int64(headerSize + len(record.Key) + len(record.Value))
	}

	return nil
}

func (s *Store) writeRecord(record *Record) (int64, error) {
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

	offset, err := s.file.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}

	if _, err := s.file.Write(buf); err != nil {
		return 0, err
	}

	return offset, nil
}
