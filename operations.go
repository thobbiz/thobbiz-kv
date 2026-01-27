package main

import (
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
