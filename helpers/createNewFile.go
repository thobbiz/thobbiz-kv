package helpers

import (
	"fmt"
	"os"
)

func NewFile(filepath string) (*os.File, uint64, error) {
	file, err := os.OpenFile(filepath, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}

	fileID, err := GenerateRandomID()
	if err != nil {
		return nil, 0, fmt.Errorf("Error occured while generating file ID: %v", err)
	}

	return file, fileID, nil
}
