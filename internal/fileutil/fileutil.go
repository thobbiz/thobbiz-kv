package fileutil

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"
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

func GenerateFileName(index int) string {
	return string("store" + strconv.Itoa(index) + ".db")
}

func GenerateRandomID() (uint64, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return 0, err
	}
	// Convert the 8 bytes into a uint64
	return binary.BigEndian.Uint64(b), nil
}
