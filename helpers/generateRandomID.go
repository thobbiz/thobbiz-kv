package helpers

import (
	"crypto/rand"
	"encoding/binary"
)

func GenerateRandomID() (uint64, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		return 0, err
	}
	// Convert the 8 bytes into a uint64
	return binary.BigEndian.Uint64(b), nil
}
