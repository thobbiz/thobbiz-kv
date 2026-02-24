package main

import (
	"fmt"

	models "github.com/thobbiz/thobbixDB/definitions"
	"github.com/thobbiz/thobbixDB/helpers"
)

func Open(filepath string) (*models.KVStore, error) {
	file, fileID, err := helpers.NewFile(filepath)
	if err != nil {
		return nil, err
	}

	KVStore := models.NewStore(file, fileID)

	if err := KVStore.BuildIndex(); err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to rebuild index: %w", err)
	}

	return KVStore, nil
}

func main() {
	kvStore, _ := Open("store.db")
	defer kvStore.Close()
	fmt.Println("Opened db")

	// Put data
	_ = kvStore.Put([]byte("God"), []byte("Greatest"))
	fmt.Println("Put data")
	_ = kvStore.Put([]byte("Me"), []byte("Sad"))

	// Retrieve data
	value, _ := kvStore.Get([]byte("God"))
	fmt.Printf("God => %s\n", value)

	value, _ = kvStore.Get([]byte("Me"))
	fmt.Printf("Me => %s\n", value)

	// update value
	_ = kvStore.Put([]byte("Me"), []byte("Wild"))
	value, _ = kvStore.Get([]byte("Me"))
	fmt.Printf("Me => %s\n", value)

	// delete data
	_ = kvStore.Delete([]byte("Tojumi"))
	value, err := kvStore.Get([]byte("Tojumi"))
	fmt.Print(err)
}
