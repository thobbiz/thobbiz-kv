package main

import (
	"fmt"
	"log"

	"github.com/thobbiz/thobbixDB/store"
)

func main() {
	kvStore, err := store.Open("./data/")
	if err != nil {
		log.Fatal(err)
	}
	log.Println("--Opened KV--")
	defer kvStore.Close()

	// Put data
	err = kvStore.Put([]byte("God"), []byte("Greatest"))
	if err != nil {
		log.Fatal(err)
	}
	log.Println("-- Insert Data")

	// Retrieve data
	value, err := kvStore.Get([]byte("God"))
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("-- Retrieved Data \n")
	fmt.Printf("God => %s\n", value)

	// delete data
	log.Println("-- Deleted Data")
	err = kvStore.Delete([]byte("God"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = kvStore.Get([]byte("God"))
	if err != nil {
		log.Fatal(err)
	}
}
