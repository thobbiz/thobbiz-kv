package main

import (
	"fmt"
	"log"

	models "github.com/thobbiz/thobbixDB/definitions"
)

func main() {
	kvStore, _ := models.Open("./data")
	fmt.Println("--Opened KV--")
	defer kvStore.Close()

	// Put data
	_ = kvStore.Put([]byte("God"), []byte("Greatest"))
	fmt.Println("- Insert Data")

	// Retrieve data
	value, err := kvStore.Get([]byte("God"))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("- Retrieved Data \n")
	fmt.Printf("God => %s\n", value)

	// delete data
	_ = kvStore.Delete([]byte("God"))
	_, err = kvStore.Get([]byte("God"))
	fmt.Println(err)
}
