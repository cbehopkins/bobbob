package main

import (
	"fmt"
	"log"
	"os"

	"github.com/cbehopkins/bobbob/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Please provide a file path for the store.")
	}

	filePath := os.Args[1]
	s, err := store.NewBasicStore(filePath)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	fmt.Printf("Store created at: %s\n", filePath)

	// Example usage of WriteObj and ReadObj
	offset, writer, finisher, err := s.LateWriteNewObj(10)
	if err != nil {
		log.Fatalf("Failed to write object: %v", err)
	}

	data := []byte("testdata")
	if _, err := writer.Write(data); err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}
	if finisher != nil {
		finisher()
	}
	fmt.Printf("Data written at offset: %d\n", offset)

	reader,finisher, err := s.LateReadObj(store.ObjectId(offset))
	if err != nil {
		log.Fatalf("Failed to read object: %v", err)
	}
	readData := make([]byte, len(data))
	if _, err := reader.Read(readData); err != nil {
		log.Fatalf("Failed to read data: %v", err)
	}
	if finisher != nil {
		defer finisher()
	}

	fmt.Printf("Data read: %s\n", string(readData))
}