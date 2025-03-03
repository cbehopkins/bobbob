package main

import (
	"fmt"
	"log"
	"os"

	"bobbob/internal/store"
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("Please provide a file path for the store.")
	}

	filePath := os.Args[1]
	s, err := store.NewBob(filePath)
	if err != nil {
		log.Fatalf("Failed to create store: %v", err)
	}
	defer s.Close()

	fmt.Printf("Store created at: %s\n", filePath)

	// Example usage of WriteObj and ReadObj
	offset, writer, err := s.WriteNewObj(10)
	if err != nil {
		log.Fatalf("Failed to write object: %v", err)
	}

	data := []byte("testdata")
	if _, err := writer.Write(data); err != nil {
		log.Fatalf("Failed to write data: %v", err)
	}

	fmt.Printf("Data written at offset: %d\n", offset)

	reader, err := s.ReadObj(offset)
	if err != nil {
		log.Fatalf("Failed to read object: %v", err)
	}

	readData := make([]byte, len(data))
	if _, err := reader.Read(readData); err != nil {
		log.Fatalf("Failed to read data: %v", err)
	}

	fmt.Printf("Data read: %s\n", string(readData))
}