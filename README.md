# bobbob Project

Bobbob (Bunch Of Binary blOBs) is a simple Go application that provides a binary file storage system. It allows users to create a binary file, write objects to it, and read objects from it.

## Project Structure

```
bobbob
├── cmd
│   └── main.go
├── internal
│   ├── store
│   │   ├── store.go
│   │   └── store_test.go
├── go.mod
└── README.md
```

## Installation

To get started with the bobbob project, clone the repository and navigate to the project directory:

```bash
git clone <repository-url>
cd bobbob
```

## Setup

Make sure you have Go installed on your machine. You can download it from [golang.org](https://golang.org/dl/).

Run the following command to initialize the Go module:

```bash
go mod tidy
```

## Usage

1. **Create a new Store**: Use the `NewBob` method to create a new binary file at a specified path.
2. **Write an object**: Call `WriteObj` with the desired size to write an object to the store. This will return the current offset and an `io.Writer` to write data.
3. **Read an object**: Use `ReadObj` with the offset to read the object from the store, returning an `io.Reader`.

## Example

```go
package main

import (
    "fmt"
    "log"
    "bobbob/internal/store"
)

func main() {
    s := store.NewBob("path/to/binary/file")
    if err := s.NewBob("path/to/binary/file"); err != nil {
        log.Fatal(err)
    }

    offset, writer, err := s.WriteObj(1024)
    if err != nil {
        log.Fatal(err)
    }

    // Write data to the writer...

    reader, err := s.ReadObj(offset)
    if err != nil {
        log.Fatal(err)
    }

    // Read data from the reader...
}
```

## Contributing

Feel free to submit issues or pull requests to improve the bobbob project.