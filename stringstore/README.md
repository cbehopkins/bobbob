# StringStore

StringStore is a sharded, disk-backed store for variable-length strings that implements the `bobbob.Storer` interface.
Each shard is an internal file with a fixed maximum number of strings. When a shard fills, a new shard is created and
ObjectIds continue monotonically across shards.

## Key Behavior

- **Shard cap**: `MaxNumberOfStrings` applies per shard.
- **ObjectIds**: `StartingObjectId` + `ObjectIdInterval` define the external ID spacing; shards map ranges internally.
- **Buffered writes**: writes are buffered and flushed in batches to reduce system calls.
- **Delete + reuse**: deleted ObjectIds can be reused within a shard via a freelist.
- **Compaction**: `Compact()` rewrites shard files to remove gaps from deletes.

## Usage

```go
cfg := stringstore.Config{
    FilePath:           "strings.blob",
    MaxNumberOfStrings: 1_000_000,
    StartingObjectId:   100,
    ObjectIdInterval:   4,
}

store, err := stringstore.NewStringStore(cfg)
if err != nil {
    // handle err
}

defer store.Close()

id, err := store.NewObj(5)
if err != nil {
    // handle err
}

writer, finisher, err := store.WriteToObj(id)
if err != nil {
    // handle err
}
_, _ = writer.Write([]byte("hello"))
_ = finisher()
```

## Notes

- `Compact()` should be run when no active readers are using prior `LateReadObj` readers.
- Shard files are derived from `FilePath` with a `-NNNN` suffix.

## Tests

```
go test ./stringstore -v
```
