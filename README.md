# thobbiz-kv
thobbizKV is an append-only, persistent key-value store written in Go, modelled after the [Bitcask](https://riak.com/assets/bitcask-a-log-structured-hash-table-for-fast-key-value-data.pdf) storage model. All writes go to an active log segment, with an in-memory hash index mapping each key to its byte offset on disk. Reads are a single disk seek. I built it as a deep dive into storage engine internals and systems-level Go.

## How It Works
- **Writes** are appended to the active data segment as fixed-format binary records.
- **Reads** use the in-memory key table to find the exact byte offset, then do a single `ReadAt` against the correct segment file.
- **Deletes** write a tombstone record rather than modifying existing data. The key is marked with a sentinel offset (`-1`) in the index.
- **Segment rollover** automatically archives the active segment and opens a new one when it reaches 500 MB.
- **Index recovery** on `Open` replays all segment files in order (inactive first, then active) to rebuild the full key table.

## Record Format
Each record written to disk has a 21-byte header followed by the key and value bytes:

| Field      | Size    | Description                        |
|------------|---------|------------------------------------|
| File ID    | 8 bytes | ID of the segment file             |
| Timestamp  | 4 bytes | Unix timestamp of the write        |
| Key Len    | 4 bytes | Length of the key in bytes         |
| Value Len  | 4 bytes | Length of the value in bytes       |
| Tombstone  | 1 byte  | `1` if this is a delete marker     |
| Key        | N bytes | Raw key bytes                      |
| Value      | M bytes | Raw value bytes                    |

## Features
- Append-only writes for sequential I/O performance
- In-memory hash index for O(1) average-case reads
- Concurrent reads via `sync.RWMutex`
- Automatic segment rollover at 500 MB
- Tombstone-based deletes
- Full index recovery on startup across multiple segments
- Benchmark suite comparing sequential and concurrent throughput against `sync.Map`

## Project Structure
```
thobbizKV/
├── store/
│   ├── store.go       # KVStore struct, Open, Close, NewStore
│   ├── index.go       # KeyTable and BuildIndex
│   ├── segment.go     # DataSegment(s), append, rollover logic
│   ├── record.go      # Binary record encoding and decoding
│   └── ops.go         # Public API: Put, Get, Delete
├── internal/
│   └── fileutil/
│       └── fileutil.go  # File creation, ID generation, name generation
├── bench_test.go
├── main.go
└── go.mod
```

## Getting Started
### Prerequisites
- Go 1.24 or later

### Installation
1. Clone the repository:
   ```bash
   git clone https://github.com/thobbiz/thobbizKV.git
   cd thobbizKV
   ```

2. Build the project:
   ```bash
   go build -o thobbizKV
   ```

3. Run the example:
   ```bash
   ./thobbizKV
   ```

### Usage
```go
// Open (or create) a store at a directory
kv, err := store.Open("./data")
if err != nil {
    log.Fatal(err)
}
defer kv.Close()

// Write
kv.Put([]byte("name"), []byte("thobbiz"))

// Read
value, err := kv.Get([]byte("name"))

// Delete
kv.Delete([]byte("name"))
```

### Benchmarks
Run the benchmark suite against `sync.Map`:
```bash
go test -bench=. -benchmem
```

## Contributing
Contributions are welcome! Feel free to fork the repository, tweak it and submit a PR.

## Acknowledgments
Inspired by **[minkv](https://github.com/galalen/minkv)** | **[py-caskdb](https://github.com/avinassh/py-caskdb)** | **[bitcask](https://github.com/basho/bitcask)** | **[tinykv](https://github.com/talent-plan/tinykv)**
