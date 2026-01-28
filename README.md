# thobbizKV
thobbizKV is an LHST and append-only key value store written in Go and based on the Bitcask model.
I built it as a fun project to expand my horizon on using go as a systems language

## Features
- In-Memory Hash Table for fast lookups
- Append-Only write mechanism
- Basic CRUD operations:
  - Put: add/update key-value pairs
  - Get: Retrieve values by keys
  - Delete: Remove key-value pairs
 
## Getting Started
### Prerequisites
- Go 1.23 or later
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

### Contributing
Contributions are welcome! Feel free to fork the repository and submit a pr.

### Acknowledgments
Inspired by the **[minkv](https://github.com/galalen/minkv)** | **[py-caskdb](https://github.com/avinassh/py-caskdb)** | **[bitcask](https://github.com/basho/bitcask)** | **[tinykv](https://github.com/talent-plan/tinykv)**
