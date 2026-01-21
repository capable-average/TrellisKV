<div align="center">

# ⋈ TrellisKV ⋈  
### A Lightweight Distributed Key–Value Store for Learning Modern Distributed Systems

[![License](https://img.shields.io/badge/license-MIT-blue.svg)]()
[![Language](https://img.shields.io/badge/C%2B%2B-17-blue.svg)]()
[![Build](https://img.shields.io/badge/build-CMake-green.svg)]()
[![Status](https://img.shields.io/badge/status-educational-orange.svg)]()

</div>

---

TrellisKV is a small, readable, **eventually-consistent distributed key–value store** inspired by systems like Amazon DynamoDB and Apache Cassandra.

#### Note

TrellisKV is not intended for production. It is purely built to learn about distributed system design concepts and their implementation. For production purposes, use Redis, Valkey, Cassandra, etc. based on your needs.

---

## Core Features

- **Consistent Hashing** with virtual nodes for balanced partitioning
- **Gossip-based membership & failure detection**
- **Async replication** with last-writer-wins conflict resolution
- **Request routing** with automatic forwarding if a node doesn’t own a key
- **Hybrid TTL + LRU** storage engine
- **TCP + UDS networking** with persistent connection pooling
- **Multithreaded worker pool** for request processing

// small architecture diagram

Detailed discussion on architecture in -> **[ARCHITECTURE](docs/ARCHITECTURE.md)**

---

## Quick Start

### Prerequisites

- CMake 3.20+
- C++17 compiler (Clang 5+, GCC 7+)
  - Note: if using GCC, edit the "CMAKE_CXX_COMPILER" field in CMakePresets.json
- Ninja build tool
  - If using `make`, edit the "generator" field in CmakePresets.json

### Building from Source

```bash
git clone https://github.com/sk-pathak/trellisKV
cd trellisKV
```

### Development Build:

```bash
cmake --preset dev
cmake --build --preset dev
```

For full IDE support (code completion, etc), you probably should do:

```bash
ln -sf build/dev/compile_commands.json compile_commands.json
```

### Production Build:

```bash
cmake --preset prod
cmake --build --preset prod
```

Build artifacts are placed under:
- `build/dev`
- `build/prod`

### Run a node:
```bash
./build/dev/trellis <port> [seed_host:seed_port]
```

Example: Start a standalone node
```bash
./build/dev/trellis 5000
```

Start a second node and join cluster:
```bash
./build/dev/trellis 5001 127.0.0.1:5000 # or localhost:5000
```

### Use the CLI:
```bash
./build/dev/trellis-cli localhost:5000 put mykey "hello"
./build/dev/trellis-cli localhost:5001 get mykey
```
---

Full details in: **[USAGE](docs/USAGE.md)**

---

## Benchmarks

These values depend on hardware but illustrate expected scale:

- **Latency**: Single-node GET ~100-500µs, PUT ~200-800µs (in-memory)
- **Throughput**: ~10K-50K ops/sec per node (depends on read/write ratio)
- **Scalability**: Horizontal scaling via partitioning; tested up to ~10 nodes

See full numbers & methodology in: **[BENCHMARKS](docs/BENCHMARKS.md)**

---

## Future Enhancement Plans

- Event-driven async I/O with epoll/kqueue (replace blocking thread-per-connection model)
- Vector clocks for causal consistency
- Strong consistency with quorum reads/writes
- Read-your-writes consistency guarantees
- Automatic data rebalancing on node join/leave
- Parallel Algos (any possibility??)

---

## Acknowledgements

Here are some resources that were extremely useful:

- Designing Data Intensive Applications
- Beej's Network Programming Guide
- The Linux Programming Interface
- CPP Concurrency in Action
- [Build Your Own Redis in C/C++](https://build-your-own.org/redis/)
