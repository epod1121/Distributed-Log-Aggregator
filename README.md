# Distributed Log Aggregator

A high-performance, lightweight distributed log aggregation engine written in Go. This project simulates a real-time event streaming platform (inspired by Apache Kafka) that ingests logs from high-traffic producer services over custom binary TCP protocols, indexes them on disk using $O(1)$ byte-offset mapping, and streams logs back to real-time consumer dashboards.

---

## Key Features

* **Custom Wire Protocol:** Low-overhead TCP protocol using prefix-length headers for topics and data payloads.
* **Protobuf Serialization:** Efficient binary payload serialization using Google Protocol Buffers (`.proto`).
* **$O(1)$ Disk Indexing:** In-memory byte-offset mapping (`topicOffsetMap`) allowing instant file seeking (`file.Seek`) without scanning entire log files.
* **Topic Isolation:** Automatic log partitioning into topic-specific storage files (`Logs/<topic>.log`).
* **Concurrent TCP Broker:** Thread-safe, non-blocking connection handler capable of handling simultaneous producer writes and consumer reads via Go routines.
* **Live Terminal UI:** In-place refreshing Linux terminal dashboard built using ANSI escape sequences (`\033[H\033[J`).
* **Traffic Simulator:** Built-in multi-event simulator generating real-time e-commerce activity (Payments, Cart additions, User Sign-ups).

---

## Architecture Overview

```text
 [Producer Traffic Simulator] 
         │
         │  (TCP / Protobuf Wire Protocol)
         ▼
 ┌─────────────────────────────────────────────────────────┐
 │                      LOG BROKER                         │
 │                                                         │
 │  1. handleConnection() -> Identifies client type        │
 │  2. acceptLog()         -> Reads topic + payload        │
 │  3. topicOffsetMap      -> Tracks Offset -> Byte Pos    │
 │  4. persistLog()        -> Appends raw bytes to disk    │
 └─────────────────────────────────────────────────────────┘
         │                                   │
         ▼ (File I/O)                        ▼ (Streams over TCP)
 ┌──────────────────┐                ┌──────────────────────────┐
 │ Disk Persistence │                │    Consumer Dashboard    │
 │ ├─ payment.log   │                │ (processLog - Live UI)   │
 │ ├─ cart.log      │                └──────────────────────────┘
 │ └─ signups.log   │
 └──────────────────┘

```

## Wire Protocol Contract

All network communications occur over raw TCP (default port `:9092`). The broker determines client intent using a **1-byte secret knock header**:

### 1. Producer Handshake

[1 byte: ID=1] ➔ [4 bytes: Topic Length] ➔ [X bytes: Topic Name] ➔ [4 bytes: Data Length] ➔ [Y bytes: Protobuf Payload]

### 2. Consumer Handshake

[1 byte: ID=2] ➔ [4 bytes: Topic Length] ➔ [X bytes: Topic Name] ➔ [8 bytes: Start Offset (int64)]

### Prerequisites
**Go:** Version 1.20 or higher

**Protocol Buffers Compiler (protoc):** Required only if modifying pb/log.proto

## Getting Started
### Clone the Repository
```bash
git clone [https://github.com/epod1121/Log-Aggregator.git](https://github.com/epod1121/Log-Aggregator.git)
cd Log-Aggregator
```

### Install Dependencies
```bash
go mod tidy
```

### Run Project
```bash
go run main.go
```
