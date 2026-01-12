# PetiteDB

A lightweight database management system implementation for the COM 3563 (Database Implementation) project.

## Overview

PetiteDB is a simplified relational DBMS designed to implement many of the common layers in database system architecture. The implementation uses the following layered approach:

```
┌─────────────────────────────────────────┐
│           Table & Index Access          │  ← Metadata catalog, indexing
├─────────────────────────────────────────┤
│           Record Management             │  ← Schema, layout, record slots
├─────────────────────────────────────────┤
│           Transaction Management        │  ← ACID, concurrency, recovery
├─────────────────────────────────────────┤
│           Buffer Management             │  ← In-memory page caching
├─────────────────────────────────────────┤
│           Log Management                │  ← Write-ahead logging (WAL)
├─────────────────────────────────────────┤
│           File Management               │  ← Block-level disk I/O
└─────────────────────────────────────────┘
```

**From Files to Tables:**

1. **File Layer**: At the lowest level, the database views storage as a collection of fixed-size _blocks_. The `FileMgr` reads and writes entire blocks to/from disk, abstracting away the OS file system details.

2. **Log Layer**: Before any data changes are written to disk, they are first recorded in a sequential log file. This _write-ahead logging_ (WAL) ensures that the database can recover to a consistent state after a crash.

3. **Buffer Layer**: Since disk I/O is slow, the `BufferMgr` maintains a pool of in-memory _buffers_ that cache frequently accessed blocks. It handles _pinning_ (locking a buffer for use) and _eviction_ (choosing which buffer to reuse when the pool is full).

4. **Transaction Layer**: The `Tx` module provides ACID guarantees. It coordinates with the _concurrency manager_ (for isolation via locking) and the _recovery manager_ (for atomicity and durability via the log). All data access flows through transactions.

5. **Record Layer**: This layer interprets raw bytes in a block as structured _records_. A `Schema` defines field names and types; a `Layout` computes byte offsets so records can be packed into fixed-size _slots_ within a block.

6. **Metadata Layer**: The `TableMgr` maintains the _system catalog_—tables that store information about all other tables (names, schemas, field metadata). This is how the database knows what tables exist and how they're structured.

7. **Index Layer**: To avoid scanning entire tables, the `IndexMgr` creates and manages _indexes_ (currently static hash indexes). Indexes map search keys directly to record locations.

## Full Architecture

The system is organized into the following modules:

### File Module (`edu.yu.dbimpl.file`)

The lowest layer of the PetiteDB stack. Provides block-level access to database files.

- **FileMgr**: Handles direct interaction with the OS file system, reading and writing fixed-size blocks
- **BlockId**: Identifies a specific block within a file
- **Page**: Represents a main-memory page for reading/writing data

### Log Module (`edu.yu.dbimpl.log`)

Manages the write-ahead log (WAL) for crash recovery.

- **LogMgr**: Writes and reads log records, supporting transaction recovery

### Buffer Module (`edu.yu.dbimpl.buffer`)

Manages a pool of main-memory buffers for caching disk blocks.

- **BufferMgr**: Handles buffer allocation, pinning/unpinning, and eviction policies (NAIVE, CLOCK)
- **Buffer**: Represents a single buffer slot containing a disk block

### Transaction Module (`edu.yu.dbimpl.tx`)

Provides ACID transaction support with concurrency control and recovery.

- **Tx**: Transaction interface with commit, rollback, and recovery operations
- **TxMgr**: Factory for creating new transactions
- Submodules for concurrency control and recovery management

### Record Module (`edu.yu.dbimpl.record`)

Manages record storage within blocks.

- **Schema**: Defines the logical structure of a table (field names, types)
- **Layout**: Maps logical schema to physical storage with offsets and slot sizes

### Metadata Module (`edu.yu.dbimpl.metadata`)

Maintains the system catalog.

- **TableMgr**: Stores and retrieves table metadata (table catalog, field catalog)

### Index Module (`edu.yu.dbimpl.index`)

Provides indexing capabilities for efficient data access.

- **IndexMgr**: Creates and manages index metadata
- **Index**: Interface for index operations (static hash indexing supported)

## Requirements

- **Java**: 17
- **Maven**: 3.x
- **Dependencies**:
  - JUnit Jupiter 5.8.1 (testing)
  - Log4j 2.20.0 (logging)
