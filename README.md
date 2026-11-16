# Overleaf FUSE Filesystem (`overleaf-mount`)

A high-performance FUSE filesystem written in Rust using [`cberner/fuser`](https://github.com/cberner/fuser) that exposes an Overleaf project as a local POSIX filesystem with real-time bidirectional synchronization, in-memory caching, and robust conflict resolution.

---

## Key Features

1. **POSIX Filesystem via `cberner/fuser`**:
   - Mounts any Overleaf project as a standard local directory.
   - Supports transparent document and directory operations: `read`, `write`, `create`, `mkdir`, `rename`, `unlink`, `rmdir`, and `getattr`.
   - Thread-safe in-memory caching for low-latency lookups and directory traversals.

2. **Real-time Bidirectional WebSockets Synchronization**:
   - Implements the Overleaf / Socket.IO 0.9 WebSocket protocol (`joinProject`, `joinDoc`, `applyOtUpdate`, `otUpdateApplied`).
   - Every remote edit by collaborators is synchronized immediately to the local FUSE filesystem.
   - Every local edit made to files in the mount is diffed and synchronized immediately to Overleaf.

3. **Robust Authoritative Conflict Resolution**:
   - Implements 3-way sentence- and line-level Operational Transformation reconciliation (`ot::reconcile_conflict`).
   - **Guaranteed Authoritative Behavior**: When two collaborators edit the same part or sentence concurrently, our version is authoritative and overrides the remote conflicting text on both the filesystem and the Overleaf server.
   - Non-conflicting edits in different sections or lines are cleanly reconciled and merged without data loss.

4. **Optimized Sync with Transparent Fallback**:
   - **Fast-Sync Mode (Optimized)**: Added `POST /project/:Project_id/doc/:Doc_id/fast-sync` endpoint to Overleaf web service, setting documents directly in Redis and Mongo for instant synchronization without OT overhead.
   - **Fallback Mode (`--no-fast-sync`)**: Automatically falls back to standard Socket.IO ShareJS text OT operations (`applyOtUpdate` with Myers diff components and server state recovery via `joinDoc`), ensuring 100% compatibility with standard, unmodified Overleaf servers.

5. **`.gitignore` Filtering & Local In-Memory Retention (via `ignore` crate)**:
   - Dynamically parses `.gitignore` rules at the project root and in subdirectories using the `ignore` library.
   - Matches wildcards, directory patterns, and whitelists (e.g. `*.log`, `build/`, `!keep.log`).
   - Any matching files or directories are kept strictly in memory: they are never sent to Overleaf via WebSocket OT or HTTP REST.
   - Preserves full local read/write/stat/truncate/delete functionality in memory for tools and compilers.
   - Automatically re-evaluates ignore matchers whenever `.gitignore` is created, updated, or deleted locally or remotely.

6. **POSIX Attributes, Executable Defaults & Mount Cleanup**:
   - Default ownership configurable via `--uid` and `--gid` (or `OVERLEAF_UID`/`OVERLEAF_GID`).
   - In-memory tracking of `chmod` and `chown`.
   - All `.sh` files are granted executable permission (`0755`) by default.
   - Traps `SIGINT` (Ctrl+C), `SIGTERM`, and `SIGHUP` to safely and cleanly unmount the FUSE filesystem without leaving dangling mounts.

---

## Architecture Overview

```
                                +---------------------------+
                                | Local Filesystem / User   |
                                +-------------+-------------+
                                              | POSIX calls
                                              v
                                +---------------------------+
                                |  OverleafFs (cberner/     |
                                |         fuser)            |
                                +-------------+-------------+
                                              |
                       +----------------------+----------------------+
                       |                                             |
                       v                                             v
          +-------------------------+                   +-------------------------+
          |         FsState         |                   |     OverleafClient      |
          | - Inodes & In-Memory    |                   | - HTTP REST Client      |
          |   File Content Cache    |                   | - WebSockets (Socket.IO)|
          | - Historical Snapshots  |                   | - Fast-Sync / Pure OT   |
          | - 3-Way OT Reconcile    |                   +------------+------------+
          +-------------------------+                                |
                                                                     | WebSockets & HTTP
                                                                     v
                                                        +-------------------------+
                                                        | Overleaf Server (Dev /  |
                                                        | Production Instance)    |
                                                        +-------------------------+
```

---

## Build & Installation

### Prerequisites

- Rust 1.98+ (`cargo`)
- `libfuse3-dev` (or `fuse3`)
- Node.js 24+ (for running automated verification suites)

### Compiling

```bash
cargo build --release
```

The compiled binary will be placed at `target/release/overleaf-mount`.

---

## Usage

### Mounting a Project

```bash
# Optimized mode (default, uses fast-sync REST route if available)
./target/release/overleaf-mount \
  --url http://localhost:8088 \
  --email admin@example.com \
  --password Password123! \
  --project 6afacb5e0082f5af0cb6d6f7 \
  --mount-point /path/to/mount

# Set custom User ID and Group ID to own all files and folders
./target/release/overleaf-mount \
  --mount-point /path/to/mount \
  --uid 1000 \
  --gid 1000

# Alternatively using environment variables
OVERLEAF_UID=1000 OVERLEAF_GID=1000 ./target/release/overleaf-mount \
  --mount-point /path/to/mount

# Fallback mode (for unmodified Overleaf servers without fast-sync)
./target/release/overleaf-mount \
  --url http://localhost:8088 \
  --email admin@example.com \
  --password Password123! \
  --project 6afacb5e0082f5af0cb6d6f7 \
  --mount-point /path/to/mount \
  --no-fast-sync
```

### Unprivileged User Namespace Mounting

When running in sandboxes or unprivileged containers where `/dev/fuse` is restricted by `no-new-privs`:
```bash
./target/release/overleaf-mount \
  --mount-point /tmp/overleaf \
  --exec "bash"
```
The `--exec` flag creates a private Linux mount and user namespace (`CLONE_NEWNS | CLONE_NEWUSER`), mounts FUSE inside the sandbox, executes the command, and cleanly unmounts upon completion.

---

## Automated Verification Suite

A comprehensive end-to-end verification suite is included in `tests/scenario_runner.mjs`.

### Tested Scenarios

1. **Scenario 1: Local FUSE Write $\to$ Overleaf Real-time Sync**:
   - Writes new content to `main.tex` via local filesystem call.
   - Verifies the Overleaf real-time service and MongoDB receive and persist the change.
2. **Scenario 2: Remote Collaborator Edit $\to$ FUSE Real-time Sync**:
   - A remote collaborator sends a ShareJS OT insert via WebSocket to `sample.bib`.
   - Verifies the FUSE filesystem immediately synchronizes and reflects the edit locally.
3. **Scenario 3: Concurrent Non-Conflicting Edits Reconciled**:
   - Local FUSE author edits Section 1 while remote collaborator edits Section 2 simultaneously.
   - Verifies that both non-conflicting edits are preserved and cleanly merged in both FUSE and Overleaf without corruption.
4. **Scenario 4: Concurrent Conflicting Edits (Our Version Authoritative)**:
   - Local FUSE author and remote collaborator edit the exact same conclusion sentence simultaneously with conflicting claims.
   - Verifies that our version is kept authoritative in FUSE, the remote conflicting edit is rejected, and the Overleaf server converges to our authoritative version.
5. **Scenario 5: Filesystem Structure Operations**:
   - Tests file creation (`create`), subfolder creation (`mkdir`), renaming (`rename`), deletion (`unlink` / `rmdir`), and reading binary assets (`frog.jpg`).
6. **Scenario 6: `.gitignore` Filtering and In-Memory Retention**:
   - Creates a `.gitignore` containing wildcards (`*.log`), directory patterns (`build/`), and whitelists (`!keep.log`).
   - Verifies that ignored files and directories exist and can be read/written locally in memory.
   - Queries the Overleaf server to verify that ignored files (`test.log`, `build/`) are NOT synchronized to Overleaf, while whitelisted (`keep.log`) and unignored files are synchronized.
   - Deletes ignored files locally without sending network deletion requests.

### Running Verification

```bash
# Run all unit tests (15 unit tests covering OT, gitignore, permissions, protocols)
cargo test --bin overleaf-mount

# Run full end-to-end scenario runner in Fast-Sync mode (Scenarios 1-6)
./target/release/overleaf-mount \
  --mount-point /tmp/test-fastsync \
  --exec "node tests/scenario_runner.mjs /tmp/test-fastsync"

# Run full end-to-end scenario runner in Fallback mode (Scenarios 1-6)
./target/release/overleaf-mount \
  --mount-point /tmp/test-fallback \
  --no-fast-sync \
  --exec "node tests/scenario_runner.mjs /tmp/test-fallback"
```
