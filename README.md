# PyShell – A Minimal Unix Shell in Python

PyShell is a custom Unix-like shell implemented in Python.
It supports **job control**, **foreground/background execution**, **signal handling**, and a **dynamic prompt** similar to `bash`.

This shell is designed to run on **Ubuntu / WSL (Windows Subsystem for Linux)**.

---

## ✨ Features

- Command execution using `fork()` and `execvp()`
- Background jobs using `&`
- Job control commands:
  - `jobs`
  - `fg <job_id>`
  - `bg <job_id>`
  - `kill <pid>`
- Proper handling of:
  - `Ctrl+C` (SIGINT)
  - `Ctrl+Z` (SIGTSTP)
- Process group and terminal control (`tcsetpgrp`)
- Dynamic shell prompt showing current directory:
  ```
  pyshell/current_folder>
  ```
- Built-in commands:
  - `cd` (supports `..`, quoted paths, and wildcards)
  - `pwd`
  - `exit`
  - `echo`
- Quoted paths and wildcard (`*`) support using `shlex` and `glob`
- Zombie-free child process cleanup via `SIGCHLD`

---

## ▶️ How to Run

Make sure you are using **Ubuntu / WSL** and Python 3.

```bash
python3 myshell1.py
```

You will see a prompt like:

```
pyshell/Project>
```

---

## 🧪 Example Usage

### Background Job
```bash
sleep 200 &
jobs
```

Output:
```
[1] RUNNING PID=12345 sleep 200 &
```

### Foreground / Stop / Resume
```bash
fg 1
Ctrl+Z
jobs
bg 1
fg 1
```

### Directory Navigation
```bash
cd test
cd ..
cd "Deliverable 1"
cd Deli*
```

---

## 🛠 Built-in Commands

| Command | Description |
|------|------------|
| `cd <dir>` | Change directory |
| `pwd` | Print current directory |
| `jobs` | List background/stopped jobs |
| `fg <job_id>` | Bring job to foreground |
| `bg <job_id>` | Resume job in background |
| `kill <pid>` | Terminate process |
| `exit` | Exit the shell |

---

## ⚙️ Implementation Highlights

- Uses **process groups** to ensure signals go to the correct job
- Shell ignores `SIGINT`, `SIGTSTP`, `SIGTTIN`, and `SIGTTOU`
- Child processes restore default signal behavior
- Terminal ownership is carefully restored after foreground execution
- Uses `shlex.split()` for correct parsing of quoted strings
- Uses `glob.glob()` for wildcard expansion in `cd`

---

## 🧠 Limitations

- No pipes (`|`) or I/O redirection (`>`, `<`) yet
- No command history or tab completion
- Designed for educational purposes, not production use

---

# Memory Management and Process Synchronization

This branch contains **memsync** of the Advanced Shell Simulation project.  
The goal of this deliverable is to simulate how an operating system manages **memory** using paging and page replacement algorithms, and how it handles **process synchronization** to prevent race conditions.

---

## Features Implemented

### 1. Memory Management (Paging)
- Fixed-size memory frames
- Page allocation and deallocation
- Page fault detection and tracking
- Memory usage tracking

### 2. Page Replacement Algorithms
- **FIFO (First-In-First-Out)**
- **LRU (Least Recently Used)**

The shell automatically triggers page replacement when memory is full.

### 3. Process Synchronization
- Producer–Consumer synchronization model
- Shared buffer protected using synchronization logic
- Prevents race conditions during concurrent access

---

## How to Run

Make sure you are using **Ubuntu (WSL)** or a Unix-like environment with Python 3 installed.

```bash
python3 memory_sync_shell.py
```

---

## Shell Commands

### Memory Management Commands

```
meminit <frames> <fifo|lru>   Initialize memory with given frames and algorithm
access <page>                Access a page (may cause page fault)
memstat                      Display total page faults
```

### Synchronization Commands

```
produce <item>               Produce an item into shared buffer
consume                      Consume an item from shared buffer
```

### General Commands

```
exit                         Exit the shell
```

---

## Example Usage

### FIFO Page Replacement

```text
memsync> meminit 3 fifo
memsync> access 1
memsync> access 2
memsync> access 3
memsync> access 4
Replacing (FIFO) page 1
```

### LRU Page Replacement

```text
memsync> meminit 3 lru
memsync> access 1
memsync> access 2
memsync> access 3
memsync> access 1
memsync> access 4
Replacing (LRU) page 2
```

### Producer–Consumer Synchronization

```text
memsync> produce A
Produced A | Buffer: ['A']
memsync> consume
Consumed A | Buffer: []
```

---

## Notes

- This repo builds on previous stages of the project.
- The implementation is a **simulation** of OS behavior, not a real kernel.