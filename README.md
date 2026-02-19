# Integrated Unix-Like Shell - Python

A custom Unix-like shell implemented in Python, simulating core operating system concepts across four deliverables: process management, scheduling, memory management, and security.

Designed to run on **Ubuntu / WSL (Windows Subsystem for Linux)**.

---

## How to Run

```bash
python3 final_shell.py
```

You will be prompted to log in:

```
Login required.
  username: root
  password: root
```

Default accounts:

| Username | Password  | Role  |
|----------|-----------|-------|
| root     | root      | admin |
| alice    | alice123  | user  |
| bob      | bob123    | user  |

---

## 1 - Basic Shell & Process Management

### Features

- Command execution using `fork()` / `exec()` / `wait()` via `subprocess` + `os.setsid()`
- Foreground and background execution (`cmd &`)
- I/O redirection (`>`, `>>`, `<`)
- Signal handling: `Ctrl+C` (SIGINT), `Ctrl+Z` (SIGTSTP), zombie cleanup via `SIGCHLD`
- Dynamic prompt showing current user and directory:
  ```
  root:~/project# 
  alice:~/project$ 
  ```
- Command history (persisted to `~/.ishell_history`)
- Aliases

### Built-in Commands

| Command | Description |
|---------|-------------|
| `cd [dir]` | Change directory (`-` returns to previous) |
| `pwd` | Print working directory |
| `exit [code]` | Exit the shell |
| `echo [text]` | Print text, supports `$VAR` expansion |
| `clear` | Clear the terminal |
| `ls [-la]` | List directory contents |
| `cat [file]` | Display file contents |
| `mkdir [-p] [dir]` | Create directory |
| `rmdir [dir]` | Remove empty directory |
| `rm [-rf] [file]` | Remove file or directory |
| `touch [file]` | Create file / update timestamp |
| `kill [-9] [pid]` | Send signal to process |
| `grep [-i] [-v] [-n] <pattern> [file]` | Search text |
| `sort [-r] [-n] [file]` | Sort lines |
| `sleep [n]` | Sleep for n seconds |
| `history [n]` | Show command history |
| `alias key=val` | Define alias |
| `ps` | Show background processes |

### Job Control

| Command | Description |
|---------|-------------|
| `jobs` | List all background/stopped jobs |
| `fg [id]` | Bring job to foreground (sends SIGCONT, then waits) |
| `bg [id]` | Resume stopped job in background (sends SIGCONT) |

### Example Usage

```bash
sleep 60 &          # start background job
jobs                # list jobs
fg 1                # bring job 1 to foreground
# Ctrl+Z to stop
bg 1                # resume in background

ls | grep .py       # piping
echo hello > out.txt
cat out.txt >> log.txt
```

---

## 2 - Process Scheduling

Simulates two CPU scheduling algorithms with `time.sleep()` per tick to represent real execution time.

### Algorithms

**Round-Robin (RR)**
- Each process gets a configurable time quantum
- After the quantum expires, the process is re-queued
- If a process finishes before its quantum, it exits immediately

**Priority-Based (Preemptive)**
- Uses a **min-heap** (`heapq`) so the highest-priority process always runs first
- Lower priority number = higher priority
- **FCFS tiebreaking**: equal-priority processes run in arrival order
- **Preemption**: if a higher-priority process becomes available mid-execution, it immediately takes over

### Commands

| Command | Description |
|---------|-------------|
| `sched add <name> <burst> [priority]` | Add process to queue (default priority=5) |
| `sched run [rr\|priority] [quantum]` | Run scheduler (default: RR, quantum=2) |
| `sched list` | Show queued processes |
| `sched clear` | Clear queue |

### Example Usage

```bash
sched add P1 6 2
sched add P2 4 1
sched add P3 3 3
sched run rr 2
```

Output includes:
- Tick-by-tick execution log with `time.sleep()` simulation
- Gantt chart
- Average wait time and turnaround time

```
  [RR] quantum=2 tick=0.05s (3 processes)
  t=  0  Running P2     remaining=4→2
  t=  2  Running P1     remaining=6→4
  ...
  Gantt Chart:
  | P2 | P1 | P3 | P2 | P1 | P1 |
  0    2    4    6    8    10   12
  Avg Wait: 3.67  Avg Turnaround: 8.33
```

---

## 3 - Memory Management & Process Synchronization

### Memory Management (Paging)

Simulates OS-level paging with virtual-to-physical address translation.

- Fixed-size page frames (`PAGE_SIZE = 256 bytes`, `NUM_FRAMES = 8`)
- Per-process page tables mapping virtual pages → physical frames
- Page fault detection and logging
- Two page replacement algorithms: **FIFO** and **LRU**
- Memory overflow simulation showing replacement in action

### Memory Commands

| Command | Description |
|---------|-------------|
| `mem status` | Show frame usage, faults, hits, per-process breakdown |
| `mem alloc <pid> <bytes>` | Allocate memory for a process (shows page faults live) |
| `mem free <pid>` | Free all pages for a process |
| `mem translate <pid> <vaddr>` | Translate virtual → physical address |
| `mem algo [fifo\|lru]` | Switch page replacement algorithm |
| `mem overflow` | Simulate filling memory and triggering page replacement |
| `mem faultlog` | Show full history of all page fault events |

### Example Usage

```bash
mem alloc 101 512      # allocates 2 pages, shows page faults
mem alloc 102 1024     # allocates 4 pages
mem status             # per-process breakdown + frame bar
mem translate 101 300  # virtual 300 → physical address
mem translate 101 999  # PAGE FAULT: not mapped
mem algo fifo          # switch to FIFO replacement
mem overflow           # fill memory, watch evictions happen
mem faultlog           # full fault history
```

Example `mem status` output:
```
  Algorithm   : LRU
  Frames      : 6/8 used  [██████░░] 75%
  Page Faults : 4   Hits: 2
  Frame state : [0, 2, 4, 6]
  PID    101  : 2 pages (512 bytes)
  PID    102  : 4 pages (1024 bytes)
```

### Process Synchronization

**Producer-Consumer Problem**

Demonstrates bounded-buffer synchronization using:
- A **mutex** (`threading.Lock`) to protect the shared buffer
- Two **semaphores** (`threading.Semaphore`): `empty` (free slots) and `full` (available items)

The output explicitly shows each `waiting on semaphore` and `acquiring mutex` step to make the synchronization mechanism visible.

**Race Condition Demonstration**

`pcsync race` runs the same shared counter increment two ways:
- **Without mutex**: two threads race → lost updates (e.g. expected 1000, got 673)
- **With mutex**: two threads sync correctly → always exactly 1000

### Synchronization Commands

| Command | Description |
|---------|-------------|
| `pcsync [producers] [consumers] [items]` | Run producer-consumer demo |
| `pcsync race [n]` | Demonstrate race condition with/without mutex |

### Example Usage

```bash
pcsync 1 1 4
```
```
  Producer-0  waiting on empty  (slots=5)
  Producer-0  PRODUCED  item-0-0     buffer=['item-0-0']
  Consumer-0  waiting on full   (items=1)
  Consumer-0  CONSUMED  item-0-0     buffer=[]
  ...
```

```bash
pcsync race 500
```
```
  WITHOUT mutex: expected=1000  actual=743  lost=257 updates (race!)
  WITH    mutex: expected=1000  actual=1000  correct! (race prevented)
```

---

## 4 - Integration, Piping & Security

### Piping

Chains multiple commands, passing stdout of each as stdin to the next.

```bash
ls | grep .py
cat file.txt | grep error | sort
cat log.txt | grep WARNING | sort -r
```

Supports chains of any length. Each segment is a separate process connected by pipes.

### User Authentication

- Users must log in before accessing the shell
- Passwords stored as **SHA-256 hashes** in `~/.ishell_users.json`
- Account lockout after 3 failed attempts
- Two roles: `root` (full access) and `user` (restricted)

### Authentication Commands

| Command | Description |
|---------|-------------|
| `login` | Log in as a different user |
| `logout` | Log out (returns to login prompt) |
| `whoami` | Show current user and role |
| `passwd [user]` | Change password |
| `adduser <user> [pass] [role]` | Create new user (root only) |
| `users` | List all users (root only) |

### File Permissions

Simulates OS-style file permission enforcement. Standard users are restricted from writing to system paths.

Simulated protected paths:

| Path | root | user |
|------|------|------|
| `/etc/passwd` | `rwx` | `r--` |
| `/etc/shadow` | `rwx` | `---` |
| `/etc/hosts`  | `rwx` | `r--` |
| `/var/log`    | `rwx` | `r--` |
| `/bin`, `/usr`| `rwx` | `r-x` |

For all other paths, real OS permission bits are checked.

### Permission Commands

| Command | Description |
|---------|-------------|
| `perms check <path> [r\|w\|x]` | Check if current user has permission |
| `perms show <path>` | Show permission bits for files in a directory |

### Example Usage

```bash
# As root
perms check /etc/shadow w    # ✓ root — full access

# As alice (standard user)
perms check /etc/shadow r    # ✗ permission denied — standard users have '---'
perms check /etc/passwd r    # ✓ allowed (simulated: r--)
perms check /etc/passwd w    # ✗ permission denied — standard users have 'r--'
```

---

## Implementation Notes

- All scheduling, memory, and synchronization features are **simulations** for educational purposes, not real kernel operations
- `subprocess.Popen` with `os.setsid()` is used to simulate `fork()`/`exec()` with proper process group isolation
- `threading.Lock` and `threading.Semaphore` provide real synchronization primitives (not simulated)
- The race condition demo uses `time.sleep(0)` to force thread context switches and reliably reproduce the race
- History is persisted to `~/.ishell_history` across sessions
- User credentials are persisted to `~/.ishell_users.json`