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

## 📜 License

This project is for educational use only.