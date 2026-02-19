#!/usr/bin/env python3
"""
Integrated Unix-Like Shell — All 4 Deliverables
  D1: Built-ins, foreground/background jobs, fg/bg/kill
  D2: Round-Robin & Priority scheduling (heap, FCFS tiebreak, preemption, timers)
  D3: Paging FIFO/LRU, page faults, overflow simulation, producer-consumer, race demo
  D4: Piping, user authentication, file permissions
Run: python3 shell.py
"""
import os, sys, shlex, subprocess, signal, time, threading, heapq
import hashlib, json, stat, math, collections
from datetime import datetime
from pathlib import Path

RED="\033[31m"; GREEN="\033[32m"; YELLOW="\033[33m"; BLUE="\033[34m"
CYAN="\033[36m"; BOLD="\033[1m"; RESET="\033[0m"; BGREEN="\033[1;32m"
def clr(t, c): return c + t + RESET

# =============================================================================
# MEMORY MANAGEMENT
# =============================================================================
PAGE_SIZE = 256
NUM_FRAMES = 8

class FIFO:
    def __init__(self): self.frames, self.faults, self.hits = [], 0, 0
    def access(self, page):
        if page in self.frames: self.hits += 1; return False
        self.faults += 1
        if len(self.frames) >= NUM_FRAMES: self.frames.pop(0)
        self.frames.append(page); return True

class LRU:
    def __init__(self): self.frames = collections.OrderedDict(); self.faults = self.hits = 0
    def access(self, page):
        if page in self.frames: self.frames.move_to_end(page); self.hits += 1; return False
        self.faults += 1
        if len(self.frames) >= NUM_FRAMES: self.frames.popitem(last=False)
        self.frames[page] = True; return True

class MemoryManager:
    """Paging simulation: virtual→physical translation, FIFO/LRU page replacement."""
    def __init__(self, algo="LRU"):
        self.algo        = algo.upper()
        self.page_tables = {}          # pid → {vpage: frame}
        self.free_frames = list(range(NUM_FRAMES))
        self.frame_map   = {}          # frame → (pid, vpage)
        self.replacer    = LRU() if self.algo == "LRU" else FIFO()
        self.lock        = threading.Lock()
        self.fault_log   = []

    def allocate(self, pid, size_bytes, verbose=False):
        pages = math.ceil(size_bytes / PAGE_SIZE)
        with self.lock:
            self.page_tables.setdefault(pid, {})
            table = self.page_tables[pid]
            for i in range(pages):
                vpage    = len(table) + i
                is_fault = self.replacer.access(vpage)
                evicted  = None
                if self.free_frames:
                    frame = self.free_frames.pop(0)
                else:
                    vp    = self.replacer.frames[0] if self.algo == "FIFO" else next(iter(self.replacer.frames))
                    frame = next((f for f, (p, v) in self.frame_map.items() if v == vp), 0)
                    op, ov = self.frame_map.get(frame, (None, None))
                    evicted = (op, ov)
                    if op and op in self.page_tables: self.page_tables[op].pop(ov, None)
                table[vpage] = frame
                self.frame_map[frame] = (pid, vpage)
                if is_fault:
                    ev  = f"  [evicted pid={evicted[0]} vpage={evicted[1]}]" if evicted else "  [free frame]"
                    msg = f"PAGE FAULT  pid={pid} vpage={vpage} → frame={frame}{ev}"
                    self.fault_log.append(msg)
                    if verbose: print(clr(f"  {msg}", RED))
        return pages

    def free(self, pid):
        with self.lock:
            if pid not in self.page_tables: return 0
            freed = len(self.page_tables[pid])
            for _, frame in self.page_tables[pid].items():
                self.free_frames.append(frame); self.frame_map.pop(frame, None)
            del self.page_tables[pid]
        return freed

    def translate(self, pid, vaddr):
        vpage, offset = vaddr // PAGE_SIZE, vaddr % PAGE_SIZE
        with self.lock:
            table = self.page_tables.get(pid, {})
            if vpage not in table:
                self.fault_log.append(f"PAGE FAULT  pid={pid} vaddr={vaddr} (vpage={vpage} not mapped)")
                return None, True
            return table[vpage] * PAGE_SIZE + offset, False

    def status(self):
        used = NUM_FRAMES - len(self.free_frames)
        frames = list(self.replacer.frames) if self.algo == "FIFO" else list(self.replacer.frames.keys())
        per_proc = {pid: {"pages": len(t), "bytes": len(t)*PAGE_SIZE} for pid, t in self.page_tables.items()}
        return {"algo": self.algo, "total": NUM_FRAMES, "used": used, "free": len(self.free_frames),
                "faults": self.replacer.faults, "hits": self.replacer.hits,
                "frames": frames, "per_proc": per_proc}

# =============================================================================
# PROCESS SYNCHRONIZATION
# =============================================================================
class ProducerConsumer:
    """Bounded buffer with mutex + semaphores. Shows acquire/release explicitly."""
    def __init__(self, buf_size=5, items=4):
        self.buffer = []; self.buf_size = buf_size; self.items = items
        self.mutex  = threading.Lock()
        self.empty  = threading.Semaphore(buf_size)
        self.full   = threading.Semaphore(0)
        self.log    = []

    def producer(self, tid):
        for i in range(self.items):
            item = f"item-{tid}-{i}"
            self.log.append(f"  Producer-{tid}  waiting on empty  (slots={self.empty._value})")
            self.empty.acquire()
            with self.mutex:
                self.buffer.append(item)
                self.log.append(f"  Producer-{tid}  PRODUCED  {item:<12} buffer={self.buffer[:]}")
            self.full.release(); time.sleep(0.03)

    def consumer(self, tid):
        for _ in range(self.items):
            self.log.append(f"  Consumer-{tid}  waiting on full   (items={self.full._value})")
            self.full.acquire()
            with self.mutex:
                item = self.buffer.pop(0) if self.buffer else "none"
                self.log.append(f"  Consumer-{tid}  CONSUMED  {item:<12} buffer={self.buffer[:]}")
            self.empty.release(); time.sleep(0.05)

    def run(self, np=1, nc=1):
        threads = ([threading.Thread(target=self.producer, args=(i,)) for i in range(np)] +
                   [threading.Thread(target=self.consumer, args=(i,)) for i in range(nc)])
        for t in threads: t.start()
        for t in threads: t.join(timeout=15)
        return self.log

class RaceConditionDemo:
    """Shows counter increment with/without mutex to demonstrate race conditions."""
    def __init__(self): self.counter = 0; self.mutex = threading.Lock()
    def _unsafe(self, n):
        for _ in range(n): tmp = self.counter; time.sleep(0); self.counter = tmp + 1
    def _safe(self, n):
        for _ in range(n):
            with self.mutex: self.counter += 1
    def run_unsafe(self, n):
        self.counter = 0
        t1, t2 = threading.Thread(target=self._unsafe, args=(n,)), threading.Thread(target=self._unsafe, args=(n,))
        t1.start(); t2.start(); t1.join(); t2.join()
        return self.counter
    def run_safe(self, n):
        self.counter = 0
        t1, t2 = threading.Thread(target=self._safe, args=(n,)), threading.Thread(target=self._safe, args=(n,))
        t1.start(); t2.start(); t1.join(); t2.join()
        return self.counter

# =============================================================================
# PROCESS SCHEDULING
# =============================================================================
class Process:
    """Scheduler process. __lt__ enables min-heap with FCFS tiebreaking."""
    _id = _order = 1
    def __init__(self, name, burst, priority=5):
        self.pid = Process._id; Process._id += 1
        self.arrival_order = Process._order; Process._order += 1
        self.name = name; self.burst = burst; self.remaining = burst
        self.priority = priority; self.start = self.finish = None
        self.wait = 0; self.state = "READY"
    def __lt__(self, o):
        return self.arrival_order < o.arrival_order if self.priority == o.priority else self.priority < o.priority
    def __repr__(self):
        return f"[pid={self.pid} name={self.name} burst={self.burst} priority={self.priority} remaining={self.remaining} state={self.state}]"

class Scheduler:
    """Round-Robin (configurable quantum) and Priority (preemptive min-heap, FCFS tiebreak)."""
    TICK = 0.05
    def __init__(self, algo="RR", quantum=2):
        self.algo = algo.upper(); self.quantum = quantum; self.queue = []; self.gantt = []

    def add(self, p): self.queue.append(p)

    def reset(self):
        for p in self.queue: p.remaining = p.burst; p.state = "READY"; p.start = p.finish = None; p.wait = 0

    def run(self):
        self.reset(); self.gantt = []
        return self._run_rr() if self.algo == "RR" else self._run_priority()

    def _run_rr(self):
        q = collections.deque(self.queue); tick = 0
        print(clr(f"\n  [RR] quantum={self.quantum} tick={self.TICK}s ({len(self.queue)} processes)", CYAN))
        while q:
            p = q.popleft()
            if p.state == "DONE": continue
            if p.start is None: p.start = tick
            p.state = "RUNNING"
            run_for = min(self.quantum, p.remaining); s = tick
            print(clr(f"  t={tick:>3}  Running {p.name:<6} remaining={p.remaining}→{p.remaining-run_for}", GREEN))
            for _ in range(run_for):
                time.sleep(self.TICK); tick += 1; p.remaining -= 1
                if p.remaining == 0: break
            self.gantt.append((p.name, s, tick))
            if p.remaining <= 0:
                p.state = "DONE"; p.finish = tick; p.wait = tick - p.burst
                print(clr(f"  t={tick:>3}  {p.name} DONE  wait={p.wait} tat={p.finish}", YELLOW))
            else:
                p.state = "READY"; q.append(p)
        return self.gantt

    def _run_priority(self):
        heap = []; [heapq.heappush(heap, p) for p in self.queue]
        tick = 0; current = None
        print(clr(f"\n  [PRIORITY] preemptive heap tick={self.TICK}s ({len(self.queue)} processes)", CYAN))
        while heap or (current and current.remaining > 0):
            if heap:
                if current and current.remaining > 0 and heap[0].priority < current.priority:
                    print(clr(f"  t={tick:>3}  PREEMPT {current.name}(pri={current.priority}) → {heap[0].name}(pri={heap[0].priority})", RED))
                    heapq.heappush(heap, current); current = heapq.heappop(heap)
                elif not current or current.remaining == 0:
                    current = heapq.heappop(heap)
            elif not current or current.remaining == 0: break
            if current.start is None:
                current.start = tick; current.state = "RUNNING"
                print(clr(f"  t={tick:>3}  Running {current.name:<6} priority={current.priority} remaining={current.remaining}", GREEN))
            time.sleep(self.TICK); tick += 1; current.remaining -= 1
            if current.remaining == 0:
                current.state = "DONE"; current.finish = tick; current.wait = current.finish - current.burst
                self.gantt.append((current.name, current.start, tick))
                print(clr(f"  t={tick:>3}  {current.name} DONE  wait={current.wait} tat={current.finish}", YELLOW))
                current = None
        return self.gantt

    def stats(self):
        done = [p for p in self.queue if p.state == "DONE"]
        if not done: return {}
        return {"algo": self.algo, "quantum": self.quantum if self.algo=="RR" else "N/A",
                "count": len(done), "avg_wait": round(sum(p.wait for p in done)/len(done), 2),
                "avg_tat": round(sum(p.finish for p in done)/len(done), 2)}

    def print_gantt(self):
        if not self.gantt: return
        bar = header = ""
        for name, s, e in self.gantt:
            w = max((e-s)*2, len(name)+2); bar += f"|{name:^{w}}"; header += f"{s:<{w+1}}"
        print(clr("  " + bar + "|", CYAN)); print("  " + header + str(self.gantt[-1][2]))

# =============================================================================
# JOB TRACKER
# =============================================================================
class JobTracker:
    def __init__(self): self.jobs = {}; self._nid = 1

    def add(self, pid, cmd, proc=None):
        jid = self._nid; self._nid += 1
        self.jobs[jid] = {"pid": pid, "cmd": cmd, "status": "running", "proc": proc, "start": datetime.now()}
        return jid

    def update(self):
        for j in self.jobs.values():
            if j["status"] in ("done", "terminated"): continue
            proc = j.get("proc")
            if proc:
                ret = proc.poll()
                if ret is not None: j["status"] = "done" if ret == 0 else f"exit {ret}"
            else:
                try: os.kill(j["pid"], 0)
                except ProcessLookupError: j["status"] = "done"

    def list_jobs(self):
        self.update()
        if not self.jobs: return "No background jobs."
        return "\n".join(f"[{jid}] pid={j['pid']}  {j['status']:<12}  "
                         f"age={str(datetime.now()-j['start']).split('.')[0]}  {j['cmd']}"
                         for jid, j in self.jobs.items())

    def fg(self, jid):
        j = self.jobs.get(jid)
        if not j: return False, f"fg: no such job [{jid}]"
        if j["status"] == "done": return False, f"fg: job [{jid}] already finished"
        try:
            os.kill(j["pid"], signal.SIGCONT); j["status"] = "running"
            print(clr(f"  [{jid}] {j['cmd']}", CYAN))
            (j["proc"].wait() if j.get("proc") else os.waitpid(j["pid"], 0))
            j["status"] = "done"; return True, ""
        except (ChildProcessError, ProcessLookupError): j["status"] = "done"; return True, ""
        except Exception as e: return False, str(e)

    def bg(self, jid):
        j = self.jobs.get(jid)
        if not j: return False, f"bg: no such job [{jid}]"
        if j["status"] == "done": return False, f"bg: job [{jid}] already finished"
        try:
            os.kill(j["pid"], signal.SIGCONT); j["status"] = "running"
            return True, f"[{jid}] {j['cmd']} &"
        except ProcessLookupError: j["status"] = "done"; return False, f"bg: process {j['pid']} not found"
        except Exception as e: return False, str(e)

# =============================================================================
# SECURITY
# =============================================================================
USERS_FILE = os.path.expanduser("~/.ishell_users.json")

class SecurityManager:
    """SHA-256 hashed passwords, role-based access (root/user), simulated file permissions."""
    PROTECTED = {
        "/etc/passwd": {"root": "rwx", "user": "r--"},
        "/etc/shadow": {"root": "rwx", "user": "---"},
        "/etc/hosts":  {"root": "rwx", "user": "r--"},
        "/var/log":    {"root": "rwx", "user": "r--"},
        "/bin":        {"root": "rwx", "user": "r-x"},
        "/usr":        {"root": "rwx", "user": "r-x"},
    }
    def __init__(self):
        self.users = {}; self.current_user = None; self.attempts = {}; self._load()

    def _load(self):
        if os.path.exists(USERS_FILE):
            try:
                with open(USERS_FILE) as f: self.users = json.load(f)
            except Exception: pass
        if not self.users:
            self.users = {"root":  {"pw": self._hash("root"),     "role": "root"},
                          "alice": {"pw": self._hash("alice123"), "role": "user"},
                          "bob":   {"pw": self._hash("bob123"),   "role": "user"}}
            self._save()

    def _save(self):
        with open(USERS_FILE, "w") as f: json.dump(self.users, f, indent=2)

    def _hash(self, pw): return hashlib.sha256(pw.encode()).hexdigest()

    def login(self, u, pw):
        if self.attempts.get(u, 0) >= 3: return False, "Account locked after 3 failed attempts."
        if u not in self.users or self.users[u]["pw"] != self._hash(pw):
            self.attempts[u] = self.attempts.get(u, 0) + 1
            return False, f"Invalid credentials. {3 - self.attempts[u]} attempt(s) left."
        self.attempts[u] = 0; self.current_user = u
        return True, f"Welcome, {u}!"

    def logout(self): u = self.current_user; self.current_user = None; return f"Logged out {u}."
    def is_root(self): return self.current_user and self.users[self.current_user]["role"] == "root"
    def whoami(self):
        if not self.current_user: return "Not logged in."
        return f"{self.current_user}  role={self.users[self.current_user]['role']}"

    def add_user(self, u, pw, role="user"):
        if not self.is_root(): return False, "Permission denied: root only."
        if u in self.users: return False, f"User '{u}' already exists."
        self.users[u] = {"pw": self._hash(pw), "role": role}; self._save()
        return True, f"User '{u}' created."

    def change_password(self, u, old, new):
        if u != self.current_user and not self.is_root(): return False, "Permission denied."
        if not self.is_root() and self.users[u]["pw"] != self._hash(old): return False, "Wrong password."
        self.users[u]["pw"] = self._hash(new); self._save(); return True, "Password changed."

    def check_permission(self, path, mode="r"):
        if not self.current_user: return False, "Not logged in."
        if self.is_root(): return True, "root — full access"
        norm = os.path.normpath(path)
        for protected, perms in self.PROTECTED.items():
            if norm == protected or norm.startswith(protected + os.sep):
                allowed = perms.get("user", "---")
                idx = {"r": 0, "w": 1, "x": 2}.get(mode, 0)
                if allowed[idx] == mode: return True, f"allowed (simulated: {allowed})"
                return False, f"permission denied — standard users have '{allowed}' on {protected}"
        if not os.path.exists(path): return False, "file not found"
        ok = bool(os.stat(path).st_mode & {"r": stat.S_IROTH, "w": stat.S_IWOTH, "x": stat.S_IXOTH}.get(mode, stat.S_IROTH))
        return ok, "allowed" if ok else "permission denied"

# =============================================================================
# MAIN SHELL
# =============================================================================
class Shell:
    HIST_FILE = os.path.expanduser("~/.ishell_history")

    def __init__(self):
        self.jobs = JobTracker(); self.scheduler = Scheduler()
        self.memory = MemoryManager(); self.security = SecurityManager()
        self.history = []; self.aliases = {}; self.running = True
        self._load_history()
        signal.signal(signal.SIGINT,  lambda s, f: print())
        signal.signal(signal.SIGCHLD, self._reap)

    def _reap(self, s, f):
        try:
            while True:
                pid, _ = os.waitpid(-1, os.WNOHANG)
                if pid == 0: break
        except ChildProcessError: pass

    def _load_history(self):
        if os.path.exists(self.HIST_FILE):
            with open(self.HIST_FILE) as f: self.history = [l.rstrip() for l in f if l.strip()]

    def _save_history(self):
        with open(self.HIST_FILE, "a") as f: [f.write(c+"\n") for c in self.history[-100:]]

    def _prompt(self):
        user = self.security.current_user or "guest"
        role = self.security.users.get(user, {}).get("role", "user") if self.security.current_user else "user"
        cwd  = os.getcwd().replace(os.path.expanduser("~"), "~")
        return f"{clr(user, BGREEN)}:{clr(cwd, BLUE)}{clr('#' if role=='root' else '$', RED if role=='root' else GREEN)} "

    def _login_prompt(self):
        print(clr("Login required.", YELLOW))
        for _ in range(3):
            try: u = input("  username: ").strip(); p = input("  password: ").strip()
            except (EOFError, KeyboardInterrupt): sys.exit(0)
            ok, msg = self.security.login(u, p)
            print(clr("  "+msg, GREEN if ok else RED))
            if ok: return
        print(clr("Too many failed attempts.", RED)); sys.exit(1)

    def run(self):
        print(clr("Integrated Shell v4.0  —  type 'help' for commands", CYAN))
        self._login_prompt()
        while self.running:
            try: line = input(self._prompt()).strip()
            except (EOFError, KeyboardInterrupt): print(); break
            if not line: continue
            parts = line.split()
            if parts[0] in self.aliases: line = self.aliases[parts[0]] + " " + " ".join(parts[1:])
            self.history.append(line); self._dispatch(line)
        self._save_history(); print(clr("Goodbye.", YELLOW))

    def _dispatch(self, line):
        if "|" in line: self._pipeline(line); return
        bg = line.endswith("&")
        if bg: line = line[:-1].strip()
        try: tokens = shlex.split(line)
        except ValueError as e: print(clr(f"parse error: {e}", RED)); return
        if not tokens: return
        cmd, args = tokens[0], tokens[1:]
        # I/O redirection
        si = so = None; app = False; clean = []; i = 0
        while i < len(args):
            if   args[i]=="<"  and i+1<len(args): si  = args[i+1]; i+=2
            elif args[i]==">>" and i+1<len(args): so  = args[i+1]; app=True; i+=2
            elif args[i]==">"  and i+1<len(args): so  = args[i+1]; i+=2
            else: clean.append(args[i]); i+=1
        args = clean
        h = {
            "cd":      self._cd,           "pwd":     lambda a: os.getcwd(),
            "exit":    self._exit,         "echo":    self._echo,
            "clear":   lambda a: os.system("clear") and None,
            "ls":      self._ls,           "cat":     self._cat,
            "mkdir":   self._mkdir,        "rmdir":   self._rmdir,
            "rm":      self._rm,           "touch":   self._touch,
            "kill":    self._kill,         "jobs":    lambda a: self.jobs.list_jobs(),
            "fg":      self._fg,           "bg":      self._bg,
            "ps":      self._ps,           "history": self._history,
            "alias":   self._alias,        "unalias": lambda a: [self.aliases.pop(x,None) for x in a] and None,
            "sleep":   lambda a: time.sleep(float(a[0])) if a else None,
            "grep":    self._grep,         "sort":    self._sort,
            "help":    self._help,
            "sched":   self._sched,
            "mem":     self._mem,          "pcsync":  self._pcsync,
            "login":   self._login,        "logout":  self._logout,
            "whoami":  lambda a: self.security.whoami(),
            "passwd":  self._passwd,       "adduser": self._adduser,
            "users":   self._users,        "perms":   self._perms,
        }
        if cmd in h:
            try:
                result = h[cmd](args)
                if result is not None:
                    if so:
                        with open(so, "a" if app else "w") as f: f.write(result+"\n")
                    else: print(result)
            except Exception as e: print(clr(f"{cmd}: {e}", RED))
        else: self._exec(tokens, bg, si, so, app)

    # Pipeline
    def _pipeline(self, line):
        segments = [s.strip() for s in line.split("|")]
        procs = []; prev = None
        for i, seg in enumerate(segments):
            try: tokens = shlex.split(seg)
            except ValueError: print(clr(f"parse error: {seg}", RED)); return
            if not tokens: continue
            try:
                p = subprocess.Popen(tokens, stdin=prev,
                                     stdout=None if i==len(segments)-1 else subprocess.PIPE,
                                     stderr=subprocess.PIPE)
                procs.append(p)
                if prev: prev.close()
                prev = p.stdout
            except FileNotFoundError: print(clr(f"{tokens[0]}: not found", RED)); return
        for p in procs:
            try:
                out, err = p.communicate(timeout=30)
                if out: sys.stdout.write(out.decode(errors="replace"))
                if err: sys.stderr.write(clr(err.decode(errors="replace"), RED))
            except subprocess.TimeoutExpired: p.kill(); print(clr("pipeline timeout", RED))

    # External exec (fork/exec/wait)
    def _exec(self, tokens, bg=False, si=None, so=None, app=False):
        sf = open(si) if si else None
        of = open(so, "a" if app else "w") if so else None
        try:
            proc = subprocess.Popen(tokens, stdin=sf or sys.stdin, stdout=of or sys.stdout,
                                    stderr=sys.stderr, preexec_fn=os.setsid)
        except FileNotFoundError: print(clr(f"{tokens[0]}: command not found", RED)); return
        except PermissionError:   print(clr(f"{tokens[0]}: permission denied", RED)); return
        finally:
            if sf: sf.close()
            if of: of.close()
        if bg:
            jid = self.jobs.add(proc.pid, " ".join(tokens), proc)
            self.memory.allocate(proc.pid, 512)
            print(clr(f"[{jid}] {proc.pid}", YELLOW))
        else:
            try: proc.wait()
            except KeyboardInterrupt: proc.terminate(); print()
            try:
                _, ws = os.waitpid(proc.pid, os.WNOHANG)
                if os.WIFSTOPPED(ws):
                    jid = self.jobs.add(proc.pid, " ".join(tokens), proc)
                    self.jobs.jobs[jid]["status"] = "stopped"
                    print(clr(f"\n[{jid}]+ Stopped  {' '.join(tokens)}", YELLOW))
            except Exception: pass

    # =========================================================================
    # BUILT-INS
    # =========================================================================
    def _cd(self, a):
        t = os.path.expanduser(a[0]) if a else os.path.expanduser("~")
        if a and a[0] == "-": t = os.environ.get("OLDPWD", os.getcwd())
        os.environ["OLDPWD"] = os.getcwd(); os.chdir(t)

    def _exit(self, a): self._save_history(); self.running = False; raise SystemExit(int(a[0]) if a else 0)

    def _echo(self, a):
        import re
        return re.sub(r'\$(\w+)', lambda m: os.environ.get(m.group(1), ""), " ".join(x for x in a if x != "-n"))

    def _ls(self, a):
        long_ = any(f in a for f in ("-l","-la","-al"))
        all_  = any(f in a for f in ("-a","-la","-al"))
        paths = [x for x in a if not x.startswith("-")] or ["."]
        out   = []
        for path in paths:
            try:
                entries = sorted(os.listdir(path))
                if not all_: entries = [e for e in entries if not e.startswith(".")]
                if long_:
                    for e in entries:
                        s = os.stat(os.path.join(path, e))
                        out.append(f"{stat.filemode(s.st_mode)}  {s.st_nlink}  {s.st_size:>8}  "
                                   f"{datetime.fromtimestamp(s.st_mtime).strftime('%b %d %H:%M')}  {e}")
                else:
                    out.append("  ".join(clr(e+"/", BLUE) if os.path.isdir(os.path.join(path, e)) else e for e in entries))
            except Exception as e: out.append(clr(str(e), RED))
        return "\n".join(out)

    def _cat(self, a):
        if not a: return clr("cat: missing operand", RED)
        out = []
        for f in a:
            try: out.append(open(f).read())
            except Exception as e: out.append(clr(str(e), RED))
        return "".join(out).rstrip("\n")

    def _mkdir(self, a):
        p = "-p" in a
        for d in [x for x in a if not x.startswith("-")]:
            try: os.makedirs(d) if p else os.mkdir(d)
            except Exception as e: print(clr(f"mkdir: {e}", RED))

    def _rmdir(self, a):
        for d in a:
            try: os.rmdir(d)
            except Exception as e: print(clr(f"rmdir: {e}", RED))

    def _rm(self, a):
        import shutil
        rec = any(f in a for f in ("-r","-rf","-fr")); frc = any(f in a for f in ("-f","-rf","-fr"))
        for t in [x for x in a if not x.startswith("-")]:
            try: shutil.rmtree(t) if (os.path.isdir(t) and rec) else os.remove(t)
            except Exception as e:
                if not frc: print(clr(f"rm: {e}", RED))

    def _touch(self, a):
        for f in a:
            try: Path(f).touch()
            except Exception as e: print(clr(f"touch: {e}", RED))

    def _kill(self, a):
        sig = signal.SIGTERM
        for x in a:
            if x == "-9": sig = signal.SIGKILL; continue
            try: os.kill(int(x), sig); print(f"Sent signal {sig} to {x}")
            except Exception as e: print(clr(f"kill: {e}", RED))

    def _history(self, a):
        n = int(a[0]) if a and a[0].isdigit() else len(self.history)
        return "\n".join(f"  {i+1:4d}  {c}" for i, c in enumerate(self.history[-n:]))

    def _alias(self, a):
        if not a: return "\n".join(f"  {k}='{v}'" for k, v in self.aliases.items()) or "No aliases."
        for x in a:
            if "=" in x: k, v = x.split("=", 1); self.aliases[k] = v.strip("'\"")

    def _ps(self, a):
        self.jobs.update()
        if not self.jobs.jobs: return "No background processes."
        lines = [f"  {'JID':>4}  {'PID':>7}  {'STATUS':<12}  COMMAND"]
        for jid, j in self.jobs.jobs.items(): lines.append(f"  [{jid:>2}]  {j['pid']:>7}  {j['status']:<12}  {j['cmd']}")
        return "\n".join(lines)

    def _fg(self, a):
        jid = int(a[0].lstrip("%")) if a else (max(self.jobs.jobs) if self.jobs.jobs else None)
        if jid is None: return clr("fg: no current job", RED)
        ok, msg = self.jobs.fg(jid)
        if not ok: return clr(msg, RED)

    def _bg(self, a):
        if a:
            try: jid = int(a[0].lstrip("%"))
            except ValueError: return clr(f"bg: invalid job id: {a[0]}", RED)
        else:
            stopped = [jid for jid, j in self.jobs.jobs.items() if j["status"] == "stopped"]
            if not stopped: return clr("bg: no stopped jobs", RED)
            jid = max(stopped)
        ok, msg = self.jobs.bg(jid)
        return clr(msg, GREEN if ok else RED)

    def _grep(self, a):
        import re
        if not a: return clr("Usage: grep [-i] [-v] [-n] <pattern> [file...]", YELLOW)
        ig = "-i" in a; inv = "-v" in a; num = "-n" in a
        rest = [x for x in a if not x.startswith("-")]
        if not rest: return clr("grep: missing pattern", RED)
        pat, files = rest[0], rest[1:]
        flags = re.IGNORECASE if ig else 0; out = []
        def search(lines, fname=""):
            for i, line in enumerate(lines, 1):
                m = bool(re.search(pat, line, flags))
                if inv: m = not m
                if m: out.append((f"{fname}:" if fname else "") + (f"{i}:" if num else "") + line.rstrip())
        if files:
            for f in files:
                try:
                    with open(f) as fh: search(fh.readlines(), f if len(files)>1 else "")
                except Exception as e: out.append(clr(f"grep: {e}", RED))
        else:
            try: search(sys.stdin.readlines())
            except Exception: pass
        return "\n".join(out)

    def _sort(self, a):
        rev = "-r" in a; num = "-n" in a
        files = [x for x in a if not x.startswith("-")]
        lines = []
        for f in files:
            try: lines += open(f).readlines()
            except Exception as e: return clr(f"sort: {e}", RED)
        if not lines:
            try: lines = sys.stdin.readlines()
            except Exception: pass
        key = (lambda l: int(l.strip()) if l.strip().lstrip("-").isdigit() else 0) if num else str.lower
        lines.sort(key=key, reverse=rev)
        return "".join(lines).rstrip("\n")

    # =========================================================================
    # SCHEDULING
    # =========================================================================
    def _sched(self, a):
        if not a or a[0] == "help":
            return clr("  sched add <name> <burst> [priority]\n"
                       "  sched run [rr|priority] [quantum]\n"
                       "  sched list  |  sched clear", CYAN)
        sub = a[0].lower()
        if sub == "add":
            if len(a) < 3: return clr("Usage: sched add <name> <burst> [priority]", YELLOW)
            p = Process(a[1], int(a[2]), int(a[3]) if len(a)>3 else 5)
            self.scheduler.add(p); return clr(f"  Added: {p}", GREEN)
        elif sub == "run":
            if not self.scheduler.queue: return clr("Queue empty. Use: sched add", YELLOW)
            self.scheduler.algo    = a[1].upper() if len(a)>1 else "RR"
            self.scheduler.quantum = int(a[2])    if len(a)>2 else 2
            gantt = self.scheduler.run(); stats = self.scheduler.stats()
            print(clr(f"\n  Algorithm: {self.scheduler.algo}" +
                      (f"  quantum={self.scheduler.quantum}" if self.scheduler.algo=="RR" else ""), BOLD))
            print(clr("  Gantt Chart:", CYAN)); self.scheduler.print_gantt()
            print(clr("\n  Execution order:", CYAN))
            for name, s, e in gantt: print(f"    t={s:>3} → {e:<3}  {name}")
            print(clr(f"\n  Avg Wait: {stats['avg_wait']}  Avg Turnaround: {stats['avg_tat']}", GREEN))
        elif sub == "list":
            return "\n".join(f"  {p}" for p in self.scheduler.queue) or "  Queue is empty."
        elif sub == "clear":
            self.scheduler.queue.clear(); Process._id = Process._order = 1
            return clr("  Queue cleared.", YELLOW)
        else: return clr(f"sched: unknown '{a[0]}'", RED)

    # =========================================================================
    # MEMORY
    # =========================================================================
    def _mem(self, a):
        if not a or a[0]=="help":
            return clr("  mem status | alloc <pid> <bytes> | free <pid>\n"
                       "  mem translate <pid> <vaddr> | algo [fifo|lru]\n"
                       "  mem overflow | mem faultlog", CYAN)
        sub = a[0].lower()
        if sub == "status":
            r = self.memory.status()
            bar = clr("█"*r['used'], RED) + clr("░"*r['free'], GREEN)
            pct = round(r['used']/r['total']*100) if r['total'] else 0
            lines = [f"  Algorithm   : {r['algo']}",
                     f"  Frames      : {r['used']}/{r['total']} used  [{bar}] {pct}%",
                     f"  Page Faults : {r['faults']}   Hits: {r['hits']}",
                     f"  Frame state : {r['frames']}"]
            for pid, info in r['per_proc'].items():
                lines.append(f"  PID {pid:>6}  : {info['pages']} pages ({info['bytes']} bytes)")
            return "\n".join(lines)
        elif sub == "alloc":
            if len(a)<3: return clr("Usage: mem alloc <pid> <bytes>", YELLOW)
            pages = self.memory.allocate(int(a[1]), int(a[2]), verbose=True)
            return clr(f"  Allocated {pages} pages for PID {a[1]}", GREEN)
        elif sub == "free":
            if len(a)<2: return clr("Usage: mem free <pid>", YELLOW)
            freed = self.memory.free(int(a[1])); return clr(f"  Freed {freed} pages for PID {a[1]}", GREEN)
        elif sub == "translate":
            if len(a)<3: return clr("Usage: mem translate <pid> <vaddr>", YELLOW)
            pid, vaddr = int(a[1]), int(a[2])
            phys, fault = self.memory.translate(pid, vaddr)
            if fault: return clr(f"  PAGE FAULT: vaddr {vaddr} not mapped for PID {pid}", RED)
            return clr(f"  Virtual {vaddr} (page={vaddr//PAGE_SIZE} offset={vaddr%PAGE_SIZE}) → Physical {phys}", GREEN)
        elif sub == "algo":
            if len(a)<2: return f"  Current: {self.memory.algo}"
            self.memory = MemoryManager(algo=a[1].upper())
            return clr(f"  Algorithm set to {a[1].upper()}", GREEN)
        elif sub == "overflow":
            print(clr(f"\n  Memory Overflow Simulation ({self.memory.algo}, {NUM_FRAMES} frames)", CYAN))
            for pid in [101, 102, 103]: self.memory.allocate(pid, (NUM_FRAMES//3)*PAGE_SIZE, verbose=True)
            r = self.memory.status()
            print(clr(f"\n  Memory full: {r['used']}/{r['total']} frames — forcing replacements:", YELLOW))
            for pid in [201, 202]: self.memory.allocate(pid, PAGE_SIZE*2, verbose=True)
            r2 = self.memory.status()
            lines = [f"  Total faults: {r2['faults']}   Hits: {r2['hits']}", f"  Frames: {r2['frames']}"]
            for pid, info in r2['per_proc'].items(): lines.append(f"  PID {pid}: {info['pages']} pages ({info['bytes']} B)")
            return "\n".join(lines)
        elif sub == "faultlog":
            if not self.memory.fault_log: return clr("  No faults recorded. Try: mem alloc or mem overflow", YELLOW)
            return clr(f"  Page Fault Log ({len(self.memory.fault_log)} events):\n", CYAN) + \
                   "\n".join(clr(f"  {i+1:>3}. {e}", RED) for i, e in enumerate(self.memory.fault_log))
        else: return clr(f"mem: unknown '{sub}'", RED)

    def _pcsync(self, a):
        if a and a[0] == "race":
            n = int(a[1]) if len(a)>1 else 500
            demo = RaceConditionDemo()
            print(clr(f"\n  Race Condition Demo (each thread increments {n} times)", CYAN))
            unsafe = demo.run_unsafe(n); expected = n*2
            print(clr(f"\n  WITHOUT mutex: expected={expected}  actual={unsafe}  lost={expected-unsafe} updates (race!)", RED))
            safe = demo.run_safe(n)
            print(clr(f"  WITH    mutex: expected={expected}  actual={safe}  correct! (race prevented)", GREEN))
            return ""
        np_ = int(a[0]) if len(a)>0 else 1
        nc  = int(a[1]) if len(a)>1 else 1
        n   = int(a[2]) if len(a)>2 else 4
        print(clr(f"\n  Producer-Consumer (producers={np_}, consumers={nc}, items={n}, buffer=5)", CYAN))
        print(clr("  Mutex guards buffer. Semaphores coordinate flow.", YELLOW))
        return "\n".join(ProducerConsumer(buf_size=5, items=n).run(np_, nc))

    # =========================================================================
    # SECURITY
    # =========================================================================
    def _login(self, a):
        if self.security.current_user: return clr(f"Already logged in as '{self.security.current_user}'.", YELLOW)
        try: u = input("  username: ").strip(); p = input("  password: ").strip()
        except (EOFError, KeyboardInterrupt): return None
        ok, msg = self.security.login(u, p); return clr("  "+msg, GREEN if ok else RED)

    def _logout(self, a):
        if not self.security.current_user: return clr("Not logged in.", YELLOW)
        print(clr("  "+self.security.logout(), YELLOW)); self._login_prompt()

    def _passwd(self, a):
        u = a[0] if a else self.security.current_user
        if not u: return clr("Not logged in.", RED)
        try:
            old = input("  Current password: ").strip() if not self.security.is_root() else ""
            new = input("  New password: ").strip(); cnf = input("  Confirm: ").strip()
        except (EOFError, KeyboardInterrupt): return None
        if new != cnf: return clr("  Passwords do not match.", RED)
        ok, msg = self.security.change_password(u, old, new); return clr("  "+msg, GREEN if ok else RED)

    def _adduser(self, a):
        if not a: return clr("Usage: adduser <user> [password] [role]", YELLOW)
        ok, msg = self.security.add_user(a[0], a[1] if len(a)>1 else a[0]+"123", a[2] if len(a)>2 else "user")
        return clr("  "+msg, GREEN if ok else RED)

    def _users(self, a):
        if not self.security.is_root(): return clr("Permission denied: root only.", RED)
        return "\n".join(f"  {u:12s} role={d['role']}" for u, d in self.security.users.items())

    def _perms(self, a):
        if not a: return clr("Usage: perms check <path> [r|w|x]  |  perms show <path>", YELLOW)
        sub = a[0]
        if sub == "check":
            if len(a)<2: return clr("Usage: perms check <path> [r|w|x]", YELLOW)
            mode = a[2] if len(a)>2 else "r"
            ok, rsn = self.security.check_permission(a[1], mode)
            return clr(f"  {'✓' if ok else '✗'}  {a[1]}  [{mode}]  {rsn}", GREEN if ok else RED)
        elif sub == "show":
            if len(a)<2: return clr("Usage: perms show <path>", YELLOW)
            path  = a[1]
            files = sorted(os.listdir(path)) if os.path.isdir(path) else [os.path.basename(path)]
            base  = path if os.path.isdir(path) else os.path.dirname(path) or "."
            lines = []
            for e in files:
                try:
                    s = os.stat(os.path.join(base, e))
                    lines.append(f"  {stat.filemode(s.st_mode)}  {s.st_size:>8}  "
                                 f"{datetime.fromtimestamp(s.st_mtime).strftime('%b %d %H:%M')}  {e}")
                except Exception as ex: lines.append(clr(str(ex), RED))
            return "\n".join(lines)
        return clr(f"perms: unknown '{sub}'", RED)

    def _help(self, a):
        print("  D1 - Shell & Process Management:")
        print("    cd, pwd, echo, clear, exit, ls [-la], cat, mkdir, rmdir")
        print("    rm [-rf], touch, kill, jobs, fg [id], bg [id], ps, history, alias, sleep")
        print("    grep [-i][-v][-n] <pat> [file]  |  sort [-r][-n] [file]")
        print("    Redirect: > >> <    Background: cmd &")
        print("  D2 - Process Scheduling:")
        print("    sched add <name> <burst> [priority]")
        print("    sched run [rr|priority] [quantum]  |  sched list  |  sched clear")
        print("  D3 - Memory & Synchronization:")
        print("    mem status | alloc <pid> <bytes> | free <pid>")
        print("    mem translate <pid> <vaddr> | algo [fifo|lru] | overflow | faultlog")
        print("    pcsync [producers] [consumers] [items]  |  pcsync race [n]")
        print("  D4 - Piping & Security:")
        print("    cmd1 | cmd2 | cmd3")
        print("    login, logout, whoami, passwd, adduser, users")
        print("    perms check <path> [r|w|x]  |  perms show <path>")
        print("  Default logins: root/root   alice/alice123   bob/bob123")

# =============================================================================
if __name__ == "__main__":
    shell = Shell()
    try: shell.run()
    except SystemExit as e: sys.exit(int(str(e)) if str(e).isdigit() else 0)