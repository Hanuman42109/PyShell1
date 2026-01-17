import os
import sys
import signal
import shlex
import glob

# ---------------- GLOBAL STATE ---------------- #

jobs = []
job_id_counter = 1
shell_pgid = os.getpgrp()
terminal_fd = sys.stdin.fileno()

# ---------------- SIGNAL SETUP ---------------- #

signal.signal(signal.SIGINT, signal.SIG_IGN)
signal.signal(signal.SIGTSTP, signal.SIG_IGN)
signal.signal(signal.SIGTTIN, signal.SIG_IGN)
signal.signal(signal.SIGTTOU, signal.SIG_IGN)


def sigchld_handler(signum, frame):
    global jobs
    new_jobs = []
    for job in jobs:
        try:
            pid, status = os.waitpid(job["pid"], os.WNOHANG)
            if pid == 0:
                new_jobs.append(job)
        except ChildProcessError:
            pass
    jobs = new_jobs


signal.signal(signal.SIGCHLD, sigchld_handler)

# ---------------- PARSING ---------------- #

def parse_command(command):
    command = command.strip()
    if not command:
        return [], False

    tokens = shlex.split(command)
    background = False

    if tokens and tokens[-1] == "&":
        background = True
        tokens = tokens[:-1]

    return tokens, background

# ---------------- BUILTINS ---------------- #

def handle_builtin(tokens):
    global jobs

    cmd = tokens[0]

    try:
        if cmd == "exit":
            sys.exit(0)
        
        elif cmd == "cd":
            if len(tokens) == 1:
                os.chdir(os.path.expanduser("~"))
            else:
                matches = glob.glob(tokens[1])
                if not matches:
                    print(f"cd: no such file or directory: {tokens[1]}")
                else:
                    os.chdir(matches[0])

        elif cmd == "jobs":
            for job in jobs:
                print(f"[{job['id']}] {job['state']} PID={job['pid']} {job['cmd']}")

        elif cmd == "fg":
            if len(tokens) < 2:
                print("Usage: fg <job_id>")
                return True

            jid = int(tokens[1])
            for job in jobs:
                if job["id"] == jid:
                    os.tcsetpgrp(terminal_fd, job["pid"])
                    os.kill(job["pid"], signal.SIGCONT)

                    while True:
                        try:
                            _, status = os.waitpid(job["pid"], os.WUNTRACED)
                            break
                        except InterruptedError:
                            continue

                    os.tcsetpgrp(terminal_fd, shell_pgid)

                    if os.WIFSTOPPED(status):
                        job["state"] = "STOPPED"
                    else:
                        jobs.remove(job)
                    return True

            print(f"fg: job {jid} not found")

        elif cmd == "bg":
            if len(tokens) < 2:
                print("Usage: bg <job_id>")
                return True

            jid = int(tokens[1])
            for job in jobs:
                if job["id"] == jid:
                    os.kill(job["pid"], signal.SIGCONT)
                    job["state"] = "RUNNING"
                    return True

            print(f"bg: job {jid} not found")

        elif cmd == "kill":
            if len(tokens) < 2:
                print("Usage: kill <pid>")
                return True

            pid = int(tokens[1])
            os.kill(pid, signal.SIGTERM)

            # Remove job immediately
            jobs[:] = [job for job in jobs if job["pid"] != pid]

        else:
            return False

    except Exception as e:
        print(f"Error: {e}")

    return True

# ---------------- EXECUTION ---------------- #

def execute_command(tokens, background, raw_cmd):
    global job_id_counter

    pid = os.fork()

    if pid == 0:
        os.setpgid(0, 0)

        if not background:
            os.tcsetpgrp(terminal_fd, os.getpid())

        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTSTP, signal.SIG_DFL)

        try:
            os.execvp(tokens[0], tokens)
        except FileNotFoundError:
            print("Command not found")
            sys.exit(1)

    else:
        os.setpgid(pid, pid)

        if background:
            jobs.append({
                "id": job_id_counter,
                "pid": pid,
                "cmd": raw_cmd,
                "state": "RUNNING"
            })
            print(f"[{job_id_counter}] {pid}")
            job_id_counter += 1
        else:
            os.tcsetpgrp(terminal_fd, pid)

            while True:
                try:
                    os.waitpid(pid, os.WUNTRACED)
                    break
                except InterruptedError:
                    continue

            os.tcsetpgrp(terminal_fd, shell_pgid)

# ---------------- MAIN LOOP ---------------- #

def main():
    os.setpgid(0, 0)
    os.tcsetpgrp(terminal_fd, shell_pgid)

    while True:
        print(get_prompt(), end="", flush=True)

        try:
            command = sys.stdin.readline()
        except OSError:
            continue

        tokens, background = parse_command(command)
        if not tokens:
            continue

        if handle_builtin(tokens):
            continue

        execute_command(tokens, background, command.strip())

def get_prompt():
    cwd = os.getcwd()
    folder = os.path.basename(cwd)
    return f"pyshell/{folder}> "

if __name__ == "__main__":
    main()