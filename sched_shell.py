import time
import heapq
from collections import deque

# ---------------- PROCESS STRUCTURE ---------------- #

class Process:
    pid_counter = 1

    def __init__(self, name, burst, priority):
        self.pid = Process.pid_counter
        Process.pid_counter += 1
        self.name = name
        self.burst = burst
        self.priority = priority
        self.reset()

    def reset(self):
        self.remaining = self.burst
        self.arrival = 0
        self.start = None
        self.finish = None


# ---------------- METRICS ---------------- #

def print_metrics(processes):
    print("\nPID | Waiting | Turnaround | Response")
    for p in processes:
        turnaround = p.finish - p.arrival
        waiting = turnaround - p.burst
        response = p.start - p.arrival
        print(f"{p.pid:3} | {waiting:7.2f} | {turnaround:10.2f} | {response:8.2f}")


# ---------------- RESET ---------------- #

def reset_processes(processes):
    current_time = 0
    for p in processes:
        p.reset()
        p.arrival = current_time


# ---------------- ROUND ROBIN ---------------- #

def round_robin(processes, quantum):
    print(f"\n--- Round Robin Scheduling (Quantum={quantum}s) ---")
    queue = deque(processes)
    time_now = 0
    completed = []

    while queue:
        p = queue.popleft()

        if p.start is None:
            p.start = time_now

        run_time = min(quantum, p.remaining)
        print(f"Running P{p.pid} for {run_time}s")
        time.sleep(run_time)

        p.remaining -= run_time
        time_now += run_time

        if p.remaining > 0:
            queue.append(p)
        else:
            p.finish = time_now
            completed.append(p)

    print_metrics(completed)


# ---------------- PRIORITY SCHEDULING ---------------- #

def priority_scheduling(processes):
    print("\n--- Priority Scheduling (Preemptive) ---")
    heap = []
    completed = []
    time_now = 0

    for p in processes:
        heapq.heappush(heap, (p.priority, p.pid, p))

    while heap:
        _, _, p = heapq.heappop(heap)

        if p.start is None:
            p.start = time_now

        print(f"Running P{p.pid} (Priority={p.priority})")
        time.sleep(1)

        p.remaining -= 1
        time_now += 1

        if p.remaining == 0:
            p.finish = time_now
            completed.append(p)
        else:
            heapq.heappush(heap, (p.priority, p.pid, p))

    print_metrics(completed)


# ---------------- SHELL ---------------- #

def main():
    processes = []

    while True:
        cmd = input("sched> ").strip().split()

        if not cmd:
            continue

        if cmd[0] == "add":
            if len(cmd) != 4:
                print("Usage: add <name> <burst> <priority>")
                continue
            name = cmd[1]
            burst = int(cmd[2])
            priority = int(cmd[3])
            p = Process(name, burst, priority)
            processes.append(p)
            print(f"Added P{p.pid}")

        elif cmd[0] == "rr":
            if len(cmd) != 2:
                print("Usage: rr <quantum>")
                continue
            quantum = int(cmd[1])
            reset_processes(processes)
            round_robin(processes.copy(), quantum)

        elif cmd[0] == "priority":
            reset_processes(processes)
            priority_scheduling(processes.copy())

        elif cmd[0] == "exit":
            break

        else:
            print("Commands:")
            print(" add <name> <burst> <priority>")
            print(" rr <quantum>")
            print(" priority")
            print(" exit")


if __name__ == "__main__":
    main()