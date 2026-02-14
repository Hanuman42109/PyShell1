import time
from collections import deque

# ================= MEMORY MANAGEMENT ================= #

class MemoryManager:
    def __init__(self, frames, algorithm="fifo"):
        self.frames = frames
        self.algorithm = algorithm
        self.memory = []
        self.page_faults = 0
        self.usage_time = {}
        self.time = 0

    def access_page(self, page):
        self.time += 1

        if page in self.memory:
            if self.algorithm == "lru":
                self.usage_time[page] = self.time
            print(f"Page {page} → HIT")
            return

        self.page_faults += 1
        print(f"Page {page} → PAGE FAULT")

        if len(self.memory) >= self.frames:
            self.replace_page()

        self.memory.append(page)
        self.usage_time[page] = self.time
        self.show_memory()

    def replace_page(self):
        if self.algorithm == "fifo":
            removed = self.memory.pop(0)
            print(f"Replacing (FIFO) page {removed}")

        elif self.algorithm == "lru":
            lru_page = min(self.memory, key=lambda p: self.usage_time[p])
            self.memory.remove(lru_page)
            print(f"Replacing (LRU) page {lru_page}")

    def show_memory(self):
        print("Memory:", self.memory)

    def stats(self):
        print("Total Page Faults:", self.page_faults)


# ================= SYNCHRONIZATION ================= #

class Semaphore:
    def __init__(self, value):
        self.value = value

    def wait(self):
        while self.value <= 0:
            time.sleep(0.5)
        self.value -= 1

    def signal(self):
        self.value += 1


class ProducerConsumer:
    def __init__(self, size):
        self.buffer = deque()
        self.size = size
        self.empty = Semaphore(size)
        self.full = Semaphore(0)
        self.mutex = Semaphore(1)

    def produce(self, item):
        self.empty.wait()
        self.mutex.wait()

        self.buffer.append(item)
        print(f"Produced {item} | Buffer: {list(self.buffer)}")

        self.mutex.signal()
        self.full.signal()

    def consume(self):
        self.full.wait()
        self.mutex.wait()

        item = self.buffer.popleft()
        print(f"Consumed {item} | Buffer: {list(self.buffer)}")

        self.mutex.signal()
        self.empty.signal()


# ================= SHELL ================= #

def main():
    memory = None
    pc = ProducerConsumer(3)

    while True:
        cmd = input("memsync> ").strip().split()

        if not cmd:
            continue

        # MEMORY COMMANDS
        if cmd[0] == "meminit":
            frames = int(cmd[1])
            algo = cmd[2].lower()
            memory = MemoryManager(frames, algo)
            print(f"Memory initialized: {frames} frames, {algo.upper()}")

        elif cmd[0] == "access":
            if memory is None:
                print("Initialize memory first")
                continue
            page = int(cmd[1])
            memory.access_page(page)

        elif cmd[0] == "memstat":
            if memory:
                memory.stats()

        # SYNCHRONIZATION COMMANDS
        elif cmd[0] == "produce":
            item = cmd[1]
            pc.produce(item)

        elif cmd[0] == "consume":
            pc.consume()

        elif cmd[0] == "exit":
            break

        else:
            print("Commands:")
            print(" meminit <frames> <fifo|lru>")
            print(" access <page>")
            print(" memstat")
            print(" produce <item>")
            print(" consume")
            print(" exit")


if __name__ == "__main__":
    main()