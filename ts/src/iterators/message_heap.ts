// Tiny binary heap with bigint priorities, used to merge per-group iterators
// into a single time-ordered stream. Mirrors go/message_heap.go semantics
// (separate forward/reverse heaps replaced by a `reverse` flag).

interface HeapItem<T> {
  priority: bigint;
  value: T;
}

export class MessageHeap<T> {
  private readonly items: HeapItem<T>[] = [];
  private readonly cmpSign: bigint;

  /** reverse=false: min-heap (time order). reverse=true: max-heap (reverse time). */
  constructor(reverse: boolean) {
    this.cmpSign = reverse ? -1n : 1n;
  }

  get size(): number {
    return this.items.length;
  }

  push(priority: bigint, value: T): void {
    this.items.push({ priority, value });
    this.siftUp(this.items.length - 1);
  }

  pop(): { priority: bigint; value: T } | undefined {
    if (this.items.length === 0) {
      return undefined;
    }
    const top = this.items[0]!;
    const last = this.items.pop()!;
    if (this.items.length > 0) {
      this.items[0] = last;
      this.siftDown(0);
    }
    return top;
  }

  /** Return < 0 if a goes before b, > 0 if after, 0 if equal. */
  private compare(a: bigint, b: bigint): bigint {
    return (a - b) * this.cmpSign;
  }

  private siftUp(idx: number): void {
    while (idx > 0) {
      const parent = (idx - 1) >> 1;
      if (
        this.compare(this.items[idx]!.priority, this.items[parent]!.priority) <
        0n
      ) {
        const tmp = this.items[idx]!;
        this.items[idx] = this.items[parent]!;
        this.items[parent] = tmp;
        idx = parent;
      } else {
        break;
      }
    }
  }

  private siftDown(idx: number): void {
    const n = this.items.length;
    while (true) {
      const l = idx * 2 + 1;
      const r = idx * 2 + 2;
      let best = idx;
      if (
        l < n &&
        this.compare(this.items[l]!.priority, this.items[best]!.priority) < 0n
      ) {
        best = l;
      }
      if (
        r < n &&
        this.compare(this.items[r]!.priority, this.items[best]!.priority) < 0n
      ) {
        best = r;
      }
      if (best === idx) {
        return;
      }
      const tmp = this.items[idx]!;
      this.items[idx] = this.items[best]!;
      this.items[best] = tmp;
      idx = best;
    }
  }
}
