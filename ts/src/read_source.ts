/**
 * ReadSource is the storage abstraction the Reader uses. It is intentionally
 * minimal: every Go ReadAt/Read+Seek call site can be expressed as `read(off,
 * len)`.
 *
 * Concurrency contract: implementations MUST be safe for concurrent `read`
 * calls. The cost-aware reader path issues many in-flight reads bounded only
 * by `maxConcurrency`.
 */
export interface ReadSource {
  /** Total length of the underlying file/blob/resource, in bytes. */
  size(): Promise<bigint>;

  /**
   * Read exactly `length` bytes starting at `offset`. Implementations should
   * throw if fewer bytes are available; the Reader assumes a full read.
   */
  read(offset: bigint, length: bigint): Promise<Uint8Array>;
}
