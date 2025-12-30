/**
 * LRU Cache and Caching FileSource Wrapper
 */

import type { IFileSource, CacheOptions, CacheStats, CacheEntry } from './types/file-source.js';

// ============================================================================
// LRU Cache
// ============================================================================

/**
 * LRU (Least Recently Used) Cache implementation
 * Evicts least recently used items when cache size exceeds limit
 */
export class LRUCache<T = ArrayBuffer> {
  private _maxSize: number;
  private _currentSize: number;
  private _cache: Map<string, CacheEntry<T>>;

  /**
   * @param maxSize - Maximum cache size in bytes
   */
  constructor(maxSize: number) {
    this._maxSize = maxSize;
    this._currentSize = 0;
    this._cache = new Map(); // Map maintains insertion order
  }

  /**
   * Get an item from the cache
   * @param key - Cache key
   * @returns Cached value or null if not found
   */
  get(key: string): T | null {
    if (!this._cache.has(key)) {
      return null;
    }

    // Move to end (most recently used)
    const entry = this._cache.get(key)!;
    this._cache.delete(key);
    this._cache.set(key, entry);

    return entry.data;
  }

  /**
   * Set an item in the cache
   * @param key - Cache key
   * @param data - Data to cache
   * @param size - Size in bytes (auto-calculated if not provided)
   */
  set(key: string, data: T, size?: number): void {
    // Calculate size if not provided
    if (size === undefined) {
      if (data instanceof ArrayBuffer) {
        size = data.byteLength;
      } else if (Array.isArray(data)) {
        // Estimate: 8 bytes per element (float64)
        size = data.length * 8;
      } else {
        size = 0;
      }
    }

    // Remove existing entry if present
    if (this._cache.has(key)) {
      const existing = this._cache.get(key)!;
      this._currentSize -= existing.size;
      this._cache.delete(key);
    }

    // Evict items until we have enough space
    while (this._currentSize + size > this._maxSize && this._cache.size > 0) {
      const [oldestKey] = this._cache.keys();
      const oldest = this._cache.get(oldestKey)!;
      this._cache.delete(oldestKey);
      this._currentSize -= oldest.size;
    }

    // Add new entry
    this._cache.set(key, { data, size });
    this._currentSize += size;
  }

  /**
   * Check if key exists in cache
   */
  has(key: string): boolean {
    return this._cache.has(key);
  }

  /**
   * Remove an item from the cache
   * @returns True if item was removed
   */
  delete(key: string): boolean {
    if (!this._cache.has(key)) {
      return false;
    }

    const entry = this._cache.get(key)!;
    this._currentSize -= entry.size;
    this._cache.delete(key);
    return true;
  }

  /**
   * Clear all items from the cache
   */
  clear(): void {
    this._cache.clear();
    this._currentSize = 0;
  }

  /**
   * Current cache size in bytes
   */
  get size(): number {
    return this._currentSize;
  }

  /**
   * Maximum cache size in bytes
   */
  get maxSize(): number {
    return this._maxSize;
  }

  /**
   * Number of items in the cache
   */
  get count(): number {
    return this._cache.size;
  }
}

// ============================================================================
// Cached File Source
// ============================================================================

/**
 * File source wrapper with block-level caching
 * Caches file reads in fixed-size blocks for efficient repeated access
 */
export class CachedFileSource implements IFileSource {
  private _source: IFileSource;
  private _cache: LRUCache<ArrayBuffer>;
  private _blockSize: number;

  /**
   * @param source - Underlying file source
   * @param options - Cache options
   */
  constructor(source: IFileSource, options: CacheOptions = {}) {
    this._source = source;
    this._cache = new LRUCache(options.cacheSize || 64 * 1024 * 1024);
    this._blockSize = options.blockSize || 64 * 1024;
  }

  /**
   * Read a range of bytes with caching
   */
  async read(start: number, end: number): Promise<ArrayBuffer> {
    const startBlock = Math.floor(start / this._blockSize);
    const endBlock = Math.ceil(end / this._blockSize);

    const blocks: ArrayBuffer[] = [];

    for (let b = startBlock; b < endBlock; b++) {
      const blockKey = `block_${b}`;
      let blockData = this._cache.get(blockKey);

      if (!blockData) {
        // Cache miss - fetch from source
        const blockStart = b * this._blockSize;
        const blockEnd = Math.min((b + 1) * this._blockSize, this._source.size);
        blockData = await this._source.read(blockStart, blockEnd);
        this._cache.set(blockKey, blockData, blockData.byteLength);
      }

      blocks.push(blockData);
    }

    // Combine blocks and extract requested range
    return this._extractRange(blocks, startBlock, start, end);
  }

  /**
   * Synchronous read with caching (if supported by underlying source)
   */
  readSync(start: number, end: number): ArrayBuffer {
    if (!this._source.readSync) {
      throw new Error('Underlying source does not support synchronous reads');
    }

    const startBlock = Math.floor(start / this._blockSize);
    const endBlock = Math.ceil(end / this._blockSize);

    const blocks: ArrayBuffer[] = [];

    for (let b = startBlock; b < endBlock; b++) {
      const blockKey = `block_${b}`;
      let blockData = this._cache.get(blockKey);

      if (!blockData) {
        const blockStart = b * this._blockSize;
        const blockEnd = Math.min((b + 1) * this._blockSize, this._source.size);
        blockData = this._source.readSync(blockStart, blockEnd);
        this._cache.set(blockKey, blockData, blockData.byteLength);
      }

      blocks.push(blockData);
    }

    return this._extractRange(blocks, startBlock, start, end);
  }

  /**
   * Extract the requested range from cached blocks
   */
  private _extractRange(
    blocks: ArrayBuffer[],
    startBlock: number,
    start: number,
    end: number
  ): ArrayBuffer {
    // Single block case - optimize
    if (blocks.length === 1) {
      const localStart = start - startBlock * this._blockSize;
      const localEnd = localStart + (end - start);
      return blocks[0].slice(localStart, localEnd);
    }

    // Multiple blocks - combine first
    const totalSize = blocks.reduce((sum, b) => sum + b.byteLength, 0);
    const combined = new Uint8Array(totalSize);

    let offset = 0;
    for (const block of blocks) {
      combined.set(new Uint8Array(block), offset);
      offset += block.byteLength;
    }

    const localStart = start - startBlock * this._blockSize;
    const localEnd = localStart + (end - start);

    return combined.buffer.slice(localStart, localEnd);
  }

  /**
   * File size
   */
  get size(): number {
    return this._source.size;
  }

  /**
   * Whether synchronous reads are supported
   */
  get supportsSync(): boolean {
    return this._source.supportsSync ?? false;
  }

  /**
   * Close the underlying source
   */
  async close(): Promise<void> {
    if (this._source.close) {
      await this._source.close();
    }
  }

  /**
   * Clear the cache
   */
  clearCache(): void {
    this._cache.clear();
  }

  /**
   * Get cache statistics
   */
  get cacheStats(): CacheStats {
    return {
      size: this._cache.size,
      maxSize: this._cache.maxSize,
      count: this._cache.count,
    };
  }
}
