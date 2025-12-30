/**
 * LRU Cache and caching file source wrapper for jsfive
 */

import { FileSource } from './file-source.js';

/**
 * LRU (Least Recently Used) Cache implementation
 * Evicts least recently used items when cache size exceeds limit
 */
export class LRUCache {
  /**
   * @param {number} maxSize - Maximum cache size in bytes
   */
  constructor(maxSize) {
    this._maxSize = maxSize;
    this._currentSize = 0;
    this._cache = new Map(); // Map maintains insertion order
  }

  /**
   * Get an item from the cache
   * @param {string} key - Cache key
   * @returns {*} Cached value or null if not found
   */
  get(key) {
    if (!this._cache.has(key)) {
      return null;
    }

    // Move to end (most recently used)
    const entry = this._cache.get(key);
    this._cache.delete(key);
    this._cache.set(key, entry);

    return entry.data;
  }

  /**
   * Set an item in the cache
   * @param {string} key - Cache key
   * @param {ArrayBuffer|Array} data - Data to cache
   * @param {number} [size] - Size in bytes (auto-calculated if not provided)
   */
  set(key, data, size) {
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
      const existing = this._cache.get(key);
      this._currentSize -= existing.size;
      this._cache.delete(key);
    }

    // Evict items until we have enough space
    while (this._currentSize + size > this._maxSize && this._cache.size > 0) {
      const [oldestKey] = this._cache.keys();
      const oldest = this._cache.get(oldestKey);
      this._cache.delete(oldestKey);
      this._currentSize -= oldest.size;
    }

    // Add new entry
    this._cache.set(key, { data, size });
    this._currentSize += size;
  }

  /**
   * Check if key exists in cache
   * @param {string} key
   * @returns {boolean}
   */
  has(key) {
    return this._cache.has(key);
  }

  /**
   * Remove an item from the cache
   * @param {string} key
   * @returns {boolean} True if item was removed
   */
  delete(key) {
    if (!this._cache.has(key)) {
      return false;
    }

    const entry = this._cache.get(key);
    this._currentSize -= entry.size;
    this._cache.delete(key);
    return true;
  }

  /**
   * Clear all items from the cache
   */
  clear() {
    this._cache.clear();
    this._currentSize = 0;
  }

  /**
   * Get the current cache size in bytes
   * @returns {number}
   */
  get size() {
    return this._currentSize;
  }

  /**
   * Get the maximum cache size in bytes
   * @returns {number}
   */
  get maxSize() {
    return this._maxSize;
  }

  /**
   * Get the number of items in the cache
   * @returns {number}
   */
  get count() {
    return this._cache.size;
  }
}


/**
 * File source wrapper with block-level caching
 * Caches file reads in fixed-size blocks for efficient repeated access
 */
export class CachedFileSource extends FileSource {
  /**
   * @param {FileSource} source - Underlying file source
   * @param {Object} options - Options
   * @param {number} [options.cacheSize=67108864] - Cache size in bytes (default 64MB)
   * @param {number} [options.blockSize=65536] - Block size in bytes (default 64KB)
   */
  constructor(source, options = {}) {
    super();
    this._source = source;
    this._cache = new LRUCache(options.cacheSize || 64 * 1024 * 1024);
    this._blockSize = options.blockSize || 64 * 1024;
  }

  /**
   * Read a range of bytes with caching
   * @param {number} start - Start offset
   * @param {number} end - End offset
   * @returns {Promise<ArrayBuffer>}
   */
  async read(start, end) {
    const startBlock = Math.floor(start / this._blockSize);
    const endBlock = Math.ceil(end / this._blockSize);

    const blocks = [];

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
   * @param {number} start
   * @param {number} end
   * @returns {ArrayBuffer}
   */
  readSync(start, end) {
    if (!this._source.supportsSync) {
      throw new Error('Underlying source does not support synchronous reads');
    }

    const startBlock = Math.floor(start / this._blockSize);
    const endBlock = Math.ceil(end / this._blockSize);

    const blocks = [];

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
   * @private
   */
  _extractRange(blocks, startBlock, start, end) {
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
   * Get the file size
   * @returns {number}
   */
  get size() {
    return this._source.size;
  }

  /**
   * Whether synchronous reads are supported
   * @returns {boolean}
   */
  get supportsSync() {
    return this._source.supportsSync;
  }

  /**
   * Close the underlying source
   * @returns {Promise<void>}
   */
  async close() {
    await this._source.close();
  }

  /**
   * Clear the cache
   */
  clearCache() {
    this._cache.clear();
  }

  /**
   * Get cache statistics
   * @returns {{size: number, maxSize: number, count: number, hitRate: number}}
   */
  get cacheStats() {
    return {
      size: this._cache.size,
      maxSize: this._cache.maxSize,
      count: this._cache.count
    };
  }
}
