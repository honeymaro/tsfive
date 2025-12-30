/**
 * Node.js file source using fs.promises
 * This file is separate to allow browser builds to exclude it
 */

import { open, stat } from 'fs/promises';
import type { FileHandle } from 'fs/promises';
import { FileSource } from './file-source.js';

/**
 * Node.js file system-based file source
 * Uses fs.promises for efficient partial file reads
 */
export class NodeFileSource extends FileSource {
  private _path: string;
  private _fileHandle: FileHandle | null;
  private _size: number | null;

  /**
   * @param filePath - Path to the file
   */
  constructor(filePath: string) {
    super();
    this._path = filePath;
    this._fileHandle = null;
    this._size = null;
  }

  /**
   * Initialize the source (open file and get size)
   * @returns This instance for chaining
   */
  async init(): Promise<NodeFileSource> {
    this._fileHandle = await open(this._path, 'r');
    const stats = await stat(this._path);
    this._size = stats.size;
    return this;
  }

  /**
   * Read a range of bytes from the file
   * @param start - Start offset (inclusive)
   * @param end - End offset (exclusive)
   * @returns The requested data
   */
  async read(start: number, end: number): Promise<ArrayBuffer> {
    if (!this._fileHandle) {
      throw new Error('NodeFileSource not initialized. Call init() first.');
    }

    const length = end - start;
    const buffer = Buffer.alloc(length);
    const { bytesRead } = await this._fileHandle.read(buffer, 0, length, start);

    if (bytesRead < length) {
      // Return only the bytes that were actually read
      return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + bytesRead);
    }

    return buffer.buffer.slice(buffer.byteOffset, buffer.byteOffset + buffer.byteLength);
  }

  /**
   * Close the file handle
   */
  async close(): Promise<void> {
    if (this._fileHandle) {
      await this._fileHandle.close();
      this._fileHandle = null;
    }
  }

  /**
   * Get the total file size
   */
  get size(): number {
    if (this._size === null) {
      throw new Error('NodeFileSource not initialized. Call init() first.');
    }
    return this._size;
  }

  /**
   * The file path
   */
  get path(): string {
    return this._path;
  }
}
