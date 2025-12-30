/**
 * FileSource abstraction layer for lazy loading HDF5 files
 * Supports various data sources: ArrayBuffer, Blob/File, HTTP Range Requests
 */

import type { IFileSource, HTTPSourceOptions } from './types/file-source.js';

// ============================================================================
// Abstract Base Class
// ============================================================================

/**
 * Abstract base class for file sources
 * All file sources must implement this interface
 */
export abstract class FileSource implements IFileSource {
  /**
   * Read a range of bytes from the file
   * @param start - Start offset (inclusive)
   * @param end - End offset (exclusive)
   * @returns Promise resolving to the requested data
   */
  async read(_start: number, _end: number): Promise<ArrayBuffer> {
    throw new Error('Not implemented');
  }

  /**
   * Synchronous read (for legacy compatibility)
   * @param start - Start offset (inclusive)
   * @param end - End offset (exclusive)
   * @returns The requested data
   */
  readSync(_start: number, _end: number): ArrayBuffer {
    throw new Error('Synchronous read not supported for this source');
  }

  /**
   * Get the total file size
   */
  abstract get size(): number;

  /**
   * Whether this source supports synchronous reads
   */
  get supportsSync(): boolean {
    return false;
  }

  /**
   * Close the file source and release resources
   */
  async close(): Promise<void> {
    // Default: no-op
  }
}

// ============================================================================
// ArrayBuffer Source
// ============================================================================

/**
 * ArrayBuffer-based file source (for legacy compatibility)
 * Supports both sync and async reads from an in-memory buffer
 */
export class ArrayBufferSource extends FileSource {
  private _buffer: ArrayBuffer;

  /**
   * @param buffer - The complete file data
   */
  constructor(buffer: ArrayBuffer) {
    super();
    this._buffer = buffer;
  }

  async read(start: number, end: number): Promise<ArrayBuffer> {
    return this._buffer.slice(start, end);
  }

  readSync(start: number, end: number): ArrayBuffer {
    return this._buffer.slice(start, end);
  }

  get size(): number {
    return this._buffer.byteLength;
  }

  get supportsSync(): boolean {
    return true;
  }
}

// ============================================================================
// Blob Source
// ============================================================================

/**
 * Blob/File-based file source (for browser File API)
 * Uses File.slice() for efficient partial reads
 */
export class BlobSource extends FileSource {
  private _blob: Blob;

  /**
   * @param blob - Browser Blob or File object
   */
  constructor(blob: Blob) {
    super();
    this._blob = blob;
  }

  async read(start: number, end: number): Promise<ArrayBuffer> {
    const slice = this._blob.slice(start, end);
    return await slice.arrayBuffer();
  }

  get size(): number {
    return this._blob.size;
  }
}

// ============================================================================
// HTTP Source
// ============================================================================

/**
 * HTTP Range Request-based file source
 * Fetches only the requested byte ranges from a remote URL
 */
export class HTTPSource extends FileSource {
  private _url: string;
  private _size: number | null;
  private _fetchOptions: RequestInit;
  private _supportsRangeRequests: boolean | null;

  /**
   * @param url - The URL to fetch from
   * @param options - Options
   */
  constructor(url: string, options: HTTPSourceOptions = {}) {
    super();
    this._url = url;
    this._size = options.size ?? null;
    this._fetchOptions = options.fetchOptions ?? {};
    this._supportsRangeRequests = null;
  }

  /**
   * Initialize the source (fetch file size if not provided)
   * @returns This instance for chaining
   */
  async init(): Promise<HTTPSource> {
    if (this._size === null) {
      const response = await fetch(this._url, {
        method: 'HEAD',
        ...this._fetchOptions,
      });

      if (!response.ok) {
        throw new Error(`HTTP HEAD request failed: ${response.status} ${response.statusText}`);
      }

      const contentLength = response.headers.get('Content-Length');
      if (contentLength) {
        this._size = parseInt(contentLength, 10);
      } else {
        throw new Error('Server did not provide Content-Length header');
      }

      // Check if server supports range requests
      const acceptRanges = response.headers.get('Accept-Ranges');
      this._supportsRangeRequests = acceptRanges === 'bytes';
    }
    return this;
  }

  async read(start: number, end: number): Promise<ArrayBuffer> {
    const response = await fetch(this._url, {
      headers: {
        Range: `bytes=${start}-${end - 1}`,
      },
      ...this._fetchOptions,
    });

    if (!response.ok && response.status !== 206) {
      throw new Error(`HTTP Range request failed: ${response.status} ${response.statusText}`);
    }

    return await response.arrayBuffer();
  }

  get size(): number {
    if (this._size === null) {
      throw new Error('HTTPSource not initialized. Call init() first.');
    }
    return this._size;
  }

  /**
   * Whether the server supports HTTP Range requests
   * @returns null if not yet determined
   */
  get supportsRangeRequests(): boolean | null {
    return this._supportsRangeRequests;
  }
}
