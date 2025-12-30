/**
 * FileSource abstraction layer for lazy loading HDF5 files
 * Supports various data sources: ArrayBuffer, Blob/File, HTTP Range Requests
 */

/**
 * Abstract base class for file sources
 * All file sources must implement this interface
 */
export class FileSource {
  /**
   * Read a range of bytes from the file
   * @param {number} start - Start offset (inclusive)
   * @param {number} end - End offset (exclusive)
   * @returns {Promise<ArrayBuffer>} The requested data
   */
  async read(start, end) {
    throw new Error('Not implemented');
  }

  /**
   * Synchronous read (for legacy compatibility)
   * @param {number} start - Start offset (inclusive)
   * @param {number} end - End offset (exclusive)
   * @returns {ArrayBuffer} The requested data
   */
  readSync(start, end) {
    throw new Error('Synchronous read not supported for this source');
  }

  /**
   * Get the total file size
   * @returns {number} File size in bytes
   */
  get size() {
    throw new Error('Not implemented');
  }

  /**
   * Whether this source supports synchronous reads
   * @returns {boolean}
   */
  get supportsSync() {
    return false;
  }

  /**
   * Close the file source and release resources
   * @returns {Promise<void>}
   */
  async close() {
    // Default: no-op
  }
}

/**
 * ArrayBuffer-based file source (for legacy compatibility)
 * Supports both sync and async reads from an in-memory buffer
 */
export class ArrayBufferSource extends FileSource {
  /**
   * @param {ArrayBuffer} buffer - The complete file data
   */
  constructor(buffer) {
    super();
    this._buffer = buffer;
  }

  async read(start, end) {
    return this._buffer.slice(start, end);
  }

  readSync(start, end) {
    return this._buffer.slice(start, end);
  }

  get size() {
    return this._buffer.byteLength;
  }

  get supportsSync() {
    return true;
  }
}

/**
 * Blob/File-based file source (for browser File API)
 * Uses File.slice() for efficient partial reads
 */
export class BlobSource extends FileSource {
  /**
   * @param {Blob|File} blob - Browser Blob or File object
   */
  constructor(blob) {
    super();
    this._blob = blob;
  }

  async read(start, end) {
    const slice = this._blob.slice(start, end);
    return await slice.arrayBuffer();
  }

  get size() {
    return this._blob.size;
  }
}

/**
 * HTTP Range Request-based file source
 * Fetches only the requested byte ranges from a remote URL
 */
export class HTTPSource extends FileSource {
  /**
   * @param {string} url - The URL to fetch from
   * @param {Object} options - Options
   * @param {number} [options.size] - Known file size (skip HEAD request if provided)
   * @param {Object} [options.fetchOptions] - Additional options to pass to fetch()
   */
  constructor(url, options = {}) {
    super();
    this._url = url;
    this._size = options.size || null;
    this._fetchOptions = options.fetchOptions || {};
    this._supportsRangeRequests = null;
  }

  /**
   * Initialize the source (fetch file size if not provided)
   * @returns {Promise<HTTPSource>} This instance for chaining
   */
  async init() {
    if (this._size === null) {
      const response = await fetch(this._url, {
        method: 'HEAD',
        ...this._fetchOptions
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

  async read(start, end) {
    const response = await fetch(this._url, {
      headers: {
        'Range': `bytes=${start}-${end - 1}`
      },
      ...this._fetchOptions
    });

    if (!response.ok && response.status !== 206) {
      throw new Error(`HTTP Range request failed: ${response.status} ${response.statusText}`);
    }

    return await response.arrayBuffer();
  }

  get size() {
    if (this._size === null) {
      throw new Error('HTTPSource not initialized. Call init() first.');
    }
    return this._size;
  }

  /**
   * Whether the server supports HTTP Range requests
   * @returns {boolean|null} null if not yet determined
   */
  get supportsRangeRequests() {
    return this._supportsRangeRequests;
  }
}
