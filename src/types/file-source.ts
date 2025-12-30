/**
 * FileSource Interface and Related Types
 */

// ============================================================================
// FileSource Interface
// ============================================================================

/**
 * Abstract interface for file data sources.
 * Implementations provide access to HDF5 file data from various sources.
 */
export interface IFileSource {
  /**
   * Read a range of bytes from the file
   * @param start - Start offset (inclusive)
   * @param end - End offset (exclusive)
   * @returns Promise resolving to the requested data
   */
  read(start: number, end: number): Promise<ArrayBuffer>;

  /**
   * Synchronous read (for legacy compatibility)
   * @param start - Start offset (inclusive)
   * @param end - End offset (exclusive)
   * @returns The requested data
   * @throws If synchronous reads are not supported
   */
  readSync?(start: number, end: number): ArrayBuffer;

  /**
   * Total file size in bytes
   */
  readonly size: number;

  /**
   * Whether this source supports synchronous reads
   */
  readonly supportsSync?: boolean;

  /**
   * Close the file source and release resources
   */
  close?(): Promise<void>;
}

// ============================================================================
// Cache Options
// ============================================================================

export interface CacheOptions {
  /**
   * Maximum cache size in bytes
   * @default 67108864 (64MB)
   */
  cacheSize?: number;

  /**
   * Block size for caching in bytes
   * @default 65536 (64KB)
   */
  blockSize?: number;

  /**
   * Alias for blockSize (for openFile options)
   */
  cacheBlockSize?: number;
}

// ============================================================================
// HTTP Source Options
// ============================================================================

export interface HTTPSourceOptions {
  /**
   * Known file size (skip HEAD request if provided)
   */
  size?: number;

  /**
   * Additional options to pass to fetch()
   */
  fetchOptions?: RequestInit;
}

// ============================================================================
// Open File Options
// ============================================================================

export interface OpenFileOptions extends CacheOptions {
  /**
   * Enable lazy loading (only load metadata initially)
   * @default false
   */
  lazy?: boolean;

  /**
   * Optional filename for reference
   */
  filename?: string;

  /**
   * Initial metadata buffer size in bytes
   * @default 65536 (64KB)
   */
  initialMetadataSize?: number;

  /**
   * Additional fetch options for HTTP sources
   */
  fetchOptions?: RequestInit;
}

// ============================================================================
// File Source Input Types
// ============================================================================

/**
 * Supported input types for openFile()
 */
export type FileSourceInput =
  | ArrayBuffer // In-memory buffer
  | Blob // Browser Blob
  | File // Browser File (extends Blob)
  | Response // fetch() Response
  | string // URL (http/https) or file path (Node.js)
  | IFileSource; // Pre-created FileSource

// ============================================================================
// Cache Statistics
// ============================================================================

export interface CacheStats {
  /** Current cache size in bytes */
  size: number;

  /** Maximum cache size in bytes */
  maxSize: number;

  /** Number of cached items */
  count: number;
}

// ============================================================================
// Cache Entry
// ============================================================================

export interface CacheEntry<T = ArrayBuffer> {
  /** Cached data */
  data: T;

  /** Size in bytes */
  size: number;
}
