/**
 * Public Type Definitions for tsfive
 *
 * This module exports all public types used by the library.
 */

// Re-export all types from submodules
export * from './hdf5.js';
export * from './dtype.js';
export * from './file-source.js';

// ============================================================================
// Attribute Types
// ============================================================================

import type { DataValue, Dtype, DatasetValue } from './dtype.js';

/** Attribute value types */
export type AttributeValue = DataValue;

/** Attributes dictionary */
export interface Attributes {
  [name: string]: AttributeValue;
}

// ============================================================================
// Group Interface
// ============================================================================

export interface IGroup {
  /** Names of all direct children */
  readonly keys: string[];

  /** Direct child objects */
  readonly values: Array<IGroup | IDataset>;

  /** Full path to this group */
  readonly name: string;

  /** File instance where this group resides */
  readonly file: IFile;

  /** Parent group (self for root) */
  readonly parent: IGroup;

  /** Attributes for this group */
  readonly attrs: Attributes;

  /** Number of direct children */
  length(): number;

  /**
   * Get a child object by path
   * @param path - Relative or absolute path
   */
  get(path: string | number): IGroup | IDataset | null;

  /**
   * Recursively visit all names in the group
   * @param func - Callback receiving name, return non-null to stop
   */
  visit(func: (name: string) => unknown): unknown;

  /**
   * Recursively visit all objects in the group
   * @param func - Callback receiving name and object, return non-null to stop
   */
  visititems(func: (name: string, obj: IGroup | IDataset) => unknown): unknown;
}

// ============================================================================
// Dataset Interface
// ============================================================================

export interface IDataset {
  /** Dataset dimensions */
  readonly shape: number[];

  /** Dataset's data type */
  readonly dtype: Dtype;

  /** Total number of elements */
  readonly size: number;

  /** Number of dimensions */
  readonly ndim: number;

  /** Fill value for uninitialized portions */
  readonly fillvalue: unknown;

  /** Dataset's attributes */
  readonly attrs: Attributes;

  /** Full path to this dataset */
  readonly name: string;

  /** File instance where this dataset resides */
  readonly file: IFile;

  /** Parent group */
  readonly parent: IGroup;

  /** Get the complete data (synchronous) */
  readonly value: DatasetValue;
}

// ============================================================================
// File Interface
// ============================================================================

export interface IFile extends IGroup {
  /** Name of the file on disk */
  readonly filename: string;

  /** File mode (always 'r' for read-only) */
  readonly mode: 'r';

  /** Size of the user block in bytes */
  readonly userblock_size: number;
}

// ============================================================================
// Async Group Interface
// ============================================================================

export interface IAsyncGroup extends Omit<IGroup, 'get' | 'values'> {
  /**
   * Get a child object asynchronously
   * @param path - Path to child
   */
  getAsync(path: string | number): Promise<IAsyncGroup | IAsyncDataset | null>;

  /**
   * Synchronous get (requires links to be initialized)
   */
  get(path: string | number): IAsyncGroup | IAsyncDataset | null;

  /**
   * Initialize links asynchronously
   */
  initLinks(): Promise<void>;

  /**
   * Close and release resources
   */
  close(): Promise<void>;
}

// ============================================================================
// Async Dataset Interface
// ============================================================================

export interface IAsyncDataset extends Omit<IDataset, 'value'> {
  /**
   * Load the entire dataset asynchronously
   */
  valueAsync(): Promise<DatasetValue>;

  /**
   * Read a slice of data asynchronously (1D indexing)
   * @param start - Start index (inclusive)
   * @param end - End index (exclusive)
   */
  sliceAsync(start: number, end: number): Promise<DataValue[]>;

  /**
   * Read data with options
   */
  read(options?: ReadOptions): Promise<DataValue[]>;

  /**
   * Iterate over the dataset in chunks
   */
  iterChunks(options?: ChunkIteratorOptions): AsyncIterableIterator<ChunkResult>;

  /**
   * Get a ReadableStream of the dataset
   */
  stream(options?: StreamOptions): ReadableStream<ChunkResult>;
}

// ============================================================================
// Async File Interface
// ============================================================================

export interface IAsyncFile extends IAsyncGroup {
  /** Name of the file */
  readonly filename: string;

  /** File mode */
  readonly mode: 'r';

  /** User block size */
  readonly userblock_size: number;
}

// ============================================================================
// Read Options
// ============================================================================

export interface ReadOptions {
  /** Start index (1D) */
  start?: number;

  /** End index (1D) */
  end?: number;

  /** Multi-dimensional slice [[start, end], ...] */
  slice?: Array<[number, number]>;
}

// ============================================================================
// Chunk Iterator Options
// ============================================================================

export interface ChunkIteratorOptions {
  /**
   * Number of elements per chunk
   * @default 100000
   */
  chunkSize?: number;
}

// ============================================================================
// Stream Options
// ============================================================================

export interface StreamOptions {
  /**
   * Number of elements per chunk
   * @default 100000
   */
  chunkSize?: number;
}

// ============================================================================
// Chunk Result
// ============================================================================

export interface ChunkResult {
  /** Chunk data */
  data: DataValue[];

  /** Offset in the dataset */
  offset: number;

  /** Number of elements in this chunk */
  size: number;

  /** Whether this is the last chunk */
  isLast: boolean;
}

// ============================================================================
// Filter Function Type
// ============================================================================

/**
 * Filter function for decompressing/processing data
 * @param data - Input data buffer
 * @param itemsize - Size of each element in bytes
 * @param clientData - Optional filter client data
 * @returns Processed data buffer
 */
export type FilterFunction = (
  data: ArrayBuffer,
  itemsize?: number,
  clientData?: number[]
) => ArrayBuffer;

// ============================================================================
// Reference Class
// ============================================================================

export interface IReference {
  /** Address of the referenced object */
  readonly address_of_reference: number;

  /** Whether the reference is valid (non-zero) */
  __bool__(): boolean;
}
