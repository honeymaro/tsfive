/**
 * tsfive - A pure TypeScript HDF5 file reader
 *
 * Main entry point exporting all public APIs
 */

// Core classes and functions
export {
  Group,
  File,
  Dataset,
  AsyncGroup,
  AsyncFile,
  AsyncDataset,
  openFile,
  Filters,
  FileSource,
  ArrayBufferSource,
  BlobSource,
  HTTPSource,
  LRUCache,
  CachedFileSource,
} from './high-level.js';

// Debug utilities
export { debugLog, debugWarn } from './debug.js';

// Types
export type { OpenFileOptions, ChunkResult } from './high-level.js';
export type { Dtype, DataValue } from './types/dtype.js';
export type { IFileSource, CacheOptions, FileSourceInput } from './types/file-source.js';
export type {
  IGroup,
  IDataset,
  IFile,
  IAsyncGroup,
  IAsyncDataset,
  IAsyncFile,
  ReadOptions,
} from './types/index.js';

// NodeFileSource is available for Node.js environments via:
// import { NodeFileSource } from 'tsfive/node'
// or it will be auto-loaded when using openFile() with a file path string
