/**
 * High-level HDF5 API
 * Provides Group, File, Dataset, and async variants for reading HDF5 files
 */

import { DataObjects } from './dataobjects.js';
import { AsyncDataObjects } from './async-dataobjects.js';
import { SuperBlock } from './misc-low-level.js';
import { ArrayBufferSource, BlobSource, HTTPSource } from './file-source.js';
import { CachedFileSource } from './cache.js';
import { debugLog } from './debug.js';
import type { IFileSource } from './types/file-source.js';
import type { Dtype, DataValue } from './types/dtype.js';
import type { Links } from './types/hdf5.js';

// Re-exports
export { Filters } from './filters.js';
export { FileSource, ArrayBufferSource, BlobSource, HTTPSource } from './file-source.js';
export { LRUCache, CachedFileSource } from './cache.js';

// ============================================================================
// Utility Functions
// ============================================================================

function posix_dirname(p: string): string {
  const sep = '/';
  const i = p.lastIndexOf(sep) + 1;
  let head = p.slice(0, i);
  const all_sep = new RegExp('^' + sep + '+$');
  const end_sep = new RegExp(sep + '$');
  if (head && !all_sep.test(head)) {
    head = head.replace(end_sep, '');
  }
  return head;
}

function normpath(path: string): string {
  return path.replace(/\/(\/)+/g, '/');
}

// ============================================================================
// Proxy Handler
// ============================================================================

const groupGetHandler: ProxyHandler<Group> = {
  get: function (target: Group, prop: string | symbol, _receiver: unknown): unknown {
    if (prop in target) {
      return (target as unknown as Record<string | symbol, unknown>)[prop];
    }
    if (typeof prop === 'string') {
      return target.get(prop);
    }
    return undefined;
  },
};

// ============================================================================
// Group Class
// ============================================================================

/**
 * An HDF5 Group which may hold attributes, datasets, or other groups.
 */
export class Group {
  parent: Group | File;
  file: File;
  name: string;
  protected _links: Links | null;
  protected _dataobjects: DataObjects;
  protected _attrs: Record<string, DataValue> | null;
  protected _keys: string[] | null;

  /**
   * @param name - Group name/path
   * @param dataobjects - DataObjects instance
   * @param parent - Parent group (null for root)
   * @param getterProxy - Whether to wrap in a Proxy for bracket notation access
   * @param skipLinksInit - Skip synchronous links initialization (for async subclasses)
   */
  constructor(
    name: string,
    dataobjects: DataObjects,
    parent?: Group | null,
    getterProxy: boolean = false,
    skipLinksInit: boolean = false
  ) {
    if (parent == null) {
      this.parent = this;
      this.file = this as unknown as File;
    } else {
      this.parent = parent;
      this.file = parent.file;
    }
    this.name = name;

    this._links = skipLinksInit ? null : dataobjects.get_links();
    this._dataobjects = dataobjects;
    this._attrs = null;
    this._keys = null;

    if (getterProxy) {
      return new Proxy(this, groupGetHandler);
    }
  }

  get keys(): string[] {
    if (this._keys == null) {
      this._keys = Object.keys(this._links || {});
    }
    return this._keys.slice();
  }

  get values(): Array<Group | Dataset> {
    return this.keys.map((k) => this.get(k)!);
  }

  length(): number {
    return this.keys.length;
  }

  protected _dereference(ref: number): Group | Dataset {
    if (!ref) {
      throw new Error('cannot dereference null reference');
    }
    const obj = this.file._get_object_by_address(ref);
    if (obj == null) {
      throw new Error('reference not found in file');
    }
    return obj;
  }

  /**
   * Get a child object by path or reference
   * @param y - Path string or reference number
   */
  get(y: string | number): Group | Dataset | File | null {
    if (typeof y === 'number') {
      return this._dereference(y);
    }

    const path = normpath(y);
    if (path === '/') {
      return this.file;
    }

    if (path === '.') {
      return this;
    }
    if (/^\//.test(path)) {
      return this.file.get(path.slice(1));
    }

    let next_obj: string;
    let additional_obj: string;

    if (posix_dirname(path) !== '') {
      [next_obj, additional_obj] = path.split(/\/(.*)/);
    } else {
      next_obj = path;
      additional_obj = '.';
    }

    if (!(next_obj in (this._links || {}))) {
      throw new Error(next_obj + ' not found in group');
    }

    const obj_name = normpath(this.name + '/' + next_obj);
    const link_target = this._links![next_obj];

    if (typeof link_target === 'string') {
      try {
        return this.get(link_target);
      } catch (_error) {
        return null;
      }
    }

    const dataobjs = new DataObjects(this.file._fh, link_target as number);
    if (dataobjs.is_dataset) {
      if (additional_obj !== '.') {
        throw new Error(obj_name + ' is a dataset, not a group');
      }
      return new Dataset(obj_name, dataobjs, this);
    } else {
      const new_group = new Group(obj_name, dataobjs, this);
      return new_group.get(additional_obj);
    }
  }

  /**
   * Recursively visit all names in the group and subgroups.
   */
  visit(func: (name: string) => unknown): unknown {
    return this.visititems((name, _obj) => func(name));
  }

  /**
   * Recursively visit all objects in this group and subgroups.
   */
  visititems(func: (name: string, obj: Group | Dataset) => unknown): unknown {
    let root_name_length = this.name.length;
    if (!/\/$/.test(this.name)) {
      root_name_length += 1;
    }

    let queue = this.values.slice();
    while (queue.length > 0) {
      const obj = queue.shift()!;
      const name = obj.name.slice(root_name_length);
      const ret = func(name, obj);
      if (ret != null) {
        return ret;
      }
      if (obj instanceof Group) {
        queue = queue.concat(obj.values);
      }
    }
    return null;
  }

  get attrs(): Record<string, DataValue> {
    if (this._attrs == null) {
      this._attrs = this._dataobjects.get_attributes();
    }
    return this._attrs;
  }
}

// ============================================================================
// File Class
// ============================================================================

/**
 * Open a HDF5 file.
 * In addition to file-specific methods, File also inherits the full interface of Group.
 */
export class File extends Group {
  _fh: ArrayBuffer;
  filename: string;
  mode: string;
  userblock_size: number;

  constructor(fh: ArrayBuffer, filename?: string) {
    const superblock = new SuperBlock(fh, 0);
    const offset = superblock.offset_to_dataobjects;
    const dataobjects = new DataObjects(fh, offset);
    super('/', dataobjects, null);
    this.parent = this;

    this._fh = fh;
    this.filename = filename || '';

    this.file = this;
    this.mode = 'r';
    this.userblock_size = 0;
  }

  _get_object_by_address(obj_addr: number): Group | Dataset | null {
    if (this._dataobjects.offset === obj_addr) {
      return this;
    }
    return this.visititems((_name, y) => {
      const dataobjects = (y as { _dataobjects?: DataObjects })._dataobjects;
      return dataobjects?.offset === obj_addr ? y : null;
    }) as Group | Dataset | null;
  }
}

// ============================================================================
// Dataset Class
// ============================================================================

/**
 * A HDF5 Dataset containing an n-dimensional array and meta-data attributes.
 */
export class Dataset extends Array<DataValue> {
  parent: Group;
  file: File;
  name: string;
  _dataobjects: DataObjects;
  protected _attrs: Record<string, DataValue> | null;
  protected _astype: string | null;

  constructor(name: string, dataobjects: DataObjects, parent: Group) {
    super();
    this.parent = parent;
    this.file = parent.file;
    this.name = name;

    this._dataobjects = dataobjects;
    this._attrs = null;
    this._astype = null;
  }

  get value(): DataValue[] {
    const data = this._dataobjects.get_data();
    if (this._astype == null) {
      return data;
    }
    // Type conversion not implemented
    return data;
  }

  get shape(): number[] {
    return this._dataobjects.shape;
  }

  get attrs(): Record<string, DataValue> {
    return this._dataobjects.get_attributes();
  }

  get dtype(): Dtype {
    return this._dataobjects.dtype;
  }

  get fillvalue(): DataValue {
    return this._dataobjects.fillvalue;
  }
}

// ============================================================================
// Async API Types
// ============================================================================

export interface OpenFileOptions {
  lazy?: boolean;
  cacheSize?: number;
  cacheBlockSize?: number;
  filename?: string;
  initialMetadataSize?: number;
  fetchOptions?: RequestInit;
  /** Enable debug logging */
  debug?: boolean;
}

// ============================================================================
// openFile Function
// ============================================================================

/**
 * Open an HDF5 file with lazy loading support
 *
 * @param source - Data source (ArrayBuffer, Blob, File, Response, URL, or file path)
 * @param options - Options
 * @returns The opened file
 */
export async function openFile(
  source: ArrayBuffer | Blob | Response | string,
  options: OpenFileOptions = {}
): Promise<AsyncFile> {
  let fileSource: IFileSource;

  // Extract HTTP-specific options
  const httpOptions = { fetchOptions: options.fetchOptions };

  if (source instanceof ArrayBuffer) {
    fileSource = new ArrayBufferSource(source);
  } else if (typeof Blob !== 'undefined' && source instanceof Blob) {
    fileSource = new BlobSource(source);
  } else if (typeof Response !== 'undefined' && source instanceof Response) {
    if (options.lazy && source.url) {
      fileSource = await new HTTPSource(source.url, httpOptions).init();
    } else {
      const buffer = await source.arrayBuffer();
      fileSource = new ArrayBufferSource(buffer);
    }
  } else if (typeof source === 'string') {
    if (source.startsWith('http://') || source.startsWith('https://')) {
      fileSource = await new HTTPSource(source, httpOptions).init();
    } else {
      try {
        const { NodeFileSource } = await import('./node-file-source.js');
        fileSource = await new NodeFileSource(source).init();
      } catch (_e) {
        throw new Error(`Cannot open file path in browser environment: ${source}`);
      }
    }
  } else {
    throw new Error('Unsupported source type');
  }

  if (options.cacheSize && options.cacheSize > 0) {
    fileSource = new CachedFileSource(fileSource, {
      cacheSize: options.cacheSize,
      blockSize: options.cacheBlockSize || 64 * 1024,
    });
  }

  return await AsyncFile.open(fileSource, options);
}

// ============================================================================
// AsyncGroup Class
// ============================================================================

/**
 * Async Group class for lazy loading
 */
export class AsyncGroup extends Group {
  protected _source: IFileSource;
  protected _linksInitialized: boolean;
  protected _debug: boolean;

  constructor(
    name: string,
    dataobjects: AsyncDataObjects,
    parent: AsyncGroup | null,
    source: IFileSource,
    debug: boolean = false
  ) {
    super(name, dataobjects, parent, false, true);
    this._source = source;
    this._linksInitialized = false;
    this._debug = debug;
  }

  /**
   * Initialize links asynchronously with auto-buffer expansion
   */
  async initLinks(): Promise<void> {
    if (this._linksInitialized) return;

    const maxRetries = 10;
    let retries = 0;

    while (retries < maxRetries) {
      try {
        if ('get_links_async' in this._dataobjects) {
          this._links = await (this._dataobjects as AsyncDataObjects).get_links_async();
        } else {
          this._links = this._dataobjects.get_links();
        }
        this._linksInitialized = true;
        return;
      } catch (e) {
        const err = e as Error;
        const isRangeError =
          e instanceof RangeError ||
          (err.message && (err.message.includes('bounds') || err.message.includes('Offset')));

        if (isRangeError && (this.file as AsyncFile)._source && (this.file as AsyncFile)._fh) {
          const currentSize = (this.file as AsyncFile)._fh.byteLength;
          const sourceSize = (this.file as AsyncFile)._source.size;

          if (currentSize >= sourceSize) {
            throw e;
          }

          const newSize = Math.min(sourceSize, currentSize * 4);
          debugLog(
            this._debug,
            `tsfive: Expanding metadata buffer from ${(currentSize / 1024 / 1024).toFixed(1)}MB to ${(newSize / 1024 / 1024).toFixed(1)}MB`
          );

          const newBuffer = await (this.file as AsyncFile)._source.read(0, newSize);
          (this.file as AsyncFile)._fh = newBuffer;

          this._dataobjects = new AsyncDataObjects(
            newBuffer,
            this._dataobjects.offset,
            this._source,
            this._debug
          );
          retries++;
        } else {
          throw e;
        }
      }
    }

    throw new Error('Failed to initialize links after maximum retries');
  }

  /**
   * Ensure links are initialized before access
   */
  async getLinksAsync(): Promise<Links> {
    await this.initLinks();
    return this._links!;
  }

  /**
   * Get a child object asynchronously
   */
  async getAsync(y: string | number): Promise<AsyncGroup | AsyncDataset | AsyncFile | null> {
    await this.initLinks();
    return this._getInternalAsync(y);
  }

  /**
   * Internal async get implementation
   */
  protected async _getInternalAsync(
    y: string | number
  ): Promise<AsyncGroup | AsyncDataset | AsyncFile | null> {
    if (typeof y === 'number') {
      return this._dereference(y) as AsyncGroup | AsyncDataset;
    }

    const path = normpath(y);
    if (path === '/') {
      return this.file as AsyncFile;
    }

    if (path === '.') {
      return this as AsyncGroup;
    }
    if (/^\//.test(path)) {
      return (this.file as AsyncFile)._getInternalAsync(path.slice(1));
    }

    let next_obj: string;
    let additional_obj: string;

    if (posix_dirname(path) !== '') {
      [next_obj, additional_obj] = path.split(/\/(.*)/);
    } else {
      next_obj = path;
      additional_obj = '.';
    }

    if (!(next_obj in (this._links || {}))) {
      throw new Error(next_obj + ' not found in group');
    }

    const obj_name = normpath(this.name + '/' + next_obj);
    const link_target = this._links![next_obj];

    if (typeof link_target === 'string') {
      try {
        return this._getInternalAsync(link_target);
      } catch (_error) {
        return null;
      }
    }

    const dataobjs = await AsyncDataObjects.createAsync(
      (this.file as AsyncFile)._fh,
      link_target as number,
      this._source,
      this._debug
    );
    if (dataobjs.is_dataset) {
      if (additional_obj !== '.') {
        throw new Error(obj_name + ' is a dataset, not a group');
      }
      return new AsyncDataset(obj_name, dataobjs, this, this._source, this._debug);
    } else {
      const new_group = new AsyncGroup(obj_name, dataobjs, this, this._source, this._debug);
      if (additional_obj === '.') {
        return new_group;
      }
      await new_group.initLinks();
      return new_group._getInternalAsync(additional_obj);
    }
  }

  /**
   * Synchronous get (requires links to be initialized)
   */
  override get(y: string | number): AsyncGroup | AsyncDataset | AsyncFile | null {
    if (!this._linksInitialized) {
      throw new Error('Links not initialized. Use getAsync() for async access.');
    }
    return this._getInternal(y);
  }

  /**
   * Internal get implementation
   */
  protected _getInternal(y: string | number): AsyncGroup | AsyncDataset | AsyncFile | null {
    if (typeof y === 'number') {
      return this._dereference(y) as AsyncGroup | AsyncDataset;
    }

    const path = normpath(y);
    if (path === '/') {
      return this.file as AsyncFile;
    }

    if (path === '.') {
      return this as AsyncGroup;
    }
    if (/^\//.test(path)) {
      return (this.file as AsyncFile)._getInternal(path.slice(1));
    }

    let next_obj: string;
    let additional_obj: string;

    if (posix_dirname(path) !== '') {
      [next_obj, additional_obj] = path.split(/\/(.*)/);
    } else {
      next_obj = path;
      additional_obj = '.';
    }

    if (!(next_obj in (this._links || {}))) {
      throw new Error(next_obj + ' not found in group');
    }

    const obj_name = normpath(this.name + '/' + next_obj);
    const link_target = this._links![next_obj];

    if (typeof link_target === 'string') {
      try {
        return this._getInternal(link_target);
      } catch (_error) {
        return null;
      }
    }

    const dataobjs = new AsyncDataObjects(
      (this.file as AsyncFile)._fh,
      link_target as number,
      this._source,
      this._debug
    );
    if (dataobjs.is_dataset) {
      if (additional_obj !== '.') {
        throw new Error(obj_name + ' is a dataset, not a group');
      }
      return new AsyncDataset(obj_name, dataobjs, this, this._source, this._debug);
    } else {
      const new_group = new AsyncGroup(obj_name, dataobjs, this, this._source, this._debug);
      if (additional_obj === '.') {
        return new_group;
      }
      throw new Error('For nested async paths, use getAsync()');
    }
  }
}

// ============================================================================
// AsyncFile Class
// ============================================================================

/**
 * Async File class with lazy loading support
 */
export class AsyncFile extends AsyncGroup {
  _fh: ArrayBuffer;
  _superblock: SuperBlock;
  filename: string;
  mode: string;
  userblock_size: number;

  constructor(
    source: IFileSource,
    superblock: SuperBlock,
    dataobjects: AsyncDataObjects,
    filename?: string,
    debug: boolean = false
  ) {
    super('/', dataobjects, null, source, debug);
    this.parent = this;
    this.file = this as unknown as File;
    this._source = source;
    this._fh = null!; // Will be set after construction
    this._superblock = superblock;
    this.filename = filename || '';
    this.mode = 'r';
    this.userblock_size = 0;
  }

  /**
   * Open a file asynchronously
   */
  static async open(fileSource: IFileSource, options: OpenFileOptions = {}): Promise<AsyncFile> {
    const initialMetadataSize = options.initialMetadataSize || 64 * 1024;
    const debug = options.debug ?? false;

    let metadataSize = Math.min(initialMetadataSize, fileSource.size);
    let metadataBuffer = await fileSource.read(0, metadataSize);

    const superblock = new SuperBlock(metadataBuffer, 0);
    const offset = superblock.offset_to_dataobjects;

    const dataobjects = new AsyncDataObjects(metadataBuffer, offset, fileSource, debug);

    const file = new AsyncFile(fileSource, superblock, dataobjects, options.filename, debug);
    file._fh = metadataBuffer;

    const maxRetries = 10;
    let retries = 0;

    while (retries < maxRetries) {
      try {
        file._linksInitialized = false;
        await file.initLinks();
        break;
      } catch (e) {
        const err = e as Error;
        const isRangeError =
          e instanceof RangeError ||
          (err.message && (err.message.includes('bounds') || err.message.includes('Offset')));

        if (isRangeError && metadataSize < fileSource.size) {
          const newSize = Math.min(fileSource.size, metadataSize * 4);
          if (newSize <= metadataSize) {
            throw e;
          }
          debugLog(
            debug,
            `tsfive: Expanding metadata buffer from ${(metadataSize / 1024 / 1024).toFixed(1)}MB to ${(newSize / 1024 / 1024).toFixed(1)}MB`
          );
          metadataSize = newSize;
          metadataBuffer = await fileSource.read(0, metadataSize);
          file._fh = metadataBuffer;
          file._dataobjects = new AsyncDataObjects(metadataBuffer, offset, fileSource, debug);
          retries++;
        } else {
          throw e;
        }
      }
    }

    if (retries >= maxRetries) {
      throw new Error('Failed to initialize file after maximum retries');
    }

    return file;
  }

  /**
   * Close the file and release resources
   */
  async close(): Promise<void> {
    if (this._source && 'close' in this._source) {
      await (this._source as IFileSource & { close(): Promise<void> }).close();
    }
  }

  _get_object_by_address(obj_addr: number): AsyncGroup | AsyncDataset | null {
    if (this._dataobjects.offset === obj_addr) {
      return this;
    }
    return this.visititems((_name, y) => {
      const dataobjects = (y as { _dataobjects?: DataObjects })._dataobjects;
      return dataobjects?.offset === obj_addr ? y : null;
    }) as AsyncGroup | AsyncDataset | null;
  }
}

// ============================================================================
// AsyncDataset Class
// ============================================================================

export interface ChunkResult {
  data: DataValue[];
  offset: number;
  size: number;
  isLast: boolean;
}

/**
 * Async Dataset with partial read support
 */
export class AsyncDataset extends Dataset {
  protected _source: IFileSource;
  protected _debug: boolean;

  constructor(
    name: string,
    dataobjects: AsyncDataObjects,
    parent: AsyncGroup,
    source: IFileSource,
    debug: boolean = false
  ) {
    super(name, dataobjects, parent);
    this._source = source;
    this._debug = debug;
  }

  /**
   * Read a slice of data asynchronously (1D indexing)
   */
  async sliceAsync(start: number, end: number): Promise<DataValue[]> {
    return await (this._dataobjects as AsyncDataObjects).getDataSliceAsync(start, end);
  }

  /**
   * Read data with options
   */
  async read(
    options: { start?: number; end?: number; slice?: number[][] } = {}
  ): Promise<DataValue[]> {
    if (options.slice) {
      // Multi-dimensional slice not implemented yet
      throw new Error('Multi-dimensional slice not implemented');
    }
    const totalSize = this.shape.reduce((a, b) => a * b, 1);
    const start = options.start ?? 0;
    const end = options.end ?? totalSize;
    return await this.sliceAsync(start, end);
  }

  /**
   * Load the entire dataset asynchronously
   */
  async valueAsync(): Promise<DataValue[]> {
    const totalSize = this.shape.reduce((a, b) => a * b, 1);
    return await this.sliceAsync(0, totalSize);
  }

  /**
   * Iterate over the dataset in chunks
   */
  async *iterChunks(options: { chunkSize?: number } = {}): AsyncGenerator<ChunkResult> {
    const chunkSize = options.chunkSize || 100000;
    const totalSize = this.shape.reduce((a, b) => a * b, 1);

    for (let start = 0; start < totalSize; start += chunkSize) {
      const end = Math.min(start + chunkSize, totalSize);
      const data = await this.sliceAsync(start, end);

      yield {
        data,
        offset: start,
        size: end - start,
        isLast: end >= totalSize,
      };
    }
  }

  /**
   * Get a ReadableStream of the dataset
   */
  stream(options: { chunkSize?: number } = {}): ReadableStream<ChunkResult> {
    // eslint-disable-next-line @typescript-eslint/no-this-alias -- needed for ReadableStream callback scope
    const dataset = this;
    const chunkSize = options.chunkSize || 100000;
    const totalSize = this.shape.reduce((a, b) => a * b, 1);
    let offset = 0;

    return new ReadableStream({
      async pull(controller) {
        if (offset >= totalSize) {
          controller.close();
          return;
        }

        const end = Math.min(offset + chunkSize, totalSize);
        const data = await dataset.sliceAsync(offset, end);
        controller.enqueue({
          data,
          offset,
          size: end - offset,
          isLast: end >= totalSize,
        });
        offset = end;
      },
    });
  }
}
