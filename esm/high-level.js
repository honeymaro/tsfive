import {DataObjects} from './dataobjects.js';
import {AsyncDataObjects} from './async-dataobjects.js';
import {SuperBlock} from './misc-low-level.js';
import {FileSource, ArrayBufferSource, BlobSource, HTTPSource} from './file-source.js';
import {LRUCache, CachedFileSource} from './cache.js';
export { Filters } from './filters.js';
export { FileSource, ArrayBufferSource, BlobSource, HTTPSource } from './file-source.js';
export { LRUCache, CachedFileSource } from './cache.js';

export class Group {
  /*
    An HDF5 Group which may hold attributes, datasets, or other groups.
    Attributes
    ----------
    attrs : dict
        Attributes for this group.
    name : str
        Full path to this group.
    file : File
        File instance where this group resides.
    parent : Group
        Group instance containing this group.
  */

  /**
   *
   *
   * @memberof Group
   * @member {Group|File} parent;
   * @member {File} file;
   * @member {string} name;
   * @member {DataObjects} _dataobjects;
   * @member {Object} _attrs;
   * @member {Array<string>} _keys;
   */
  // parent;
  // file;
  // name;
  // _links;
  // _dataobjects;
  // _attrs;
  // _keys;

  /**
   *
   * @param {string} name
   * @param {DataObjects} dataobjects
   * @param {Group} [parent]
   * @param {boolean} [getterProxy=false]
   * @param {boolean} [skipLinksInit=false] - Skip synchronous links initialization (for async subclasses)
   * @returns {Group}
   */
  constructor(name, dataobjects, parent, getterProxy=false, skipLinksInit=false) {
    if (parent == null) {
      this.parent = this;
      this.file = this;
    }
    else {
      this.parent = parent;
      this.file = parent.file;
    }
    this.name = name;

    this._links = skipLinksInit ? null : dataobjects.get_links();
    this._dataobjects = dataobjects;
    this._attrs = null;  // cached property
    this._keys = null;
    if (getterProxy) {
      return new Proxy(this, groupGetHandler);
    }
  }

  get keys() {
    if (this._keys == null) {
      this._keys = Object.keys(this._links);
    }
    return this._keys.slice();
  }

  get values() {
    return this.keys.map(k => this.get(k));
  }

  length() {
    return this.keys.length;
  }

  _dereference(ref) {
    //""" Deference a Reference object. """
    if (!ref) {
      throw 'cannot deference null reference';
    }
    let obj = this.file._get_object_by_address(ref);
    if (obj == null) {
      throw 'reference not found in file';
    }
    return obj
  }

  get(y) {
    //""" x.__getitem__(y) <==> x[y] """
    if (typeof(y) == 'number') {
      return this._dereference(y);
    }

    var path = normpath(y);
    if (path == '/') {
      return this.file;
    }

    if (path == '.') {
      return this
    }    
    if (/^\//.test(path)) {
      return this.file.get(path.slice(1));
    }

    if (posix_dirname(path) != '') {
      var [next_obj, additional_obj] = path.split(/\/(.*)/);
    }
    else {
      var next_obj = path;
      var additional_obj = '.'
    }
    if (!(next_obj in this._links)) {
      throw next_obj + ' not found in group';
    }

    var obj_name = normpath(this.name + '/' + next_obj);
    let link_target = this._links[next_obj];

    if (typeof(link_target) == "string") {
      try {
        return this.get(link_target)
      } catch (error) {
        return null
      } 
    }

    var dataobjs = new DataObjects(this.file._fh, link_target);
    if (dataobjs.is_dataset) {
      if (additional_obj != '.') {
        throw obj_name + ' is a dataset, not a group';
      }
      return new Dataset(obj_name, dataobjs, this);
    }
    else {
      var new_group = new Group(obj_name, dataobjs, this);
      return new_group.get(additional_obj);
    }
  }

  visit(func) {
    /*
    Recursively visit all names in the group and subgroups.
    func should be a callable with the signature:
        func(name) -> None or return value
    Returning None continues iteration, return anything else stops and
    return that value from the visit method.
    */
    return this.visititems((name, obj) => func(name));
  }

  visititems(func) {
    /*
    Recursively visit all objects in this group and subgroups.
    func should be a callable with the signature:
        func(name, object) -> None or return value
    Returning None continues iteration, return anything else stops and
    return that value from the visit method.
    */
    var root_name_length = this.name.length;
    if (!(/\/$/.test(this.name))) {
      root_name_length += 1;
    }
    //queue = deque(this.values())
    var queue = this.values.slice();
    while (queue) {
      let obj = queue.shift();
      if (queue.length == 1) console.log(obj);
      let name = obj.name.slice(root_name_length);
      let ret = func(name, obj);
      if (ret != null) {
        return ret
      }
      if (obj instanceof Group) {
        queue = queue.concat(obj.values);
      }
    }
    return null
  }

  get attrs() {
    //""" attrs attribute. """
    if (this._attrs == null) {
      this._attrs = this._dataobjects.get_attributes();
    }
    return this._attrs
  }

}

const groupGetHandler = {
  get: function(target, prop, receiver) {
    if (prop in target) {
      return target[prop];
    }
    return target.get(prop);
  }
};


export class File extends Group {
  /*
  Open a HDF5 file.
  Note in addition to having file specific methods the File object also
  inherit the full interface of **Group**.
  File is also a context manager and therefore supports the with statement.
  Files opened by the class will be closed after the with block, file-like
  object are not closed.
  Parameters
  ----------
  filename : str or file-like
      Name of file (string or unicode) or file like object which has read
      and seek methods which behaved like a Python file object.
  Attributes
  ----------
  filename : str
      Name of the file on disk, None if not available.
  mode : str
      String indicating that the file is open readonly ("r").
  userblock_size : int
      Size of the user block in bytes (currently always 0).
  */

  constructor (fh, filename) {
    //""" initalize. """
    //if hasattr(filename, 'read'):
    //    if not hasattr(filename, 'seek'):
    //        raise ValueError(
    //            'File like object must have a seek method')
    
    var superblock = new SuperBlock(fh, 0);
    var offset = superblock.offset_to_dataobjects;
    var dataobjects = new DataObjects(fh, offset);
    super('/', dataobjects, null);
    this.parent = this;

    this._fh = fh
    this.filename = filename || '';

    this.file = this;
    this.mode = 'r';
    this.userblock_size = 0;
  }

  _get_object_by_address(obj_addr) {
    //""" Return the object pointed to by a given address. """
    if (this._dataobjects.offset == obj_addr) {
      return this
    }
    return this.visititems(
      (y) => {(y._dataobjects.offset == obj_addr) ? y : null;}
    );
  }
}

export class Dataset extends Array {
  /*
  A HDF5 Dataset containing an n-dimensional array and meta-data attributes.
  Attributes
  ----------
  shape : tuple
      Dataset dimensions.
  dtype : dtype
      Dataset's type.
  size : int
      Total number of elements in the dataset.
  chunks : tuple or None
      Chunk shape, or NOne is chunked storage not used.
  compression : str or None
      Compression filter used on dataset.  None if compression is not enabled
      for this dataset.
  compression_opts : dict or None
      Options for the compression filter.
  scaleoffset : dict or None
      Setting for the HDF5 scale-offset filter, or None if scale-offset
      compression is not used for this dataset.
  shuffle : bool
      Whether the shuffle filter is applied for this dataset.
  fletcher32 : bool
      Whether the Fletcher32 checksumming is enabled for this dataset.
  fillvalue : float or None
      Value indicating uninitialized portions of the dataset. None is no fill
      values has been defined.
  dim : int
      Number of dimensions.
  dims : None
      Dimension scales.
  attrs : dict
      Attributes for this dataset.
  name : str
      Full path to this dataset.
  file : File
      File instance where this dataset resides.
  parent : Group
      Group instance containing this dataset.
  */

  /**
   *
   *
   * @memberof Dataset
   * @member {Group|File} parent;
   * @member {File} file;
   * @member {string} name;
   * @member {DataObjects} _dataobjects;
   * @member {Object} _attrs;
   * @member {string} _astype;
   */
  // parent;
  // file;
  // name;
  // _dataobjects;
  // _attrs;
  // _astype;

  constructor(name, dataobjects, parent) {
    //""" initalize. """
    super();
    this.parent = parent;
    this.file = parent.file
    this.name = name;

    this._dataobjects = dataobjects
    this._attrs = null;
    this._astype = null;
  }

  get value() {
    var data = this._dataobjects.get_data();
    if (this._astype == null) {
      return data
    }
    return data.astype(this._astype);
  }

  get shape() {
    return this._dataobjects.shape;
  }

  get attrs() {
    return this._dataobjects.get_attributes();
  }

  get dtype() {
    return this._dataobjects.dtype;
  }

  get fillvalue() {
    return this._dataobjects.fillvalue;
  }
}


function posix_dirname(p) {
  let sep = '/';
  let i = p.lastIndexOf(sep) + 1;
  let head = p.slice(0, i);
  let all_sep = new RegExp('^' + sep + '+$');
  let end_sep = new RegExp(sep + '$');
  if (head && !(all_sep.test(head))) {
    head = head.replace(end_sep, '');
  }
  return head
}

function normpath(path) {
  return path.replace(/\/(\/)+/g, '/');
  // path = posixpath.normpath(y)
}


// ============================================================================
// Async API for lazy loading large HDF5 files
// ============================================================================

/**
 * Open an HDF5 file with lazy loading support
 *
 * @param {ArrayBuffer|Blob|File|Response|string} source - Data source
 *   - ArrayBuffer: Complete file data (legacy mode)
 *   - Blob/File: Browser File API object
 *   - string: URL (http/https) or file path (Node.js)
 * @param {Object} [options] - Options
 * @param {boolean} [options.lazy=false] - Enable lazy loading (only load metadata initially)
 * @param {number} [options.cacheSize] - Cache size in bytes for LRU cache
 * @param {string} [options.filename] - Optional filename
 * @param {number} [options.initialMetadataSize=65536] - Initial metadata buffer size
 * @returns {Promise<AsyncFile>} The opened file
 */
export async function openFile(source, options = {}) {
  let fileSource;

  if (source instanceof ArrayBuffer) {
    fileSource = new ArrayBufferSource(source);
  } else if (typeof Blob !== 'undefined' && source instanceof Blob) {
    // Browser File extends Blob
    fileSource = new BlobSource(source);
  } else if (typeof Response !== 'undefined' && source instanceof Response) {
    // fetch Response - use URL for range requests if lazy
    if (options.lazy && source.url) {
      fileSource = await new HTTPSource(source.url, options).init();
    } else {
      // Load entire response
      const buffer = await source.arrayBuffer();
      fileSource = new ArrayBufferSource(buffer);
    }
  } else if (typeof source === 'string') {
    if (source.startsWith('http://') || source.startsWith('https://')) {
      // HTTP URL
      fileSource = await new HTTPSource(source, options).init();
    } else {
      // Assume Node.js file path - dynamically import NodeFileSource
      try {
        const { NodeFileSource } = await import('./node-file-source.js');
        fileSource = await new NodeFileSource(source).init();
      } catch (e) {
        throw new Error(`Cannot open file path in browser environment: ${source}`);
      }
    }
  } else {
    throw new Error('Unsupported source type');
  }

  // Wrap with caching if cacheSize is specified
  if (options.cacheSize && options.cacheSize > 0) {
    fileSource = new CachedFileSource(fileSource, {
      cacheSize: options.cacheSize,
      blockSize: options.cacheBlockSize || 64 * 1024
    });
  }

  return await AsyncFile.open(fileSource, options);
}


/**
 * Async Group class for lazy loading
 */
export class AsyncGroup extends Group {
  /**
   * @param {string} name
   * @param {AsyncDataObjects} dataobjects
   * @param {AsyncGroup} [parent]
   * @param {FileSource} source
   */
  constructor(name, dataobjects, parent, source) {
    // Skip synchronous links initialization - will be done async
    super(name, dataobjects, parent, false, true);
    this._source = source;
    this._linksInitialized = false;
  }

  /**
   * Initialize links asynchronously with auto-buffer expansion
   * @returns {Promise<void>}
   */
  async initLinks() {
    if (this._linksInitialized) return;

    const maxRetries = 10;
    let retries = 0;

    while (retries < maxRetries) {
      try {
        if (this._dataobjects.get_links_async) {
          this._links = await this._dataobjects.get_links_async();
        } else {
          this._links = this._dataobjects.get_links();
        }
        this._linksInitialized = true;
        return;
      } catch (e) {
        const isRangeError = e instanceof RangeError ||
          (e.message && (e.message.includes('bounds') || e.message.includes('Offset')));

        if (isRangeError && this.file._source && this.file._fh) {
          // Try to expand the file's metadata buffer
          const currentSize = this.file._fh.byteLength;
          const sourceSize = this.file._source.size;

          if (currentSize >= sourceSize) {
            throw e; // Already at max size
          }

          const newSize = Math.min(sourceSize, currentSize * 4);
          console.log(`jsfive: Expanding metadata buffer from ${(currentSize / 1024 / 1024).toFixed(1)}MB to ${(newSize / 1024 / 1024).toFixed(1)}MB`);

          const newBuffer = await this.file._source.read(0, newSize);
          this.file._fh = newBuffer;

          // Recreate dataobjects with new buffer
          this._dataobjects = new AsyncDataObjects(newBuffer, this._dataobjects.offset, this._source);
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
   * @returns {Promise<Object>}
   */
  async getLinksAsync() {
    await this.initLinks();
    return this._links;
  }

  /**
   * Get a child object asynchronously
   * @param {string} y - Path to child
   * @returns {Promise<AsyncGroup|AsyncDataset|null>}
   */
  async getAsync(y) {
    await this.initLinks();
    return this._getInternalAsync(y);
  }

  /**
   * Internal async get implementation - handles out-of-buffer offsets
   * @private
   */
  async _getInternalAsync(y) {
    if (typeof(y) == 'number') {
      return this._dereference(y);
    }

    var path = normpath(y);
    if (path == '/') {
      return this.file;
    }

    if (path == '.') {
      return this;
    }
    if (/^\//.test(path)) {
      return this.file._getInternalAsync(path.slice(1));
    }

    if (posix_dirname(path) != '') {
      var [next_obj, additional_obj] = path.split(/\/(.*)/);
    } else {
      var next_obj = path;
      var additional_obj = '.';
    }
    if (!(next_obj in this._links)) {
      throw next_obj + ' not found in group';
    }

    var obj_name = normpath(this.name + '/' + next_obj);
    let link_target = this._links[next_obj];

    if (typeof(link_target) == 'string') {
      try {
        return this._getInternalAsync(link_target);
      } catch (error) {
        return null;
      }
    }

    // Create AsyncDataObjects with async loading for out-of-buffer offsets
    var dataobjs = await AsyncDataObjects.createAsync(this.file._fh, link_target, this._source);
    if (dataobjs.is_dataset) {
      if (additional_obj != '.') {
        throw obj_name + ' is a dataset, not a group';
      }
      return new AsyncDataset(obj_name, dataobjs, this, this._source);
    } else {
      var new_group = new AsyncGroup(obj_name, dataobjs, this, this._source);
      if (additional_obj === '.') {
        return new_group;
      }
      // For nested paths, continue async traversal
      await new_group.initLinks();
      return new_group._getInternalAsync(additional_obj);
    }
  }

  /**
   * Synchronous get (for backward compatibility, but requires links to be initialized)
   * @param {string|number} y - Path or reference
   * @returns {AsyncGroup|AsyncDataset|null}
   */
  get(y) {
    if (!this._linksInitialized) {
      throw new Error('Links not initialized. Use getAsync() for async access.');
    }
    return this._getInternal(y);
  }

  /**
   * Internal get implementation
   * @private
   */
  _getInternal(y) {
    if (typeof(y) == 'number') {
      return this._dereference(y);
    }

    var path = normpath(y);
    if (path == '/') {
      return this.file;
    }

    if (path == '.') {
      return this;
    }
    if (/^\//.test(path)) {
      return this.file._getInternal(path.slice(1));
    }

    if (posix_dirname(path) != '') {
      var [next_obj, additional_obj] = path.split(/\/(.*)/);
    } else {
      var next_obj = path;
      var additional_obj = '.';
    }
    if (!(next_obj in this._links)) {
      throw next_obj + ' not found in group';
    }

    var obj_name = normpath(this.name + '/' + next_obj);
    let link_target = this._links[next_obj];

    if (typeof(link_target) == "string") {
      try {
        return this._getInternal(link_target);
      } catch (error) {
        return null;
      }
    }

    // Create AsyncDataObjects for lazy loading
    var dataobjs = new AsyncDataObjects(this.file._fh, link_target, this._source);
    if (dataobjs.is_dataset) {
      if (additional_obj != '.') {
        throw obj_name + ' is a dataset, not a group';
      }
      return new AsyncDataset(obj_name, dataobjs, this, this._source);
    } else {
      var new_group = new AsyncGroup(obj_name, dataobjs, this, this._source);
      // Note: new_group._links will be null, need to use getAsync for traversal
      if (additional_obj === '.') {
        return new_group;
      }
      // For nested paths, we'd need async - throw for now
      throw new Error('For nested async paths, use getAsync()');
    }
  }
}


/**
 * Async File class with lazy loading support
 */
export class AsyncFile extends AsyncGroup {
  /**
   * @param {FileSource} source - The file source
   * @param {SuperBlock} superblock - Parsed superblock
   * @param {AsyncDataObjects} dataobjects - Root data objects
   * @param {string} filename - Optional filename
   */
  constructor(source, superblock, dataobjects, filename) {
    super('/', dataobjects, null, source);
    this.parent = this;
    this.file = this;
    this._source = source;
    this._fh = null; // Will be set to cached buffer
    this._superblock = superblock;
    this.filename = filename || '';
    this.mode = 'r';
    this.userblock_size = 0;
  }

  /**
   * Open a file asynchronously
   * @param {FileSource} fileSource - The file source
   * @param {Object} options - Options
   * @returns {Promise<AsyncFile>}
   */
  static async open(fileSource, options = {}) {
    const initialMetadataSize = options.initialMetadataSize || 64 * 1024;

    // Read initial metadata (SuperBlock + root DataObjects)
    let metadataSize = Math.min(initialMetadataSize, fileSource.size);
    let metadataBuffer = await fileSource.read(0, metadataSize);

    // Parse SuperBlock
    const superblock = new SuperBlock(metadataBuffer, 0);
    const offset = superblock.offset_to_dataobjects;

    // Create AsyncDataObjects with the cached buffer
    let dataobjects = new AsyncDataObjects(metadataBuffer, offset, fileSource);

    const file = new AsyncFile(fileSource, superblock, dataobjects, options.filename);
    file._fh = metadataBuffer; // Cache the metadata buffer

    // Initialize root links asynchronously with auto-expand
    const maxRetries = 10;
    let retries = 0;

    while (retries < maxRetries) {
      try {
        file._linksInitialized = false; // Reset for retry
        await file.initLinks();
        break; // Success
      } catch (e) {
        const isRangeError = e instanceof RangeError ||
          (e.message && (e.message.includes('bounds') || e.message.includes('Offset')));

        if (isRangeError && metadataSize < fileSource.size) {
          // Buffer too small - expand and retry
          const newSize = Math.min(fileSource.size, metadataSize * 4);
          if (newSize <= metadataSize) {
            // Can't expand further
            throw e;
          }
          console.log(`jsfive: Expanding metadata buffer from ${(metadataSize / 1024 / 1024).toFixed(1)}MB to ${(newSize / 1024 / 1024).toFixed(1)}MB`);
          metadataSize = newSize;
          metadataBuffer = await fileSource.read(0, metadataSize);
          file._fh = metadataBuffer;
          file._dataobjects = new AsyncDataObjects(metadataBuffer, offset, fileSource);
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
   * @returns {Promise<void>}
   */
  async close() {
    if (this._source) {
      await this._source.close();
    }
  }

  _get_object_by_address(obj_addr) {
    if (this._dataobjects.offset == obj_addr) {
      return this;
    }
    return this.visititems(
      (y) => { (y._dataobjects.offset == obj_addr) ? y : null; }
    );
  }
}


/**
 * Async Dataset with partial read support
 */
export class AsyncDataset extends Dataset {
  /**
   * @param {string} name
   * @param {AsyncDataObjects} dataobjects
   * @param {AsyncGroup} parent
   * @param {FileSource} source
   */
  constructor(name, dataobjects, parent, source) {
    super(name, dataobjects, parent);
    this._source = source;
  }

  /**
   * Read a slice of data asynchronously (1D indexing)
   *
   * @param {number} start - Start index (inclusive)
   * @param {number} end - End index (exclusive)
   * @returns {Promise<Array>} The requested data
   */
  async sliceAsync(start, end) {
    return await this._dataobjects.getDataSliceAsync(start, end);
  }

  /**
   * Read data with options
   *
   * @param {Object} options - Read options
   * @param {number} [options.start] - Start index (1D)
   * @param {number} [options.end] - End index (1D)
   * @param {Array<Array<number>>} [options.slice] - Multi-dimensional slice [[start, end], ...]
   * @returns {Promise<Array>} The requested data
   */
  async read(options = {}) {
    if (options.slice) {
      return await this._dataobjects.getDataMultiSliceAsync(options.slice);
    }
    const totalSize = this.shape.reduce((a, b) => a * b, 1);
    const start = options.start ?? 0;
    const end = options.end ?? totalSize;
    return await this.sliceAsync(start, end);
  }

  /**
   * Load the entire dataset asynchronously
   *
   * @returns {Promise<Array>} The complete data
   */
  async valueAsync() {
    const totalSize = this.shape.reduce((a, b) => a * b, 1);
    return await this.sliceAsync(0, totalSize);
  }

  /**
   * Iterate over the dataset in chunks
   *
   * @param {Object} options - Iterator options
   * @param {number} [options.chunkSize=100000] - Number of elements per chunk
   * @yields {{data: Array, offset: number, size: number, isLast: boolean}}
   */
  async *iterChunks(options = {}) {
    const chunkSize = options.chunkSize || 100000;
    const totalSize = this.shape.reduce((a, b) => a * b, 1);

    for (let start = 0; start < totalSize; start += chunkSize) {
      const end = Math.min(start + chunkSize, totalSize);
      const data = await this.sliceAsync(start, end);

      yield {
        data,
        offset: start,
        size: end - start,
        isLast: end >= totalSize
      };
    }
  }

  /**
   * Get a ReadableStream of the dataset
   *
   * @param {Object} options - Stream options
   * @param {number} [options.chunkSize=100000] - Number of elements per chunk
   * @returns {ReadableStream} A readable stream of data chunks
   */
  stream(options = {}) {
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
          isLast: end >= totalSize
        });
        offset = end;
      }
    });
  }
}
