/**
 * Low-level HDF5 structures
 * SuperBlock, Heap, SymbolTable, GlobalHeap, FractalHeap
 */

import {
  _structure_size,
  _padded_size,
  _unpack_struct_from,
  struct,
  assert,
  _unpack_integer,
  bitSize,
} from './core.js';
import { debugLog } from './debug.js';
import type { StructureDefinition, UnpackedStruct } from './types/hdf5.js';
import type { IFileSource } from './types/file-source.js';

// ============================================================================
// Constants
// ============================================================================

const FORMAT_SIGNATURE = struct.unpack_from(
  '8s',
  new Uint8Array([137, 72, 68, 70, 13, 10, 26, 10]).buffer
)[0] as string;

const UNDEFINED_ADDRESS = struct.unpack_from(
  '<Q',
  new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255]).buffer
)[0] as number;

// ============================================================================
// Structure Definitions
// ============================================================================

// Version 0 SUPERBLOCK
const SUPERBLOCK_V0: StructureDefinition = new Map([
  ['format_signature', '8s'],
  ['superblock_version', 'B'],
  ['free_storage_version', 'B'],
  ['root_group_version', 'B'],
  ['reserved_0', 'B'],
  ['shared_header_version', 'B'],
  ['offset_size', 'B'],
  ['length_size', 'B'],
  ['reserved_1', 'B'],
  ['group_leaf_node_k', 'H'],
  ['group_internal_node_k', 'H'],
  ['file_consistency_flags', 'L'],
  ['base_address_lower', 'Q'],
  ['free_space_address', 'Q'],
  ['end_of_file_address', 'Q'],
  ['driver_information_address', 'Q'],
]);
const SUPERBLOCK_V0_SIZE = _structure_size(SUPERBLOCK_V0);

// Version 2/3 SUPERBLOCK
const SUPERBLOCK_V2_V3: StructureDefinition = new Map([
  ['format_signature', '8s'],
  ['superblock_version', 'B'],
  ['offset_size', 'B'],
  ['length_size', 'B'],
  ['file_consistency_flags', 'B'],
  ['base_address', 'Q'],
  ['superblock_extension_address', 'Q'],
  ['end_of_file_address', 'Q'],
  ['root_group_address', 'Q'],
  ['superblock_checksum', 'I'],
]);
const SUPERBLOCK_V2_V3_SIZE = _structure_size(SUPERBLOCK_V2_V3);

// Symbol Table Entry
const SYMBOL_TABLE_ENTRY: StructureDefinition = new Map([
  ['link_name_offset', 'Q'],
  ['object_header_address', 'Q'],
  ['cache_type', 'I'],
  ['reserved', 'I'],
  ['scratch', '16s'],
]);
const SYMBOL_TABLE_ENTRY_SIZE = _structure_size(SYMBOL_TABLE_ENTRY);

// Symbol Table Node
const SYMBOL_TABLE_NODE: StructureDefinition = new Map([
  ['signature', '4s'],
  ['version', 'B'],
  ['reserved_0', 'B'],
  ['symbols', 'H'],
]);
const SYMBOL_TABLE_NODE_SIZE = _structure_size(SYMBOL_TABLE_NODE);

// Local Heap
const LOCAL_HEAP: StructureDefinition = new Map([
  ['signature', '4s'],
  ['version', 'B'],
  ['reserved', '3s'],
  ['data_segment_size', 'Q'],
  ['offset_to_free_list', 'Q'],
  ['address_of_data_segment', 'Q'],
]);

// Global Heap Header
const GLOBAL_HEAP_HEADER: StructureDefinition = new Map([
  ['signature', '4s'],
  ['version', 'B'],
  ['reserved', '3s'],
  ['collection_size', 'Q'],
]);
const GLOBAL_HEAP_HEADER_SIZE = _structure_size(GLOBAL_HEAP_HEADER);

// Global Heap Object
const GLOBAL_HEAP_OBJECT: StructureDefinition = new Map([
  ['object_index', 'H'],
  ['reference_count', 'H'],
  ['reserved', 'I'],
  ['object_size', 'Q'],
]);
const GLOBAL_HEAP_OBJECT_SIZE = _structure_size(GLOBAL_HEAP_OBJECT);

// Fractal Heap Header
const FRACTAL_HEAP_HEADER: StructureDefinition = new Map([
  ['signature', '4s'],
  ['version', 'B'],
  ['object_index_size', 'H'],
  ['filter_info_size', 'H'],
  ['flags', 'B'],
  ['max_managed_object_size', 'I'],
  ['next_huge_object_index', 'Q'],
  ['btree_address_huge_objects', 'Q'],
  ['managed_freespace_size', 'Q'],
  ['freespace_manager_address', 'Q'],
  ['managed_space_size', 'Q'],
  ['managed_alloc_size', 'Q'],
  ['next_directblock_iterator_address', 'Q'],
  ['managed_object_count', 'Q'],
  ['huge_objects_total_size', 'Q'],
  ['huge_object_count', 'Q'],
  ['tiny_objects_total_size', 'Q'],
  ['tiny_object_count', 'Q'],
  ['table_width', 'H'],
  ['starting_block_size', 'Q'],
  ['maximum_direct_block_size', 'Q'],
  ['log2_maximum_heap_size', 'H'],
  ['indirect_starting_rows_count', 'H'],
  ['root_block_address', 'Q'],
  ['indirect_current_rows_count', 'H'],
]);

// ============================================================================
// SuperBlock Class
// ============================================================================

export class SuperBlock {
  version: number;
  private _contents: UnpackedStruct;
  private _end_of_sblock: number;
  private _root_symbol_table: SymbolTable | null;
  private _fh: ArrayBuffer;

  constructor(fh: ArrayBuffer, offset: number) {
    const versionHint = struct.unpack_from('<B', fh, offset + 8)[0] as number;
    let contents: UnpackedStruct;

    if (versionHint === 0) {
      contents = _unpack_struct_from(SUPERBLOCK_V0, fh, offset);
      this._end_of_sblock = offset + SUPERBLOCK_V0_SIZE;
    } else if (versionHint === 2 || versionHint === 3) {
      contents = _unpack_struct_from(SUPERBLOCK_V2_V3, fh, offset);
      this._end_of_sblock = offset + SUPERBLOCK_V2_V3_SIZE;
    } else {
      throw new Error('unsupported superblock version: ' + versionHint.toFixed());
    }

    // Verify contents
    if (contents.get('format_signature') !== FORMAT_SIGNATURE) {
      throw new Error('Incorrect file signature: ' + contents.get('format_signature'));
    }
    if (contents.get('offset_size') !== 8 || contents.get('length_size') !== 8) {
      throw new Error('File uses non-64-bit addressing');
    }

    this.version = contents.get('superblock_version') as number;
    this._contents = contents;
    this._root_symbol_table = null;
    this._fh = fh;
  }

  get offset_to_dataobjects(): number {
    if (this.version === 0) {
      const symTable = new SymbolTable(this._fh, this._end_of_sblock, true);
      this._root_symbol_table = symTable;
      return symTable.group_offset!;
    } else if (this.version === 2 || this.version === 3) {
      return this._contents.get('root_group_address') as number;
    } else {
      throw new Error('Not implemented version = ' + this.version.toFixed());
    }
  }
}

// ============================================================================
// Heap Class
// ============================================================================

export class Heap {
  private _contents: UnpackedStruct;
  data: ArrayBuffer;

  /**
   * Get heap data location without reading the data
   */
  static getDataLocation(
    fh: ArrayBuffer,
    offset: number
  ): { dataOffset: number; dataSize: number } {
    const localHeap = _unpack_struct_from(LOCAL_HEAP, fh, offset);
    return {
      dataOffset: localHeap.get('address_of_data_segment') as number,
      dataSize: localHeap.get('data_segment_size') as number,
    };
  }

  /**
   * Create a Heap with async data loading
   */
  static async createAsync(
    fh: ArrayBuffer,
    offset: number,
    source: IFileSource,
    debug: boolean = false
  ): Promise<Heap> {
    const localHeap = _unpack_struct_from(LOCAL_HEAP, fh, offset);
    assert(localHeap.get('signature') === 'HEAP');
    assert(localHeap.get('version') === 0);

    const dataOffset = localHeap.get('address_of_data_segment') as number;
    const dataSize = localHeap.get('data_segment_size') as number;

    let heapData: ArrayBuffer;
    if (dataOffset + dataSize <= fh.byteLength) {
      // Data is in buffer
      heapData = fh.slice(dataOffset, dataOffset + dataSize);
    } else if (source) {
      // Load data from source
      debugLog(
        debug,
        `tsfive: Loading heap data ${dataOffset}-${dataOffset + dataSize} (${dataSize} bytes)`
      );
      heapData = await source.read(dataOffset, dataOffset + dataSize);
    } else {
      throw new Error('Heap data is outside buffer and no source provided');
    }

    localHeap.set('heap_data', heapData);

    const heap = Object.create(Heap.prototype) as Heap;
    heap._contents = localHeap;
    heap.data = heapData;
    return heap;
  }

  constructor(fh: ArrayBuffer, offset: number) {
    const localHeap = _unpack_struct_from(LOCAL_HEAP, fh, offset);
    assert(localHeap.get('signature') === 'HEAP');
    assert(localHeap.get('version') === 0);

    const dataOffset = localHeap.get('address_of_data_segment') as number;
    const dataSize = localHeap.get('data_segment_size') as number;

    const heapData = fh.slice(dataOffset, dataOffset + dataSize);
    localHeap.set('heap_data', heapData);
    this._contents = localHeap;
    this.data = heapData;
  }

  get_object_name(offset: number): string {
    const end = new Uint8Array(this.data).indexOf(0, offset);
    const nameSize = end - offset;
    const name = struct.unpack_from('<' + nameSize.toFixed() + 's', this.data, offset)[0] as string;
    return name;
  }
}

// ============================================================================
// SymbolTable Class
// ============================================================================

export class SymbolTable {
  entries: UnpackedStruct[];
  group_offset?: number;
  private _contents: UnpackedStruct;

  constructor(fh: ArrayBuffer, offset: number, root: boolean = false) {
    let node: UnpackedStruct;

    if (root) {
      // The root symbol table has no Symbol table node header
      // and contains only a single entry
      node = new Map([['symbols', 1]]);
    } else {
      node = _unpack_struct_from(SYMBOL_TABLE_NODE, fh, offset);
      if (node.get('signature') !== 'SNOD') {
        throw new Error('incorrect node type');
      }
      offset += SYMBOL_TABLE_NODE_SIZE;
    }

    const entries: UnpackedStruct[] = [];
    const nSymbols = node.get('symbols') as number;

    for (let i = 0; i < nSymbols; i++) {
      entries.push(_unpack_struct_from(SYMBOL_TABLE_ENTRY, fh, offset));
      offset += SYMBOL_TABLE_ENTRY_SIZE;
    }

    if (root) {
      this.group_offset = entries[0].get('object_header_address') as number;
    }

    this.entries = entries;
    this._contents = node;
  }

  assign_name(heap: Heap): void {
    this.entries.forEach((entry) => {
      const offset = entry.get('link_name_offset') as number;
      const linkName = heap.get_object_name(offset);
      entry.set('link_name', linkName);
    });
  }

  get_links(heap: Heap): Record<string, string | number> {
    const links: Record<string, string | number> = {};

    this.entries.forEach((e) => {
      const cacheType = e.get('cache_type') as number;
      const linkName = e.get('link_name') as string;

      if (cacheType === 0 || cacheType === 1) {
        links[linkName] = e.get('object_header_address') as number;
      } else if (cacheType === 2) {
        const scratch = e.get('scratch') as string;
        const buf = new ArrayBuffer(4);
        const bufView = new Uint8Array(buf);
        for (let i = 0; i < 4; i++) {
          bufView[i] = scratch.charCodeAt(i);
        }
        const offset = struct.unpack_from('<I', buf, 0)[0] as number;
        links[linkName] = heap.get_object_name(offset);
      }
    });

    return links;
  }
}

// ============================================================================
// GlobalHeap Class
// ============================================================================

export class GlobalHeap {
  heap_data: ArrayBuffer;
  private _header: UnpackedStruct;
  private _objects: Map<number, ArrayBuffer> | null;

  constructor(fh: ArrayBuffer, offset: number) {
    const header = _unpack_struct_from(GLOBAL_HEAP_HEADER, fh, offset);
    offset += GLOBAL_HEAP_HEADER_SIZE;

    const heapDataSize = (header.get('collection_size') as number) - GLOBAL_HEAP_HEADER_SIZE;
    const heapData = fh.slice(offset, offset + heapDataSize);

    this.heap_data = heapData;
    this._header = header;
    this._objects = null;
  }

  get objects(): Map<number, ArrayBuffer> {
    if (this._objects === null) {
      this._objects = new Map();
      let offset = 0;

      while (offset <= this.heap_data.byteLength - GLOBAL_HEAP_OBJECT_SIZE) {
        const info = _unpack_struct_from(GLOBAL_HEAP_OBJECT, this.heap_data, offset);

        if ((info.get('object_index') as number) === 0) {
          break;
        }

        offset += GLOBAL_HEAP_OBJECT_SIZE;
        const objSize = info.get('object_size') as number;
        const objData = this.heap_data.slice(offset, offset + objSize);
        this._objects.set(info.get('object_index') as number, objData);
        offset += _padded_size(objSize);
      }
    }

    return this._objects;
  }
}

// ============================================================================
// FractalHeap Class
// ============================================================================

export class FractalHeap {
  fh: ArrayBuffer;
  header: UnpackedStruct;
  nobjects: number;
  managed: ArrayBuffer;

  private indirect_block_header: StructureDefinition;
  private indirect_block_header_size: number;
  private direct_block_header: StructureDefinition;
  private direct_block_header_size: number;
  private _managed_object_offset_size: number;
  private _managed_object_length_size: number;
  private _max_direct_nrows: number;
  private _indirect_nrows_sub: number;

  constructor(fh: ArrayBuffer, offset: number) {
    this.fh = fh;
    const header = _unpack_struct_from(FRACTAL_HEAP_HEADER, fh, offset);
    offset += _structure_size(FRACTAL_HEAP_HEADER);

    assert(header.get('signature') === 'FRHP');
    assert(header.get('version') === 0);

    if ((header.get('filter_info_size') as number) > 0) {
      throw new Error('Filter info size not supported on FractalHeap');
    }

    if ((header.get('btree_address_huge_objects') as number) === UNDEFINED_ADDRESS) {
      header.set('btree_address_huge_objects', null);
    } else {
      throw new Error('Huge objects not implemented in FractalHeap');
    }

    if ((header.get('root_block_address') as number) === UNDEFINED_ADDRESS) {
      header.set('root_block_address', null);
    }

    const nbits = header.get('log2_maximum_heap_size') as number;
    const blockOffsetSize = this._min_size_nbits(nbits);

    const h: StructureDefinition = new Map([
      ['signature', '4s'],
      ['version', 'B'],
      ['heap_header_adddress', 'Q'],
      ['block_offset', `${blockOffsetSize}B`],
    ]);

    this.indirect_block_header = new Map(h);
    this.indirect_block_header_size = _structure_size(h);

    if (((header.get('flags') as number) & 2) === 2) {
      h.set('checksum', 'I');
    }

    this.direct_block_header = h;
    this.direct_block_header_size = _structure_size(h);

    const maximumDblockSize = header.get('maximum_direct_block_size') as number;
    this._managed_object_offset_size = this._min_size_nbits(nbits);

    const value = Math.min(maximumDblockSize, header.get('max_managed_object_size') as number);
    this._managed_object_length_size = this._min_size_integer(value);

    const startBlockSize = header.get('starting_block_size') as number;
    const tableWidth = header.get('table_width') as number;

    if (!(startBlockSize > 0)) {
      throw new Error('Starting block size == 0 not implemented');
    }

    const log2MaximumDblockSize = Math.floor(Math.log2(maximumDblockSize));
    assert(1n << BigInt(log2MaximumDblockSize) === BigInt(maximumDblockSize));

    const log2StartBlockSize = Math.floor(Math.log2(startBlockSize));
    assert(1n << BigInt(log2StartBlockSize) === BigInt(startBlockSize));

    this._max_direct_nrows = log2MaximumDblockSize - log2StartBlockSize + 2;

    const log2TableWidth = Math.floor(Math.log2(tableWidth));
    assert(1 << log2TableWidth === tableWidth);
    this._indirect_nrows_sub = log2TableWidth + log2StartBlockSize - 1;

    this.header = header;
    this.nobjects =
      (header.get('managed_object_count') as number) +
      (header.get('huge_object_count') as number) +
      (header.get('tiny_object_count') as number);

    const managed: ArrayBuffer[] = [];
    const rootAddress = header.get('root_block_address') as number | null;
    let nrows = 0;

    if (rootAddress !== null) {
      nrows = header.get('indirect_current_rows_count') as number;
    }

    if (nrows > 0) {
      for (const data of this._iter_indirect_block(fh, rootAddress!, nrows)) {
        managed.push(data);
      }
    } else if (rootAddress !== null) {
      const data = this._read_direct_block(fh, rootAddress, startBlockSize);
      managed.push(data);
    }

    const dataSize = managed.reduce((p, c) => p + c.byteLength, 0);
    const combined = new Uint8Array(dataSize);
    let moffset = 0;
    managed.forEach((m) => {
      combined.set(new Uint8Array(m), moffset);
      moffset += m.byteLength;
    });
    this.managed = combined.buffer;
  }

  private _read_direct_block(fh: ArrayBuffer, offset: number, blockSize: number): ArrayBuffer {
    const data = fh.slice(offset, offset + blockSize);
    const header = _unpack_struct_from(this.direct_block_header, data);
    assert(header.get('signature') === 'FHDB');
    return data;
  }

  get_data(heapid: ArrayBuffer): ArrayBuffer {
    const firstbyte = struct.unpack_from('<B', heapid, 0)[0] as number;

    const _reserved = firstbyte & 15; // bit 0-3
    const idtype = (firstbyte >> 4) & 3; // bit 4-5
    const version = firstbyte >> 6; // bit 6-7
    let dataOffset = 1;

    if (idtype === 0) {
      // managed
      assert(version === 0);
      let nbytes = this._managed_object_offset_size;
      const offset = _unpack_integer(nbytes, heapid, dataOffset);
      dataOffset += nbytes;

      nbytes = this._managed_object_length_size;
      const size = _unpack_integer(nbytes, heapid, dataOffset);

      return this.managed.slice(offset, offset + size);
    } else if (idtype === 1) {
      throw new Error('tiny objectID not supported in FractalHeap');
    } else if (idtype === 2) {
      throw new Error('huge objectID not supported in FractalHeap');
    } else {
      throw new Error('unknown objectID type in FractalHeap');
    }
  }

  private _min_size_integer(integer: number): number {
    return this._min_size_nbits(bitSize(integer));
  }

  private _min_size_nbits(nbits: number): number {
    return Math.ceil(nbits / 8);
  }

  private *_iter_indirect_block(
    fh: ArrayBuffer,
    offset: number,
    nrows: number
  ): Generator<ArrayBuffer> {
    const header = _unpack_struct_from(this.indirect_block_header, fh, offset);
    offset += this.indirect_block_header_size;
    assert(header.get('signature') === 'FHIB');

    const blockOffsetBytes = header.get('block_offset') as number[];
    // Equivalent to python int.from_bytes with byteorder="little"
    const blockOffset = blockOffsetBytes.reduce(
      (p: number, c: number, i: number) => p + (c << (i * 8)),
      0
    );
    header.set('block_offset', blockOffset);

    const [ndirect, nindirect] = this._indirect_info(nrows);

    const directBlocks: Array<[number, number]> = [];
    for (let i = 0; i < ndirect; i++) {
      const address = struct.unpack_from('<Q', fh, offset)[0] as number;
      offset += 8;
      if (address === UNDEFINED_ADDRESS) {
        break;
      }
      const blockSize = this._calc_block_size(i);
      directBlocks.push([address, blockSize]);
    }

    const indirectBlocks: Array<[number, number]> = [];
    for (let i = ndirect; i < ndirect + nindirect; i++) {
      const address = struct.unpack_from('<Q', fh, offset)[0] as number;
      offset += 8;
      if (address === UNDEFINED_ADDRESS) {
        break;
      }
      const blockSize = this._calc_block_size(i);
      const nrowsBlock = this._iblock_nrows_from_block_size(blockSize);
      indirectBlocks.push([address, nrowsBlock]);
    }

    for (const [address, blockSize] of directBlocks) {
      const obj = this._read_direct_block(fh, address, blockSize);
      yield obj;
    }

    for (const [address, nrowsBlock] of indirectBlocks) {
      for (const obj of this._iter_indirect_block(fh, address, nrowsBlock)) {
        yield obj;
      }
    }
  }

  private _calc_block_size(iblock: number): number {
    const row = Math.floor(iblock / (this.header.get('table_width') as number));
    return Math.pow(2, Math.max(row - 1, 0)) * (this.header.get('starting_block_size') as number);
  }

  private _iblock_nrows_from_block_size(blockSize: number): number {
    const log2BlockSize = Math.floor(Math.log2(blockSize));
    assert(Math.pow(2, log2BlockSize) === blockSize);
    return log2BlockSize - this._indirect_nrows_sub;
  }

  private _indirect_info(nrows: number): [number, number] {
    const tableWidth = this.header.get('table_width') as number;
    const nobjects = nrows * tableWidth;
    const ndirectMax = this._max_direct_nrows * tableWidth;

    let ndirect: number;
    let nindirect: number;

    if (nrows <= ndirectMax) {
      ndirect = nobjects;
      nindirect = 0;
    } else {
      ndirect = ndirectMax;
      nindirect = nobjects - ndirectMax;
    }

    return [ndirect, nindirect];
  }

  private _int_format(bytelength: number): string {
    return ['B', 'H', 'I', 'Q'][bytelength - 1];
  }
}

// Export constants for external use
export { UNDEFINED_ADDRESS, FORMAT_SIGNATURE };
