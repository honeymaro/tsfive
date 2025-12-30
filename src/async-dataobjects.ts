/**
 * AsyncDataObjects - Separate module for async lazy loading support
 * Uses lazy BTree traversal to avoid OOM on large files
 */

import { _structure_size, _unpack_struct_from, struct, dtype_getter, DataView64 } from './core.js';
import { BTreeV1Groups } from './btree.js';
import { Heap, SymbolTable } from './misc-low-level.js';
import { Filters } from './filters.js';
import {
  DataObjects,
  SYMBOL_TABLE_MSG_TYPE,
  LINK_MSG_TYPE,
  LINK_INFO_MSG_TYPE,
  DATA_STORAGE_MSG_TYPE,
  OBJECT_CONTINUATION_MSG_TYPE,
  UNDEFINED_ADDRESS,
} from './dataobjects.js';
import type { IFileSource } from './types/file-source.js';
import type { StructureDefinition, UnpackedStruct, Links } from './types/hdf5.js';
import type { DataValue } from './types/dtype.js';

// ============================================================================
// Structure Definitions
// ============================================================================

const OBJECT_HEADER_V1: StructureDefinition = new Map([
  ['version', 'B'],
  ['reserved', 'B'],
  ['total_header_messages', 'H'],
  ['object_reference_count', 'I'],
  ['object_header_size', 'I'],
  ['padding', 'I'],
]);

const OBJECT_HEADER_V2: StructureDefinition = new Map([
  ['signature', '4s'],
  ['version', 'B'],
  ['flags', 'B'],
]);

const HEADER_MSG_INFO_V1: StructureDefinition = new Map([
  ['type', 'H'],
  ['size', 'H'],
  ['flags', 'B'],
  ['reserved', '3s'],
]);
const HEADER_MSG_INFO_V1_SIZE = _structure_size(HEADER_MSG_INFO_V1);

const HEADER_MSG_INFO_V2: StructureDefinition = new Map([
  ['type', 'B'],
  ['size', 'H'],
  ['flags', 'B'],
]);
const HEADER_MSG_INFO_V2_SIZE = _structure_size(HEADER_MSG_INFO_V2);

const SYMBOL_TABLE_MSG: StructureDefinition = new Map([
  ['btree_address', 'Q'],
  ['heap_address', 'Q'],
]);

// ============================================================================
// Types
// ============================================================================

interface ChunkInfo {
  address: number;
  offset: number[];
  size: number;
  filterMask: number;
}

interface BTreeEntry {
  chunkSize: number;
  filterMask: number;
  chunkOffset: number[];
  childAddr: number;
}

type Slice = [number, number];

// ============================================================================
// AsyncDataObjects Class
// ============================================================================

/**
 * Async DataObjects class for lazy loading support
 * KEY FEATURE: Uses lazy BTree traversal to avoid loading all nodes (OOM prevention)
 */
export class AsyncDataObjects extends DataObjects {
  protected _source: IFileSource;

  constructor(fh: ArrayBuffer, offset: number, source: IFileSource) {
    super(fh, offset);
    this._source = source;
  }

  /**
   * Async factory that pre-loads metadata including continuation blocks
   */
  static async createAsync(
    fh: ArrayBuffer,
    offset: number,
    source: IFileSource
  ): Promise<AsyncDataObjects> {
    console.log(`tsfive async: createAsync offset=${offset}, buffer=${fh.byteLength}`);

    let workingBuffer = fh;
    let workingOffset = offset;

    // If offset is outside buffer, load it first
    if (offset >= fh.byteLength) {
      const loadSize = 64 * 1024;
      const loadEnd = Math.min(offset + loadSize, source.size);
      console.log(`tsfive async: Loading DataObjects at ${offset}`);
      workingBuffer = await source.read(offset, loadEnd);
      workingOffset = 0;
    }

    const versionHint = struct.unpack_from('<B', workingBuffer, workingOffset)[0] as number;

    if (versionHint === 1) {
      workingBuffer = await AsyncDataObjects._loadV1WithContinuations(
        workingBuffer,
        workingOffset,
        source
      );
    } else if (versionHint === 79) {
      // 'O' = v2
      workingBuffer = await AsyncDataObjects._loadV2WithContinuations(
        workingBuffer,
        workingOffset,
        source
      );
    }

    return new AsyncDataObjects(workingBuffer, workingOffset, source);
  }

  private static async _loadV1WithContinuations(
    buffer: ArrayBuffer,
    offset: number,
    source: IFileSource
  ): Promise<ArrayBuffer> {
    const header = _unpack_struct_from(OBJECT_HEADER_V1, buffer, offset);
    const blockSize = header.get('object_header_size') as number;
    const blockOffset = offset + _structure_size(OBJECT_HEADER_V1);

    const blocksToProcess: Array<[number, number]> = [[blockOffset, blockSize]];
    const loadedBlocks = new Map<number, boolean>();
    let expandedBuffer = buffer;

    while (blocksToProcess.length > 0) {
      const [blkOffset, blkSize] = blocksToProcess.shift()!;

      if (blkOffset + blkSize > expandedBuffer.byteLength) {
        console.log(`tsfive async: Loading V1 block ${blkOffset}-${blkOffset + blkSize}`);
        const blockData = await source.read(blkOffset, blkOffset + blkSize);

        const newSize = Math.max(expandedBuffer.byteLength, blkOffset + blkSize);
        const newBuffer = new ArrayBuffer(newSize);
        new Uint8Array(newBuffer).set(new Uint8Array(expandedBuffer));
        new Uint8Array(newBuffer).set(new Uint8Array(blockData), blkOffset);
        expandedBuffer = newBuffer;
      }

      if (loadedBlocks.has(blkOffset)) continue;
      loadedBlocks.set(blkOffset, true);

      let localOffset = 0;
      while (localOffset < blkSize) {
        const msgOffset = blkOffset + localOffset;
        if (msgOffset + HEADER_MSG_INFO_V1_SIZE > expandedBuffer.byteLength) break;

        const msg = _unpack_struct_from(HEADER_MSG_INFO_V1, expandedBuffer, msgOffset);
        const dataOffset = msgOffset + HEADER_MSG_INFO_V1_SIZE;

        if (msg.get('type') === OBJECT_CONTINUATION_MSG_TYPE) {
          if (dataOffset + 16 <= expandedBuffer.byteLength) {
            const [contOff, contSize] = struct.unpack_from('<QQ', expandedBuffer, dataOffset) as [
              number,
              number,
            ];
            if (!loadedBlocks.has(Number(contOff))) {
              blocksToProcess.push([Number(contOff), Number(contSize)]);
            }
          }
        }

        localOffset += HEADER_MSG_INFO_V1_SIZE + (msg.get('size') as number);
        if (localOffset >= blkSize) break;
      }
    }

    return expandedBuffer;
  }

  private static async _loadV2WithContinuations(
    buffer: ArrayBuffer,
    offset: number,
    source: IFileSource
  ): Promise<ArrayBuffer> {
    const header = _unpack_struct_from(OBJECT_HEADER_V2, buffer, offset);
    const flags = header.get('flags') as number;
    const creationOrderSize = flags & 0b00000100 ? 2 : 0;

    let blockOffset = offset + _structure_size(OBJECT_HEADER_V2);
    if (flags & 0b00010000) blockOffset += 4;
    if (flags & 0b00100000) blockOffset += 4;

    const chunk0SizeLen = 1 << (flags & 0b00000011);
    const blockSize = struct.unpack_from(
      `<${['B', 'H', 'I', 'Q'][chunk0SizeLen - 1]}`,
      buffer,
      blockOffset
    )[0] as number;
    blockOffset += chunk0SizeLen;

    const msgHeaderSize = HEADER_MSG_INFO_V2_SIZE + creationOrderSize;

    const blocksToProcess: Array<[number, number]> = [[blockOffset, Number(blockSize)]];
    const loadedBlocks = new Map<number, boolean>();
    let expandedBuffer = buffer;

    while (blocksToProcess.length > 0) {
      const [blkOffset, blkSize] = blocksToProcess.shift()!;

      if (blkOffset + blkSize > expandedBuffer.byteLength) {
        console.log(`tsfive async: Loading V2 block ${blkOffset}-${blkOffset + blkSize}`);
        const blockData = await source.read(blkOffset, blkOffset + blkSize);

        const newSize = Math.max(expandedBuffer.byteLength, blkOffset + blkSize);
        const newBuffer = new ArrayBuffer(newSize);
        new Uint8Array(newBuffer).set(new Uint8Array(expandedBuffer));
        new Uint8Array(newBuffer).set(new Uint8Array(blockData), blkOffset);
        expandedBuffer = newBuffer;
      }

      if (loadedBlocks.has(blkOffset)) continue;
      loadedBlocks.set(blkOffset, true);

      let localOffset = 0;
      while (localOffset < blkSize - msgHeaderSize) {
        const msgOffset = blkOffset + localOffset;
        if (msgOffset + msgHeaderSize > expandedBuffer.byteLength) break;

        const msg = _unpack_struct_from(HEADER_MSG_INFO_V2, expandedBuffer, msgOffset);
        const dataOffset = msgOffset + msgHeaderSize;

        if (msg.get('type') === OBJECT_CONTINUATION_MSG_TYPE) {
          if (dataOffset + 16 <= expandedBuffer.byteLength) {
            const [contOff, contSize] = struct.unpack_from('<QQ', expandedBuffer, dataOffset) as [
              number,
              number,
            ];
            if (!loadedBlocks.has(Number(contOff) + 4)) {
              blocksToProcess.push([Number(contOff) + 4, Number(contSize) - 4]);
            }
          }
        }

        localOffset += msgHeaderSize + (msg.get('size') as number);
      }
    }

    return expandedBuffer;
  }

  async get_links_async(): Promise<Links> {
    const links: Links = {};

    for (const msg of this.msgs) {
      if (msg.get('type') === SYMBOL_TABLE_MSG_TYPE) {
        const data = _unpack_struct_from(
          SYMBOL_TABLE_MSG,
          this.fh,
          msg.get('offset_to_message') as number
        );
        const btreeAddress = data.get('btree_address') as number;
        const heapAddress = data.get('heap_address') as number;

        const btree = new BTreeV1Groups(this.fh, btreeAddress);
        const heap = await Heap.createAsync(this.fh, heapAddress, this._source);

        for (const symbolTableAddress of btree.symbol_table_addresses()) {
          let tableBuffer: ArrayBuffer = this.fh;
          let adjustedOffset = symbolTableAddress;

          if (symbolTableAddress >= this.fh.byteLength) {
            if (!this._source) continue;
            const loadSize = 64 * 1024;
            const loadEnd = Math.min(symbolTableAddress + loadSize, this._source.size);
            const remoteBuffer = await this._source.read(symbolTableAddress, loadEnd);
            tableBuffer = remoteBuffer;
            adjustedOffset = 0;
          }

          try {
            const table = new SymbolTable(tableBuffer, adjustedOffset);
            table.assign_name(heap);
            const tableLinks = table.get_links(heap);
            for (const [name, addr] of Object.entries(tableLinks)) {
              links[name] = addr;
            }
          } catch (e) {
            console.error(`tsfive async: Error parsing symbol_table: ${(e as Error).message}`);
          }
        }
      } else if (msg.get('type') === LINK_MSG_TYPE) {
        const [name, addr] = this._get_link_from_link_msg(msg);
        links[name] = addr;
      } else if (msg.get('type') === LINK_INFO_MSG_TYPE) {
        for (const [name, addr] of this._iter_link_from_link_info_msg(msg)) {
          links[name] = addr;
        }
      }
    }

    return links;
  }

  async getDataSliceAsync(start: number, end: number): Promise<DataValue[]> {
    const msg = this.find_msg_type(DATA_STORAGE_MSG_TYPE)[0];
    const msgOffset = msg.get('offset_to_message') as number;
    const [_version, _dims, layoutClass, propertyOffset] =
      this._get_data_message_properties(msgOffset);

    if (layoutClass === 0) {
      throw new Error('Compact storage not implemented');
    } else if (layoutClass === 1) {
      return await this._get_contiguous_data_slice_async(propertyOffset, start, end);
    } else if (layoutClass === 2) {
      return await this._get_chunked_data_slice_async(start, end);
    }

    throw new Error(`Unknown layout class: ${layoutClass}`);
  }

  private async _get_contiguous_data_slice_async(
    propertyOffset: number,
    start: number,
    end: number
  ): Promise<DataValue[]> {
    const [dataOffset] = struct.unpack_from('<Q', this.fh, propertyOffset) as [number];

    if (dataOffset === UNDEFINED_ADDRESS) {
      return new Array(end - start).fill(this.fillvalue);
    }

    const dtype = this.dtype;

    if (!(dtype instanceof Array) && /[<>=!@|]?(i|u|f|S)(\d*)/.test(dtype as string)) {
      const [itemGetter, itemIsBigEndian, itemSize] = dtype_getter(dtype as string);
      const byteStart = Number(dataOffset) + start * itemSize;
      const byteEnd = Number(dataOffset) + end * itemSize;

      const buffer = await this._source.read(byteStart, byteEnd);
      const view = new DataView64(buffer);

      const count = end - start;
      const output: DataValue[] = new Array(count);
      const method = view[itemGetter as keyof DataView64] as (
        offset: number,
        littleEndian: boolean,
        size: number
      ) => DataValue;

      for (let i = 0; i < count; i++) {
        output[i] = method.call(view, i * itemSize, !itemIsBigEndian, itemSize);
      }
      return output;
    }

    throw new Error('Unsupported dtype for async read');
  }

  private async _get_chunked_data_slice_async(start: number, end: number): Promise<DataValue[]> {
    this._get_chunk_params();

    if (this._chunk_address === UNDEFINED_ADDRESS) {
      return new Array(end - start).fill(this.fillvalue);
    }

    const shape = this.shape;
    const startCoords = this._linearToMultiIndex(start, shape);
    const endCoords = this._linearToMultiIndex(end - 1, shape);
    const slices: Slice[] = startCoords.map((s, i) => [s, endCoords[i] + 1]);

    return await this._get_chunked_data_multi_slice_async(slices);
  }

  /**
   * Multi-dimensional slice with LAZY BTree traversal
   * Processes chunks in batches to prevent OOM
   */
  private async _get_chunked_data_multi_slice_async(slices: Slice[]): Promise<DataValue[]> {
    this._get_chunk_params();

    if (this._chunk_address === UNDEFINED_ADDRESS) {
      const size = slices.reduce((acc, [s, e]) => acc * (e - s), 1);
      return new Array(size).fill(this.fillvalue);
    }

    const shape = this.shape;
    const chunkShape = this._chunks!;
    const dtype = this.dtype;
    const [itemGetter, itemIsBigEndian, itemSize] = dtype_getter(dtype as string);

    const outputShape = slices.map(([s, e]) => e - s);
    const outputSize = outputShape.reduce((a, b) => a * b, 1);

    // Safety check: prevent OOM for very large reads
    const MAX_OUTPUT_SIZE = 50 * 1024 * 1024; // 50M elements max
    if (outputSize > MAX_OUTPUT_SIZE) {
      throw new Error(
        `Slice too large: ${outputSize} elements requested (max ${MAX_OUTPUT_SIZE}). ` +
          `Slices: ${JSON.stringify(slices)}. Use smaller slices.`
      );
    }

    // LAZY BTree - only loads nodes needed for the slice!
    const requiredChunks = await this._findChunksLazy(
      this._chunk_address!,
      this._chunk_dims!,
      slices,
      chunkShape,
      shape
    );

    // Safety check: limit number of chunks to process at once
    const MAX_CHUNKS = 1000;
    if (requiredChunks.length > MAX_CHUNKS) {
      console.warn(`tsfive: Processing ${requiredChunks.length} chunks in batches`);
    }

    const output: DataValue[] = new Array(outputSize);
    const outputStrides = this._calculateStrides(outputShape);
    const chunkStrides = this._calculateStrides(chunkShape);

    // Process chunks in batches to avoid memory pressure
    const BATCH_SIZE = 100;
    for (let batchStart = 0; batchStart < requiredChunks.length; batchStart += BATCH_SIZE) {
      const batchEnd = Math.min(batchStart + BATCH_SIZE, requiredChunks.length);
      const batch = requiredChunks.slice(batchStart, batchEnd);

      // Process batch - load and extract one chunk at a time
      for (const chunkInfo of batch) {
        const chunkBuffer = await this._source.read(
          chunkInfo.address,
          chunkInfo.address + chunkInfo.size
        );

        let processedBuffer = chunkBuffer;
        if (this.filter_pipeline) {
          processedBuffer = this._applyFilters(
            chunkBuffer,
            chunkInfo.filterMask,
            this.filter_pipeline,
            itemSize
          );
        }

        const chunkView = new DataView64(processedBuffer);

        this._extractChunkSlice(
          chunkView,
          chunkInfo.offset,
          chunkShape,
          chunkStrides,
          slices,
          output,
          outputShape,
          outputStrides,
          itemGetter,
          itemIsBigEndian,
          itemSize
        );
      }
    }

    return output;
  }

  /**
   * LAZY BTree traversal - KEY for OOM prevention
   * Only loads nodes that might contain chunks overlapping with the slice
   */
  private async _findChunksLazy(
    btreeAddress: number,
    dims: number,
    slices: Slice[],
    chunkShape: number[],
    dataShape: number[]
  ): Promise<ChunkInfo[]> {
    const B_LINK_NODE_SIZE = 24;
    const requiredChunks: ChunkInfo[] = [];
    const nodesToProcess: Array<[number, number | null]> = [[btreeAddress, null]];
    let nodesLoaded = 0;

    // Log large slice requests for debugging
    const sliceSize = slices.reduce((acc, [s, e]) => acc * (e - s), 1);
    if (sliceSize > 1000000) {
      console.log(
        `tsfive lazy: Large slice - size=${sliceSize}, slices=${JSON.stringify(slices)}, shape=${JSON.stringify(dataShape)}`
      );
    }

    while (nodesToProcess.length > 0) {
      const [nodeAddr, _expectedLevel] = nodesToProcess.pop()!;

      // Load node on-demand (small buffer, not huge concatenation)
      const nodeBuffer = await this._loadBTreeNode(nodeAddr);
      if (!nodeBuffer) continue;
      nodesLoaded++;

      const sig = struct.unpack_from('4s', nodeBuffer, 0)[0] as string;
      if (sig !== 'TREE') continue;

      const nodeType = struct.unpack_from('<B', nodeBuffer, 4)[0] as number;
      const nodeLevel = struct.unpack_from('<B', nodeBuffer, 5)[0] as number;
      const entriesUsed = struct.unpack_from('<H', nodeBuffer, 6)[0] as number;

      if (nodeType !== 1) continue;

      // Read all entries first to get keys for internal node range checking
      const entries: BTreeEntry[] = [];
      let offset = B_LINK_NODE_SIZE;
      for (let i = 0; i < entriesUsed; i++) {
        const chunkSize = struct.unpack_from('<I', nodeBuffer, offset)[0] as number;
        const filterMask = struct.unpack_from('<I', nodeBuffer, offset + 4)[0] as number;
        offset += 8;

        const chunkOffset: number[] = [];
        for (let d = 0; d < dims; d++) {
          chunkOffset.push(Number(struct.unpack_from('<Q', nodeBuffer, offset)[0]));
          offset += 8;
        }

        const childAddr = Number(struct.unpack_from('<Q', nodeBuffer, offset)[0]);
        offset += 8;

        entries.push({ chunkSize, filterMask, chunkOffset, childAddr });
      }

      // Process entries with correct overlap logic for internal vs leaf nodes
      for (let i = 0; i < entries.length; i++) {
        const { chunkSize, filterMask, chunkOffset, childAddr } = entries[i];

        let overlaps: boolean;
        if (nodeLevel === 0) {
          // Leaf node: chunkOffset is the actual chunk position
          overlaps = this._chunkMayOverlapSlice(chunkOffset, chunkShape, slices, dataShape);
        } else {
          // Internal node: chunkOffset is the FIRST key in the subtree
          // We need to follow this subtree if the slice could be within its range
          // Entry i covers from chunkOffset[i] to chunkOffset[i+1] (or infinity for last entry)
          const keyStart = chunkOffset[0];
          const keyEnd = i + 1 < entries.length ? entries[i + 1].chunkOffset[0] : Infinity;
          const sliceStart = slices[0][0];
          const sliceEnd = slices[0][1];

          // Subtree covers chunks from keyStart to keyEnd (exclusive)
          // We need to follow if there's any overlap
          overlaps = !(sliceEnd <= keyStart || sliceStart >= keyEnd);
        }

        if (overlaps) {
          if (nodeLevel === 0) {
            requiredChunks.push({
              address: childAddr,
              offset: chunkOffset.slice(0, -1),
              size: chunkSize,
              filterMask: filterMask,
            });
          } else {
            nodesToProcess.push([childAddr, nodeLevel - 1]);
          }
        }
      }
    }

    console.log(`tsfive lazy: Found ${requiredChunks.length} chunks, loaded ${nodesLoaded} nodes`);
    return requiredChunks;
  }

  private async _loadBTreeNode(nodeAddr: number): Promise<ArrayBuffer | null> {
    const neededSize = 8192;

    if (nodeAddr + neededSize <= this.fh.byteLength) {
      return this.fh.slice(nodeAddr, nodeAddr + neededSize);
    }

    if (!this._source) return null;

    const loadEnd = Math.min(nodeAddr + neededSize, this._source.size);
    try {
      return await this._source.read(nodeAddr, loadEnd);
    } catch (e) {
      console.log(`tsfive lazy: Error loading node at ${nodeAddr}: ${(e as Error).message}`);
      return null;
    }
  }

  private _chunkMayOverlapSlice(
    chunkOffset: number[],
    chunkShape: number[],
    slices: Slice[],
    dataShape: number[]
  ): boolean {
    for (let d = 0; d < slices.length; d++) {
      const chunkStart = chunkOffset[d];
      const chunkEnd = Math.min(chunkStart + chunkShape[d], dataShape[d]);
      const sliceStart = slices[d][0];
      const sliceEnd = slices[d][1];

      if (chunkEnd <= sliceStart || chunkStart >= sliceEnd) {
        return false;
      }
    }
    return true;
  }

  private _applyFilters(
    chunkBuffer: ArrayBuffer,
    filterMask: number,
    filterPipeline: UnpackedStruct[],
    itemsize: number
  ): ArrayBuffer {
    let buf: ArrayBuffer =
      'slice' in chunkBuffer
        ? (chunkBuffer as ArrayBuffer).slice(0)
        : new Uint8Array(chunkBuffer).buffer;

    for (let i = filterPipeline.length - 1; i >= 0; i--) {
      if (filterMask & (1 << i)) continue;

      const entry = filterPipeline[i];
      const filterId = entry.get('filter_id') as number;
      const clientData = entry.get('client_data') as number[];

      if (Filters.has(filterId)) {
        buf = Filters.get(filterId)!(buf, itemsize, clientData);
      } else {
        throw new Error(`Filter ${filterId} not supported`);
      }
    }

    return buf;
  }

  private _extractChunkSlice(
    chunkView: DataView64,
    chunkOffset: number[],
    chunkShape: number[],
    chunkStrides: number[],
    slices: Slice[],
    output: DataValue[],
    outputShape: number[],
    outputStrides: number[],
    itemGetter: string,
    itemIsBigEndian: boolean,
    itemSize: number
  ): void {
    const dims = slices.length;

    const intersect: Slice[] = slices.map(([s, e], d) => {
      const chunkStart = chunkOffset[d];
      const chunkEnd = chunkStart + chunkShape[d];
      return [Math.max(s, chunkStart), Math.min(e, chunkEnd)];
    });

    const method = chunkView[itemGetter as keyof DataView64] as (
      offset: number,
      littleEndian: boolean,
      size: number
    ) => DataValue;

    const processPoint = (dim: number, chunkIdx: number, outputIdx: number): void => {
      if (dim === dims) {
        const value = method.call(chunkView, chunkIdx * itemSize, !itemIsBigEndian, itemSize);
        output[outputIdx] = value;
        return;
      }

      for (let i = intersect[dim][0]; i < intersect[dim][1]; i++) {
        const localChunkIdx = i - chunkOffset[dim];
        const localOutputIdx = i - slices[dim][0];
        processPoint(
          dim + 1,
          chunkIdx + localChunkIdx * chunkStrides[dim],
          outputIdx + localOutputIdx * outputStrides[dim]
        );
      }
    };

    processPoint(0, 0, 0);
  }

  private _calculateStrides(shape: number[]): number[] {
    const strides = new Array<number>(shape.length);
    let stride = 1;
    for (let i = shape.length - 1; i >= 0; i--) {
      strides[i] = stride;
      stride *= shape[i];
    }
    return strides;
  }

  private _linearToMultiIndex(linearIdx: number, shape: number[]): number[] {
    const coords = new Array<number>(shape.length);
    let remaining = linearIdx;
    for (let i = shape.length - 1; i >= 0; i--) {
      coords[i] = remaining % shape[i];
      remaining = Math.floor(remaining / shape[i]);
    }
    return coords;
  }
}
