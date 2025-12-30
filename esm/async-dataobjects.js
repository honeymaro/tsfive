/**
 * AsyncDataObjects - Separate module for async lazy loading support
 * Uses lazy BTree traversal to avoid OOM on large files
 */

import { _structure_size, _unpack_struct_from, struct, dtype_getter, DataView64 } from './core.js';
import { BTreeV1Groups } from './btree.js';
import { Heap, SymbolTable } from './misc-low-level.js';
import { Filters } from './filters.js';
import { DataObjects } from './dataobjects.js';

// Message types (copied from dataobjects.js)
const SYMBOL_TABLE_MSG_TYPE = 0x0011;
const LINK_MSG_TYPE = 0x0006;
const LINK_INFO_MSG_TYPE = 0x0002;
const DATA_STORAGE_MSG_TYPE = 0x0008;
const OBJECT_CONTINUATION_MSG_TYPE = 0x0010;

// Header structures
const OBJECT_HEADER_V1 = new Map([
  ['version', 'B'],
  ['reserved', 'B'],
  ['total_header_messages', 'H'],
  ['object_reference_count', 'I'],
  ['object_header_size', 'I'],
  ['padding', 'I'],
]);

const OBJECT_HEADER_V2 = new Map([
  ['signature', '4s'],
  ['version', 'B'],
  ['flags', 'B'],
]);

const HEADER_MSG_INFO_V1 = new Map([
  ['type', 'H'],
  ['size', 'H'],
  ['flags', 'B'],
  ['reserved', '3s']
]);
const HEADER_MSG_INFO_V1_SIZE = _structure_size(HEADER_MSG_INFO_V1);

const HEADER_MSG_INFO_V2 = new Map([
  ['type', 'B'],
  ['size', 'H'],
  ['flags', 'B'],
]);
const HEADER_MSG_INFO_V2_SIZE = _structure_size(HEADER_MSG_INFO_V2);

const SYMBOL_TABLE_MSG = new Map([
  ['btree_address', 'Q'],
  ['heap_address', 'Q'],
]);

const UNDEFINED_ADDRESS = struct.unpack_from('<Q', new Uint8Array([255,255,255,255,255,255,255,255]).buffer);

/**
 * Async DataObjects class for lazy loading support
 * KEY FEATURE: Uses lazy BTree traversal to avoid loading all nodes (OOM prevention)
 */
export class AsyncDataObjects extends DataObjects {
  constructor(fh, offset, source) {
    super(fh, offset);
    this._source = source;
  }

  /**
   * Async factory that pre-loads metadata including continuation blocks
   */
  static async createAsync(fh, offset, source) {
    console.log(`jsfive async: createAsync offset=${offset}, buffer=${fh.byteLength}`);

    let workingBuffer = fh;

    // If offset is outside buffer, load it first
    if (offset >= fh.byteLength) {
      const loadSize = 64 * 1024;
      const loadEnd = Math.min(offset + loadSize, source.size);
      console.log(`jsfive async: Loading DataObjects at ${offset}`);
      workingBuffer = await source.read(offset, loadEnd);
      offset = 0;
    }

    const version_hint = struct.unpack_from('<B', workingBuffer, offset)[0];

    if (version_hint == 1) {
      workingBuffer = await AsyncDataObjects._loadV1WithContinuations(workingBuffer, offset, source);
    } else if (version_hint == 79) { // 'O' = v2
      workingBuffer = await AsyncDataObjects._loadV2WithContinuations(workingBuffer, offset, source);
    }

    return new AsyncDataObjects(workingBuffer, offset, source);
  }

  static async _loadV1WithContinuations(buffer, offset, source) {
    const header = _unpack_struct_from(OBJECT_HEADER_V1, buffer, offset);
    let block_size = header.get('object_header_size');
    let block_offset = offset + _structure_size(OBJECT_HEADER_V1);

    const blocksToProcess = [[block_offset, block_size]];
    const loadedBlocks = new Map();
    let expandedBuffer = buffer;

    while (blocksToProcess.length > 0) {
      const [blkOffset, blkSize] = blocksToProcess.shift();

      if (blkOffset + blkSize > expandedBuffer.byteLength) {
        console.log(`jsfive async: Loading V1 block ${blkOffset}-${blkOffset + blkSize}`);
        const blockData = await source.read(blkOffset, blkOffset + blkSize);

        const newSize = Math.max(expandedBuffer.byteLength, blkOffset + blkSize);
        const newBuffer = new ArrayBuffer(newSize);
        new Uint8Array(newBuffer).set(new Uint8Array(expandedBuffer));
        new Uint8Array(newBuffer).set(new Uint8Array(blockData), blkOffset);
        expandedBuffer = newBuffer;
      }

      if (loadedBlocks.has(blkOffset)) continue;
      loadedBlocks.set(blkOffset, true);

      let local_offset = 0;
      while (local_offset < blkSize) {
        const msg_offset = blkOffset + local_offset;
        if (msg_offset + HEADER_MSG_INFO_V1_SIZE > expandedBuffer.byteLength) break;

        const msg = _unpack_struct_from(HEADER_MSG_INFO_V1, expandedBuffer, msg_offset);
        const data_offset = msg_offset + HEADER_MSG_INFO_V1_SIZE;

        if (msg.get('type') == OBJECT_CONTINUATION_MSG_TYPE) {
          if (data_offset + 16 <= expandedBuffer.byteLength) {
            const [cont_off, cont_size] = struct.unpack_from('<QQ', expandedBuffer, data_offset);
            if (!loadedBlocks.has(Number(cont_off))) {
              blocksToProcess.push([Number(cont_off), Number(cont_size)]);
            }
          }
        }

        local_offset += HEADER_MSG_INFO_V1_SIZE + msg.get('size');
        if (local_offset >= blkSize) break;
      }
    }

    return expandedBuffer;
  }

  static async _loadV2WithContinuations(buffer, offset, source) {
    const header = _unpack_struct_from(OBJECT_HEADER_V2, buffer, offset);
    const flags = header.get('flags');
    const creation_order_size = (flags & 0b00000100) ? 2 : 0;

    let block_offset = offset + _structure_size(OBJECT_HEADER_V2);
    if (flags & 0b00010000) block_offset += 4;
    if (flags & 0b00100000) block_offset += 4;

    const chunk0_size_len = 1 << (flags & 0b00000011);
    const block_size = struct.unpack_from(`<${['B','H','I','Q'][chunk0_size_len-1]}`, buffer, block_offset)[0];
    block_offset += chunk0_size_len;

    const msgHeaderSize = HEADER_MSG_INFO_V2_SIZE + creation_order_size;

    const blocksToProcess = [[block_offset, Number(block_size)]];
    const loadedBlocks = new Map();
    let expandedBuffer = buffer;

    while (blocksToProcess.length > 0) {
      const [blkOffset, blkSize] = blocksToProcess.shift();

      if (blkOffset + blkSize > expandedBuffer.byteLength) {
        console.log(`jsfive async: Loading V2 block ${blkOffset}-${blkOffset + blkSize}`);
        const blockData = await source.read(blkOffset, blkOffset + blkSize);

        const newSize = Math.max(expandedBuffer.byteLength, blkOffset + blkSize);
        const newBuffer = new ArrayBuffer(newSize);
        new Uint8Array(newBuffer).set(new Uint8Array(expandedBuffer));
        new Uint8Array(newBuffer).set(new Uint8Array(blockData), blkOffset);
        expandedBuffer = newBuffer;
      }

      if (loadedBlocks.has(blkOffset)) continue;
      loadedBlocks.set(blkOffset, true);

      let local_offset = 0;
      while (local_offset < blkSize - msgHeaderSize) {
        const msg_offset = blkOffset + local_offset;
        if (msg_offset + msgHeaderSize > expandedBuffer.byteLength) break;

        const msg = _unpack_struct_from(HEADER_MSG_INFO_V2, expandedBuffer, msg_offset);
        const data_offset = msg_offset + msgHeaderSize;

        if (msg.get('type') == OBJECT_CONTINUATION_MSG_TYPE) {
          if (data_offset + 16 <= expandedBuffer.byteLength) {
            const [cont_off, cont_size] = struct.unpack_from('<QQ', expandedBuffer, data_offset);
            if (!loadedBlocks.has(Number(cont_off) + 4)) {
              blocksToProcess.push([Number(cont_off) + 4, Number(cont_size) - 4]);
            }
          }
        }

        local_offset += msgHeaderSize + msg.get('size');
      }
    }

    return expandedBuffer;
  }

  async get_links_async() {
    const links = {};

    for (let msg of this.msgs) {
      if (msg.get('type') == SYMBOL_TABLE_MSG_TYPE) {
        let data = _unpack_struct_from(SYMBOL_TABLE_MSG, this.fh, msg.get('offset_to_message'));
        let btree_address = data.get('btree_address');
        let heap_address = data.get('heap_address');

        let btree = new BTreeV1Groups(this.fh, btree_address);
        let heap = await Heap.createAsync(this.fh, heap_address, this._source);

        for (let symbol_table_address of btree.symbol_table_addresses()) {
          let tableBuffer = this.fh;

          if (symbol_table_address >= this.fh.byteLength) {
            if (!this._source) continue;
            const loadSize = 64 * 1024;
            const loadEnd = Math.min(symbol_table_address + loadSize, this._source.size);
            const remoteBuffer = await this._source.read(symbol_table_address, loadEnd);
            tableBuffer = { _remoteBuffer: remoteBuffer, _remoteOffset: symbol_table_address };
          }

          try {
            let table;
            if (tableBuffer._remoteBuffer) {
              const adjustedOffset = symbol_table_address - tableBuffer._remoteOffset;
              table = new SymbolTable(tableBuffer._remoteBuffer, adjustedOffset);
            } else {
              table = new SymbolTable(tableBuffer, symbol_table_address);
            }
            table.assign_name(heap);
            const tableLinks = table.get_links(heap);
            for (let [name, addr] of Object.entries(tableLinks)) {
              links[name] = addr;
            }
          } catch (e) {
            console.error(`jsfive async: Error parsing symbol_table: ${e.message}`);
          }
        }
      } else if (msg.get('type') == LINK_MSG_TYPE) {
        let [name, addr] = this._get_link_from_link_msg(msg);
        links[name] = addr;
      } else if (msg.get('type') == LINK_INFO_MSG_TYPE) {
        for (let [name, addr] of this._iter_link_from_link_info_msg(msg)) {
          links[name] = addr;
        }
      }
    }

    return links;
  }

  async getDataSliceAsync(start, end) {
    const msg = this.find_msg_type(DATA_STORAGE_MSG_TYPE)[0];
    const msg_offset = msg.get('offset_to_message');
    const [version, dims, layout_class, property_offset] =
      this._get_data_message_properties(msg_offset);

    if (layout_class === 0) {
      throw new Error('Compact storage not implemented');
    } else if (layout_class === 1) {
      return await this._get_contiguous_data_slice_async(property_offset, start, end);
    } else if (layout_class === 2) {
      return await this._get_chunked_data_slice_async(start, end);
    }

    throw new Error(`Unknown layout class: ${layout_class}`);
  }

  async _get_contiguous_data_slice_async(property_offset, start, end) {
    const [data_offset] = struct.unpack_from('<Q', this.fh, property_offset);

    if (data_offset === UNDEFINED_ADDRESS[0]) {
      return new Array(end - start).fill(this.fillvalue);
    }

    const dtype = this.dtype;

    if (!(dtype instanceof Array) && /[<>=!@\|]?(i|u|f|S)(\d*)/.test(dtype)) {
      const [item_getter, item_is_big_endian, item_size] = dtype_getter(dtype);
      const byteStart = Number(data_offset) + start * item_size;
      const byteEnd = Number(data_offset) + end * item_size;

      const buffer = await this._source.read(byteStart, byteEnd);
      const view = new DataView64(buffer);

      const count = end - start;
      const output = new Array(count);
      for (let i = 0; i < count; i++) {
        output[i] = view[item_getter](i * item_size, !item_is_big_endian, item_size);
      }
      return output;
    }

    throw new Error('Unsupported dtype for async read');
  }

  async _get_chunked_data_slice_async(start, end) {
    this._get_chunk_params();

    if (this._chunk_address === UNDEFINED_ADDRESS[0]) {
      return new Array(end - start).fill(this.fillvalue);
    }

    const shape = this.shape;
    const startCoords = this._linearToMultiIndex(start, shape);
    const endCoords = this._linearToMultiIndex(end - 1, shape);
    const slices = startCoords.map((s, i) => [s, endCoords[i] + 1]);

    return await this._get_chunked_data_multi_slice_async(slices);
  }

  /**
   * Multi-dimensional slice with LAZY BTree traversal
   * Processes chunks in batches to prevent OOM
   */
  async _get_chunked_data_multi_slice_async(slices) {
    this._get_chunk_params();

    if (this._chunk_address === UNDEFINED_ADDRESS[0]) {
      const size = slices.reduce((acc, [s, e]) => acc * (e - s), 1);
      return new Array(size).fill(this.fillvalue);
    }

    const shape = this.shape;
    const chunkShape = this._chunks;
    const dtype = this.dtype;
    const [item_getter, item_is_big_endian, item_size] = dtype_getter(dtype);

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
      this._chunk_address, this._chunk_dims, slices, chunkShape, shape);

    // Safety check: limit number of chunks to process at once
    const MAX_CHUNKS = 1000;
    if (requiredChunks.length > MAX_CHUNKS) {
      console.warn(`jsfive: Processing ${requiredChunks.length} chunks in batches`);
    }

    const output = new Array(outputSize);
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
            chunkBuffer, chunkInfo.filterMask, this.filter_pipeline, item_size
          );
        }

        const chunkView = new DataView64(processedBuffer);

        this._extractChunkSlice(
          chunkView, chunkInfo.offset, chunkShape, chunkStrides,
          slices, output, outputShape, outputStrides,
          item_getter, item_is_big_endian, item_size
        );
      }
    }

    return output;
  }

  /**
   * LAZY BTree traversal - KEY for OOM prevention
   * Only loads nodes that might contain chunks overlapping with the slice
   */
  async _findChunksLazy(btreeAddress, dims, slices, chunkShape, dataShape) {
    const B_LINK_NODE_SIZE = 24;
    const requiredChunks = [];
    const nodesToProcess = [[btreeAddress, null]];
    let nodesLoaded = 0;

    // Log large slice requests for debugging
    const sliceSize = slices.reduce((acc, [s, e]) => acc * (e - s), 1);
    if (sliceSize > 1000000) {
      console.log(`jsfive lazy: Large slice - size=${sliceSize}, slices=${JSON.stringify(slices)}, shape=${JSON.stringify(dataShape)}`);
    }

    while (nodesToProcess.length > 0) {
      const [nodeAddr, expectedLevel] = nodesToProcess.pop();

      // Load node on-demand (small buffer, not huge concatenation)
      const nodeBuffer = await this._loadBTreeNode(nodeAddr);
      if (!nodeBuffer) continue;
      nodesLoaded++;

      const sig = struct.unpack_from('4s', nodeBuffer, 0)[0];
      if (sig !== 'TREE') continue;

      const nodeType = struct.unpack_from('<B', nodeBuffer, 4)[0];
      const nodeLevel = struct.unpack_from('<B', nodeBuffer, 5)[0];
      const entriesUsed = struct.unpack_from('<H', nodeBuffer, 6)[0];

      if (nodeType !== 1) continue;

      // Read all entries first to get keys for internal node range checking
      const entries = [];
      let offset = B_LINK_NODE_SIZE;
      for (let i = 0; i < entriesUsed; i++) {
        const chunkSize = struct.unpack_from('<I', nodeBuffer, offset)[0];
        const filterMask = struct.unpack_from('<I', nodeBuffer, offset + 4)[0];
        offset += 8;

        const chunkOffset = [];
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

        let overlaps;
        if (nodeLevel === 0) {
          // Leaf node: chunkOffset is the actual chunk position
          overlaps = this._chunkMayOverlapSlice(chunkOffset, chunkShape, slices, dataShape);
        } else {
          // Internal node: chunkOffset is the FIRST key in the subtree
          // We need to follow this subtree if the slice could be within its range
          // Entry i covers from chunkOffset[i] to chunkOffset[i+1] (or infinity for last entry)
          const keyStart = chunkOffset[0];
          const keyEnd = (i + 1 < entries.length) ? entries[i + 1].chunkOffset[0] : Infinity;
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
              filterMask: filterMask
            });
          } else {
            nodesToProcess.push([childAddr, nodeLevel - 1]);
          }
        }
      }
    }

    console.log(`jsfive lazy: Found ${requiredChunks.length} chunks, loaded ${nodesLoaded} nodes`);
    return requiredChunks;
  }

  async _loadBTreeNode(nodeAddr) {
    const neededSize = 8192;

    if (nodeAddr + neededSize <= this.fh.byteLength) {
      return this.fh.slice(nodeAddr, nodeAddr + neededSize);
    }

    if (!this._source) return null;

    const loadEnd = Math.min(nodeAddr + neededSize, this._source.size);
    try {
      return await this._source.read(nodeAddr, loadEnd);
    } catch (e) {
      console.log(`jsfive lazy: Error loading node at ${nodeAddr}: ${e.message}`);
      return null;
    }
  }

  _chunkMayOverlapSlice(chunkOffset, chunkShape, slices, dataShape) {
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

  _applyFilters(chunkBuffer, filterMask, filterPipeline, itemsize) {
    let buf = chunkBuffer.slice ? chunkBuffer.slice() : new Uint8Array(chunkBuffer).buffer;

    for (let i = filterPipeline.length - 1; i >= 0; i--) {
      if (filterMask & (1 << i)) continue;

      const entry = filterPipeline[i];
      const filterId = entry.get('filter_id');
      const clientData = entry.get('client_data');

      if (Filters.has(filterId)) {
        buf = Filters.get(filterId)(buf, itemsize, clientData);
      } else {
        throw new Error(`Filter ${filterId} not supported`);
      }
    }

    return buf;
  }

  _extractChunkSlice(chunkView, chunkOffset, chunkShape, chunkStrides,
                     slices, output, outputShape, outputStrides,
                     item_getter, item_is_big_endian, item_size) {
    const dims = slices.length;

    const intersect = slices.map(([s, e], d) => {
      const chunkStart = chunkOffset[d];
      const chunkEnd = chunkStart + chunkShape[d];
      return [Math.max(s, chunkStart), Math.min(e, chunkEnd)];
    });

    function processPoint(dim, chunkIdx, outputIdx) {
      if (dim === dims) {
        const value = chunkView[item_getter](
          chunkIdx * item_size, !item_is_big_endian, item_size
        );
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
    }

    processPoint(0, 0, 0);
  }

  _calculateStrides(shape) {
    const strides = new Array(shape.length);
    let stride = 1;
    for (let i = shape.length - 1; i >= 0; i--) {
      strides[i] = stride;
      stride *= shape[i];
    }
    return strides;
  }

  _linearToMultiIndex(linearIdx, shape) {
    const coords = new Array(shape.length);
    let remaining = linearIdx;
    for (let i = shape.length - 1; i >= 0; i--) {
      coords[i] = remaining % shape[i];
      remaining = Math.floor(remaining / shape[i]);
    }
    return coords;
  }
}
