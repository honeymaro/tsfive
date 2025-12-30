/**
 * HDF5 B-Tree implementations (V1 and V2)
 */

import {
  _unpack_struct_from,
  _structure_size,
  struct,
  dtype_getter,
  bitSize,
  DataView64,
} from './core.js';
import { Filters } from './filters.js';
import type { StructureDefinition, UnpackedStruct } from './types/hdf5.js';
import type { Dtype } from './types/dtype.js';

// ============================================================================
// Abstract B-Tree Base Class
// ============================================================================

abstract class AbstractBTree {
  protected fh: ArrayBuffer;
  protected offset: number;
  protected depth: number | null;
  protected all_nodes: Map<number, UnpackedStruct[]>;

  constructor(fh: ArrayBuffer, offset: number) {
    this.fh = fh;
    this.offset = offset;
    this.depth = null;
    this.all_nodes = new Map();
  }

  protected init(): void {
    this.all_nodes = new Map();
    this._read_root_node();
    this._read_children();
  }

  protected _read_children(): void {
    let nodeLevel = this.depth!;
    while (nodeLevel > 0) {
      for (const parentNode of this.all_nodes.get(nodeLevel)!) {
        for (const childAddr of parentNode.get('addresses') as number[]) {
          this._add_node(this._read_node(childAddr, nodeLevel - 1));
        }
      }
      nodeLevel--;
    }
  }

  protected _read_root_node(): void {
    const rootNode = this._read_node(this.offset, null);
    this._add_node(rootNode);
    this.depth = rootNode.get('node_level') as number;
  }

  protected _add_node(node: UnpackedStruct): void {
    const nodeLevel = node.get('node_level') as number;
    if (this.all_nodes.has(nodeLevel)) {
      this.all_nodes.get(nodeLevel)!.push(node);
    } else {
      this.all_nodes.set(nodeLevel, [node]);
    }
  }

  protected _read_node(offset: number, nodeLevel: number | null): UnpackedStruct {
    const node = this._read_node_header(offset, nodeLevel);
    node.set('keys', []);
    node.set('addresses', []);
    return node;
  }

  protected abstract _read_node_header(offset: number, nodeLevel: number | null): UnpackedStruct;
}

// ============================================================================
// B-Tree V1
// ============================================================================

export class BTreeV1 extends AbstractBTree {
  protected B_LINK_NODE: StructureDefinition = new Map([
    ['signature', '4s'],
    ['node_type', 'B'],
    ['node_level', 'B'],
    ['entries_used', 'H'],
    ['left_sibling', 'Q'],
    ['right_sibling', 'Q'],
  ]);

  protected NODE_TYPE = 0;

  protected _read_node_header(offset: number, nodeLevel: number | null): UnpackedStruct {
    const node = _unpack_struct_from(this.B_LINK_NODE, this.fh, offset);
    if (nodeLevel !== null) {
      if (node.get('node_level') !== nodeLevel) {
        throw new Error('node level does not match');
      }
    }
    return node;
  }
}

// ============================================================================
// B-Tree V1 Groups (Type 0)
// ============================================================================

export class BTreeV1Groups extends BTreeV1 {
  NODE_TYPE = 0;

  constructor(fh: ArrayBuffer, offset: number) {
    super(fh, offset);
    this.init();
  }

  protected _read_node(offset: number, nodeLevel: number | null): UnpackedStruct {
    const node = this._read_node_header(offset, nodeLevel);
    offset += _structure_size(this.B_LINK_NODE);

    const keys: number[] = [];
    const addresses: number[] = [];
    const entriesUsed = node.get('entries_used') as number;

    for (let i = 0; i < entriesUsed; i++) {
      const key = struct.unpack_from('<Q', this.fh, offset)[0] as number;
      offset += 8;
      const address = struct.unpack_from('<Q', this.fh, offset)[0] as number;
      offset += 8;
      keys.push(key);
      addresses.push(address);
    }

    // N+1 key
    keys.push(struct.unpack_from('<Q', this.fh, offset)[0] as number);
    node.set('keys', keys);
    node.set('addresses', addresses);
    return node;
  }

  symbol_table_addresses(): number[] {
    let allAddress: number[] = [];
    const rootNodes = this.all_nodes.get(0)!;
    for (const node of rootNodes) {
      allAddress = allAddress.concat(node.get('addresses') as number[]);
    }
    return allAddress;
  }
}

// ============================================================================
// B-Tree V1 Raw Data Chunks (Type 1)
// ============================================================================

export class BTreeV1RawDataChunks extends BTreeV1 {
  NODE_TYPE = 1;
  private dims: number;

  constructor(fh: ArrayBuffer, offset: number, dims: number) {
    super(fh, offset);
    this.dims = dims;
    this.init();
  }

  protected _read_node(offset: number, nodeLevel: number | null): UnpackedStruct {
    const node = this._read_node_header(offset, nodeLevel);
    offset += _structure_size(this.B_LINK_NODE);

    const keys: UnpackedStruct[] = [];
    const addresses: number[] = [];
    const entriesUsed = node.get('entries_used') as number;

    for (let i = 0; i < entriesUsed; i++) {
      const [chunkSize, filterMask] = struct.unpack_from('<II', this.fh, offset) as [
        number,
        number,
      ];
      offset += 8;

      const fmt = '<' + this.dims.toFixed() + 'Q';
      const fmtSize = struct.calcsize(fmt);
      const chunkOffset = struct.unpack_from(fmt, this.fh, offset) as number[];
      offset += fmtSize;

      const chunkAddress = struct.unpack_from('<Q', this.fh, offset)[0] as number;
      offset += 8;

      keys.push(
        new Map<string, number | number[]>([
          ['chunk_size', chunkSize],
          ['filter_mask', filterMask],
          ['chunk_offset', chunkOffset],
        ])
      );
      addresses.push(chunkAddress);
    }

    node.set('keys', keys);
    node.set('addresses', addresses);
    return node;
  }

  construct_data_from_chunks(
    chunkShape: number[],
    dataShape: number[],
    dtype: Dtype,
    filterPipeline: UnpackedStruct[] | null
  ): unknown[] {
    let itemGetter: string;
    let itemBigEndian: boolean;
    let itemSize: number;
    let _trueDtype: Dtype | null;

    if (dtype instanceof Array) {
      _trueDtype = dtype;
      const dtypeClass = dtype[0];

      if (dtypeClass === 'REFERENCE') {
        const size = dtype[1] as number;
        if (size !== 8) {
          throw new Error("NotImplementedError('Unsupported Reference type')");
        }
        itemGetter = 'getUint64';
        itemBigEndian = false;
        itemSize = 8;
      } else if (dtypeClass === 'VLEN_STRING' || dtypeClass === 'VLEN_SEQUENCE') {
        itemGetter = 'getVLENStruct';
        itemBigEndian = false;
        itemSize = 16;
      } else {
        throw new Error("NotImplementedError('datatype not implemented')");
      }
    } else {
      _trueDtype = null;
      [itemGetter, itemBigEndian, itemSize] = dtype_getter(dtype as string);
    }

    const dataSize = dataShape.reduce((a, b) => a * b, 1);
    const chunkSize = chunkShape.reduce((a, b) => a * b, 1);
    const dims = dataShape.length;

    let currentStride = 1;
    const _chunkStrides = chunkShape.slice().map((d) => {
      const s = currentStride;
      currentStride *= d;
      return s;
    });

    currentStride = 1;
    const dataStrides = dataShape
      .slice()
      .reverse()
      .map((d) => {
        const s = currentStride;
        currentStride *= d;
        return s;
      })
      .reverse();

    const data: unknown[] = new Array(dataSize);
    const chunkBufferSize = chunkSize * itemSize;

    for (const node of this.all_nodes.get(0)!) {
      const nodeKeys = node.get('keys') as UnpackedStruct[];
      const nodeAddresses = node.get('addresses') as number[];
      const nkeys = nodeKeys.length;

      for (let ik = 0; ik < nkeys; ik++) {
        const nodeKey = nodeKeys[ik];
        const addr = nodeAddresses[ik];

        let chunkBuffer: ArrayBuffer;
        if (filterPipeline === null) {
          chunkBuffer = this.fh.slice(addr, addr + chunkBufferSize);
        } else {
          chunkBuffer = this.fh.slice(addr, addr + (nodeKey.get('chunk_size') as number));
          const filterMask = nodeKey.get('filter_mask') as number;
          chunkBuffer = this._filter_chunk(chunkBuffer, filterMask, filterPipeline, itemSize);
        }

        const chunkOffset = (nodeKey.get('chunk_offset') as number[]).slice();
        const apos = chunkOffset.slice();
        const cpos = apos.map(() => 0);
        const cview = new DataView64(chunkBuffer);

        for (let ci = 0; ci < chunkSize; ci++) {
          for (let d = dims - 1; d >= 0; d--) {
            if (cpos[d] >= chunkShape[d]) {
              cpos[d] = 0;
              apos[d] = chunkOffset[d];
              if (d > 0) {
                cpos[d - 1] += 1;
                apos[d - 1] += 1;
              }
            } else {
              break;
            }
          }

          const inbounds = apos.slice(0, -1).every((p, d) => p < dataShape[d]);
          if (inbounds) {
            const cbOffset = ci * itemSize;
            const getter = cview[itemGetter as keyof DataView64] as (
              offset: number,
              littleEndian: boolean,
              size: number
            ) => unknown;
            const datum = getter.call(cview, cbOffset, !itemBigEndian, itemSize);
            const ai = apos.slice(0, -1).reduce((prev, curr, index) => {
              return curr * dataStrides[index] + prev;
            }, 0);
            data[ai] = datum;
          }

          cpos[dims - 1] += 1;
          apos[dims - 1] += 1;
        }
      }
    }

    return data;
  }

  private _filter_chunk(
    chunkBuffer: ArrayBuffer,
    filterMask: number,
    filterPipeline: UnpackedStruct[],
    itemsize: number
  ): ArrayBuffer {
    const numFilters = filterPipeline.length;
    let buf = chunkBuffer.slice(0);

    for (let filterIndex = numFilters - 1; filterIndex >= 0; filterIndex--) {
      // Skip if bit is set in filter_mask
      if (filterMask & (1 << filterIndex)) {
        continue;
      }

      const pipelineEntry = filterPipeline[filterIndex];
      const filterId = pipelineEntry.get('filter_id') as number;
      const _clientData = pipelineEntry.get('client_data') as number[];

      if (Filters.has(filterId)) {
        const filterFn = Filters.get(filterId)!;
        buf = filterFn(buf, itemsize);
      } else {
        throw new Error(
          'NotImplementedError("Filter with id:' + filterId.toFixed() + ' not supported")'
        );
      }
    }

    return buf;
  }
}

// ============================================================================
// B-Tree V2
// ============================================================================

export class BTreeV2 extends AbstractBTree {
  protected B_TREE_HEADER: StructureDefinition = new Map([
    ['signature', '4s'],
    ['version', 'B'],
    ['node_type', 'B'],
    ['node_size', 'I'],
    ['record_size', 'H'],
    ['depth', 'H'],
    ['split_percent', 'B'],
    ['merge_percent', 'B'],
    ['root_address', 'Q'],
    ['root_nrecords', 'H'],
    ['total_nrecords', 'Q'],
  ]);

  protected B_LINK_NODE: StructureDefinition = new Map([
    ['signature', '4s'],
    ['version', 'B'],
    ['node_type', 'B'],
  ]);

  protected NODE_TYPE = 0;
  protected header!: UnpackedStruct;
  protected address_formats!: Map<number, unknown[]>;

  constructor(fh: ArrayBuffer, offset: number) {
    super(fh, offset);
    this.init();
  }

  protected _read_root_node(): void {
    const h = this._read_tree_header(this.offset);
    this.address_formats = this._calculate_address_formats(h);
    this.header = h;
    this.depth = h.get('depth') as number;

    const address = [
      h.get('root_address') as number,
      h.get('root_nrecords') as number,
      h.get('total_nrecords') as number,
    ];
    const rootNode = this._read_node_v2(address, this.depth);
    this._add_node(rootNode);
  }

  protected _read_children(): void {
    let nodeLevel = this.depth!;
    while (nodeLevel > 0) {
      for (const parentNode of this.all_nodes.get(nodeLevel)!) {
        for (const childAddr of parentNode.get('addresses') as number[][]) {
          this._add_node(this._read_node_v2(childAddr, nodeLevel - 1));
        }
      }
      nodeLevel--;
    }
  }

  private _read_tree_header(offset: number): UnpackedStruct {
    const header = _unpack_struct_from(this.B_TREE_HEADER, this.fh, offset);
    return header;
  }

  private _calculate_address_formats(header: UnpackedStruct): Map<number, unknown[]> {
    const nodeSize = header.get('node_size') as number;
    const recordSize = header.get('record_size') as number;
    let nrecordsMax = 0;
    let ntotalrecordsMax = 0;
    const addressFormats = new Map<number, unknown[]>();
    const maxDepth = header.get('depth') as number;

    for (let nodeLevel = 0; nodeLevel <= maxDepth; nodeLevel++) {
      let offsetFmt = '';
      let num1Fmt = '';
      let num2Fmt = '';
      let offsetSize: number, num1Size: number, num2Size: number;

      if (nodeLevel === 0) {
        offsetSize = 0;
        num1Size = 0;
        num2Size = 0;
      } else if (nodeLevel === 1) {
        offsetSize = 8;
        offsetFmt = '<Q';
        num1Size = this._required_bytes(nrecordsMax);
        num1Fmt = this._int_format(num1Size);
        num2Size = 0;
      } else {
        offsetSize = 8;
        offsetFmt = '<Q';
        num1Size = this._required_bytes(nrecordsMax);
        num1Fmt = this._int_format(num1Size);
        num2Size = this._required_bytes(ntotalrecordsMax);
        num2Fmt = this._int_format(num2Size);
      }

      addressFormats.set(nodeLevel, [offsetSize, num1Size, num2Size, offsetFmt, num1Fmt, num2Fmt]);

      if (nodeLevel < maxDepth) {
        const addrSize = offsetSize + num1Size + num2Size;
        nrecordsMax = this._nrecords_max(nodeSize, recordSize, addrSize);
        if (ntotalrecordsMax > 0) {
          ntotalrecordsMax *= nrecordsMax;
        } else {
          ntotalrecordsMax = nrecordsMax;
        }
      }
    }

    return addressFormats;
  }

  private _nrecords_max(nodeSize: number, recordSize: number, addrSize: number): number {
    return Math.floor((nodeSize - 10 - addrSize) / (recordSize + addrSize));
  }

  private _required_bytes(integer: number): number {
    return Math.ceil(bitSize(integer) / 8);
  }

  private _int_format(bytelength: number): string {
    return ['<B', '<H', '<I', '<Q'][bytelength - 1];
  }

  private _read_node_v2(address: number[], nodeLevel: number): UnpackedStruct {
    // eslint-disable-next-line prefer-const -- offset is reassigned, but destructuring requires uniform declaration
    let [offset, nrecords, _ntotalrecords] = address;
    const node = this._read_node_header(offset, nodeLevel);
    offset += _structure_size(this.B_LINK_NODE);

    const recordSize = this.header.get('record_size') as number;
    const keys: UnpackedStruct[] = [];

    for (let i = 0; i < nrecords; i++) {
      const record = this._parse_record(this.fh, offset, recordSize);
      offset += recordSize;
      keys.push(record);
    }

    const addresses: number[][] = [];
    const fmts = this.address_formats.get(nodeLevel)!;

    if (nodeLevel !== 0) {
      const [offsetSize, num1Size, num2Size, offsetFmt, num1Fmt, num2Fmt] = fmts as [
        number,
        number,
        number,
        string,
        string,
        string,
      ];

      for (let j = 0; j <= nrecords; j++) {
        const addressOffset = struct.unpack_from(offsetFmt, this.fh, offset)[0] as number;
        offset += offsetSize;
        const num1 = struct.unpack_from(num1Fmt, this.fh, offset)[0] as number;
        offset += num1Size;
        let num2 = num1;
        if (num2Size > 0) {
          num2 = struct.unpack_from(num2Fmt, this.fh, offset)[0] as number;
          offset += num2Size;
        }
        addresses.push([addressOffset, num1, num2]);
      }
    }

    node.set('keys', keys);
    node.set('addresses', addresses);
    return node;
  }

  protected _read_node_header(offset: number, nodeLevel: number | null): UnpackedStruct {
    const node = _unpack_struct_from(this.B_LINK_NODE, this.fh, offset);
    node.set('node_level', nodeLevel);
    return node;
  }

  *iter_records(): Generator<UnpackedStruct> {
    for (const nodelist of this.all_nodes.values()) {
      for (const node of nodelist) {
        for (const key of node.get('keys') as UnpackedStruct[]) {
          yield key;
        }
      }
    }
  }

  protected _parse_record(_buf: ArrayBuffer, _offset: number, _size: number): UnpackedStruct {
    throw new Error('NotImplementedError');
  }
}

// ============================================================================
// B-Tree V2 Group Names (Type 5)
// ============================================================================

export class BTreeV2GroupNames extends BTreeV2 {
  NODE_TYPE = 5;

  protected _parse_record(buf: ArrayBuffer, offset: number, _size: number): UnpackedStruct {
    const namehash = struct.unpack_from('<I', buf, offset)[0] as number;
    offset += 4;
    return new Map<string, unknown>([
      ['namehash', namehash],
      ['heapid', buf.slice(offset, offset + 7)],
    ]) as UnpackedStruct;
  }
}

// ============================================================================
// B-Tree V2 Group Orders (Type 6)
// ============================================================================

export class BTreeV2GroupOrders extends BTreeV2 {
  NODE_TYPE = 6;

  protected _parse_record(buf: ArrayBuffer, offset: number, _size: number): UnpackedStruct {
    const creationorder = struct.unpack_from('<Q', buf, offset)[0] as number;
    offset += 8;
    return new Map<string, unknown>([
      ['creationorder', creationorder],
      ['heapid', buf.slice(offset, offset + 7)],
    ]) as UnpackedStruct;
  }
}
