/**
 * HDF5 DataObjects - Core data parsing
 */

import { DatatypeMessage } from './datatype-msg.js';
import {
  _structure_size,
  _padded_size,
  _unpack_struct_from,
  struct,
  dtype_getter,
  DataView64,
  assert,
} from './core.js';
import {
  BTreeV1Groups,
  BTreeV1RawDataChunks,
  BTreeV2GroupNames,
  BTreeV2GroupOrders,
} from './btree.js';
import { Heap, SymbolTable, GlobalHeap, FractalHeap } from './misc-low-level.js';
import type { StructureDefinition, UnpackedStruct, Links } from './types/hdf5.js';
import type { Dtype, DataValue } from './types/dtype.js';

// ============================================================================
// Constants
// ============================================================================

const UNDEFINED_ADDRESS = struct.unpack_from(
  '<Q',
  new Uint8Array([255, 255, 255, 255, 255, 255, 255, 255]).buffer
)[0] as number;

// Data Object Message types
// Note: Some constants are prefixed with _ as they are reserved for future use
const _NIL_MSG_TYPE = 0x0000;
const DATASPACE_MSG_TYPE = 0x0001;
const LINK_INFO_MSG_TYPE = 0x0002;
const DATATYPE_MSG_TYPE = 0x0003;
const _FILLVALUE_OLD_MSG_TYPE = 0x0004;
const FILLVALUE_MSG_TYPE = 0x0005;
const LINK_MSG_TYPE = 0x0006;
const _EXTERNAL_DATA_FILES_MSG_TYPE = 0x0007;
const DATA_STORAGE_MSG_TYPE = 0x0008;
const _BOGUS_MSG_TYPE = 0x0009;
const _GROUP_INFO_MSG_TYPE = 0x000a;
const DATA_STORAGE_FILTER_PIPELINE_MSG_TYPE = 0x000b;
const ATTRIBUTE_MSG_TYPE = 0x000c;
const _OBJECT_COMMENT_MSG_TYPE = 0x000d;
const _OBJECT_MODIFICATION_TIME_OLD_MSG_TYPE = 0x000e;
const _SHARED_MSG_TABLE_MSG_TYPE = 0x000f;
const OBJECT_CONTINUATION_MSG_TYPE = 0x0010;
const SYMBOL_TABLE_MSG_TYPE = 0x0011;
const _OBJECT_MODIFICATION_TIME_MSG_TYPE = 0x0012;
const _BTREE_K_VALUE_MSG_TYPE = 0x0013;
const _DRIVER_INFO_MSG_TYPE = 0x0014;
const _ATTRIBUTE_INFO_MSG_TYPE = 0x0015;
const _OBJECT_REFERENCE_COUNT_MSG_TYPE = 0x0016;
const _FILE_SPACE_INFO_MSG_TYPE = 0x0018;

// ============================================================================
// Structure Definitions
// ============================================================================

const GLOBAL_HEAP_ID: StructureDefinition = new Map([
  ['collection_address', 'Q'],
  ['object_index', 'I'],
]);
const _GLOBAL_HEAP_ID_SIZE = _structure_size(GLOBAL_HEAP_ID);

const ATTR_MSG_HEADER_V1: StructureDefinition = new Map([
  ['version', 'B'],
  ['reserved', 'B'],
  ['name_size', 'H'],
  ['datatype_size', 'H'],
  ['dataspace_size', 'H'],
]);
const ATTR_MSG_HEADER_V1_SIZE = _structure_size(ATTR_MSG_HEADER_V1);

const ATTR_MSG_HEADER_V3: StructureDefinition = new Map([
  ['version', 'B'],
  ['flags', 'B'],
  ['name_size', 'H'],
  ['datatype_size', 'H'],
  ['dataspace_size', 'H'],
  ['character_set_encoding', 'B'],
]);
const ATTR_MSG_HEADER_V3_SIZE = _structure_size(ATTR_MSG_HEADER_V3);

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

const DATASPACE_MSG_HEADER_V1: StructureDefinition = new Map([
  ['version', 'B'],
  ['dimensionality', 'B'],
  ['flags', 'B'],
  ['reserved_0', 'B'],
  ['reserved_1', 'I'],
]);
const DATASPACE_MSG_HEADER_V1_SIZE = _structure_size(DATASPACE_MSG_HEADER_V1);

const DATASPACE_MSG_HEADER_V2: StructureDefinition = new Map([
  ['version', 'B'],
  ['dimensionality', 'B'],
  ['flags', 'B'],
  ['type', 'B'],
]);
const DATASPACE_MSG_HEADER_V2_SIZE = _structure_size(DATASPACE_MSG_HEADER_V2);

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

const LINK_INFO_MSG1: StructureDefinition = new Map([
  ['heap_address', 'Q'],
  ['name_btree_address', 'Q'],
]);

const LINK_INFO_MSG2: StructureDefinition = new Map([
  ['heap_address', 'Q'],
  ['name_btree_address', 'Q'],
  ['order_btree_address', 'Q'],
]);

const FILLVAL_MSG_V1V2: StructureDefinition = new Map([
  ['version', 'B'],
  ['space_allocation_time', 'B'],
  ['fillvalue_write_time', 'B'],
  ['fillvalue_defined', 'B'],
]);
const FILLVAL_MSG_V1V2_SIZE = _structure_size(FILLVAL_MSG_V1V2);

const FILLVAL_MSG_V3: StructureDefinition = new Map([
  ['version', 'B'],
  ['flags', 'B'],
]);
const FILLVAL_MSG_V3_SIZE = _structure_size(FILLVAL_MSG_V3);

const FILTER_PIPELINE_DESCR_V1: StructureDefinition = new Map([
  ['filter_id', 'H'],
  ['name_length', 'H'],
  ['flags', 'H'],
  ['client_data_values', 'H'],
]);
const FILTER_PIPELINE_DESCR_V1_SIZE = _structure_size(FILTER_PIPELINE_DESCR_V1);

// ============================================================================
// Helper Functions
// ============================================================================

function determine_data_shape(buf: ArrayBuffer, offset: number): number[] {
  const version = struct.unpack_from('<B', buf, offset)[0] as number;
  let header: UnpackedStruct;

  if (version === 1) {
    header = _unpack_struct_from(DATASPACE_MSG_HEADER_V1, buf, offset);
    assert(header.get('version') === 1);
    offset += DATASPACE_MSG_HEADER_V1_SIZE;
  } else if (version === 2) {
    header = _unpack_struct_from(DATASPACE_MSG_HEADER_V2, buf, offset);
    assert(header.get('version') === 2);
    offset += DATASPACE_MSG_HEADER_V2_SIZE;
  } else {
    throw new Error("InvalidHDF5File('unknown dataspace message version')");
  }

  const ndims = header.get('dimensionality') as number;
  const dimSizes = struct.unpack_from('<' + (ndims * 2).toFixed() + 'I', buf, offset) as number[];
  return dimSizes.filter((_s, i) => i % 2 === 0);
}

// ============================================================================
// DataObjects Class
// ============================================================================

export class DataObjects {
  fh: ArrayBuffer;
  msgs: UnpackedStruct[];
  msg_data: ArrayBuffer;
  offset: number;
  protected _global_heaps: Record<number, GlobalHeap>;
  protected _header: UnpackedStruct;
  protected _filter_pipeline: UnpackedStruct[] | null;
  protected _chunk_params_set: boolean;
  protected _chunks: number[] | null;
  protected _chunk_dims: number | null;
  protected _chunk_address: number | null;

  constructor(fh: ArrayBuffer, offset: number) {
    const versionHint = struct.unpack_from('<B', fh, offset)[0] as number;
    let msgs: UnpackedStruct[];
    let msgData: ArrayBuffer;
    let header: UnpackedStruct;

    if (versionHint === 1) {
      [msgs, msgData, header] = this._parse_v1_objects(fh, offset);
    } else if (versionHint === 'O'.charCodeAt(0)) {
      [msgs, msgData, header] = this._parse_v2_objects(fh, offset);
    } else {
      throw new Error("InvalidHDF5File('unknown Data Object Header')");
    }

    this.fh = fh;
    this.msgs = msgs;
    this.msg_data = msgData;
    this.offset = offset;
    this._global_heaps = {};
    this._header = header;
    this._filter_pipeline = null;
    this._chunk_params_set = false;
    this._chunks = null;
    this._chunk_dims = null;
    this._chunk_address = null;
  }

  get dtype(): Dtype {
    const msg = this.find_msg_type(DATATYPE_MSG_TYPE)[0];
    const msgOffset = msg.get('offset_to_message') as number;
    return new DatatypeMessage(this.fh, msgOffset).dtype;
  }

  get chunks(): number[] | null {
    this._get_chunk_params();
    return this._chunks;
  }

  get shape(): number[] {
    const msg = this.find_msg_type(DATASPACE_MSG_TYPE)[0];
    const msgOffset = msg.get('offset_to_message') as number;
    return determine_data_shape(this.fh, msgOffset);
  }

  get filter_pipeline(): UnpackedStruct[] | null {
    if (this._filter_pipeline !== null) {
      return this._filter_pipeline;
    }

    const filterMsgs = this.find_msg_type(DATA_STORAGE_FILTER_PIPELINE_MSG_TYPE);
    if (!filterMsgs.length) {
      this._filter_pipeline = null;
      return this._filter_pipeline;
    }

    let offset = filterMsgs[0].get('offset_to_message') as number;
    const [version, nfilters] = struct.unpack_from('<BB', this.fh, offset) as [number, number];
    offset += struct.calcsize('<BB');

    const filters: UnpackedStruct[] = [];

    if (version === 1) {
      offset += struct.calcsize('<HI');

      for (let i = 0; i < nfilters; i++) {
        const filterInfo = _unpack_struct_from(FILTER_PIPELINE_DESCR_V1, this.fh, offset);
        offset += FILTER_PIPELINE_DESCR_V1_SIZE;

        const paddedNameLength = _padded_size(filterInfo.get('name_length') as number, 8);
        const fmt = '<' + paddedNameLength.toFixed() + 's';
        const filterName = struct.unpack_from(fmt, this.fh, offset)[0] as string;
        filterInfo.set('filter_name', filterName);
        offset += paddedNameLength;

        const clientDataValues = filterInfo.get('client_data_values') as number;
        const clientFmt = '<' + clientDataValues.toFixed() + 'I';
        const clientData = struct.unpack_from(clientFmt, this.fh, offset) as number[];
        filterInfo.set('client_data', clientData);
        offset += 4 * clientDataValues;

        if (clientDataValues % 2) {
          offset += 4;
        }

        filters.push(filterInfo);
      }
    } else if (version === 2) {
      for (let nf = 0; nf < nfilters; nf++) {
        const filterInfo: UnpackedStruct = new Map();
        const buf = this.fh;

        const filterId = struct.unpack_from('<H', buf, offset)[0] as number;
        offset += 2;
        filterInfo.set('filter_id', filterId);

        let nameLength = 0;
        if (filterId > 255) {
          nameLength = struct.unpack_from('<H', buf, offset)[0] as number;
          offset += 2;
        }

        const flags = struct.unpack_from('<H', buf, offset)[0] as number;
        offset += 2;
        const optional = (flags & 1) > 0;
        filterInfo.set('optional', optional);

        const numClientValues = struct.unpack_from('<H', buf, offset)[0] as number;
        offset += 2;

        let name: string | undefined;
        if (nameLength > 0) {
          name = struct.unpack_from(`${nameLength}s`, buf, offset)[0] as string;
          offset += nameLength;
        }
        filterInfo.set('name', name);

        const clientValues = struct.unpack_from(`<${numClientValues}i`, buf, offset) as number[];
        offset += 4 * numClientValues;
        filterInfo.set('client_data', clientValues);
        filterInfo.set('client_data_values', numClientValues);

        filters.push(filterInfo);
      }
    } else {
      throw new Error(`version ${version} is not supported`);
    }

    this._filter_pipeline = filters;
    return this._filter_pipeline;
  }

  find_msg_type(msgType: number): UnpackedStruct[] {
    return this.msgs.filter((m) => m.get('type') === msgType);
  }

  get_attributes(): Record<string, DataValue> {
    const attrs: Record<string, DataValue> = {};
    const attrMsgs = this.find_msg_type(ATTRIBUTE_MSG_TYPE);

    for (const msg of attrMsgs) {
      const offset = msg.get('offset_to_message') as number;
      const [name, value] = this.unpack_attribute(offset);
      attrs[name] = value;
    }

    return attrs;
  }

  get fillvalue(): DataValue {
    const msg = this.find_msg_type(FILLVALUE_MSG_TYPE)[0];
    let offset = msg.get('offset_to_message') as number;

    const version = struct.unpack_from('<B', this.fh, offset)[0] as number;
    let info: UnpackedStruct;
    let isDefined: boolean;
    let size: number;
    let fillvalue: DataValue;

    if (version === 1 || version === 2) {
      info = _unpack_struct_from(FILLVAL_MSG_V1V2, this.fh, offset);
      offset += FILLVAL_MSG_V1V2_SIZE;
      isDefined = (info.get('fillvalue_defined') as number) !== 0;
    } else if (version === 3) {
      info = _unpack_struct_from(FILLVAL_MSG_V3, this.fh, offset);
      offset += FILLVAL_MSG_V3_SIZE;
      isDefined = ((info.get('flags') as number) & 0b00100000) !== 0;
    } else {
      throw new Error('InvalidHDF5File("Unknown fillvalue msg version: "' + String(version));
    }

    if (isDefined) {
      size = struct.unpack_from('<I', this.fh, offset)[0] as number;
      offset += 4;
    } else {
      size = 0;
    }

    if (size) {
      const [getter, bigEndian] = dtype_getter(this.dtype as string);
      const payloadView = new DataView64(this.fh);
      const method = payloadView[getter as keyof DataView64] as (
        offset: number,
        littleEndian: boolean,
        size: number
      ) => DataValue;
      fillvalue = method.call(payloadView, offset, !bigEndian, size);
    } else {
      fillvalue = 0;
    }

    return fillvalue;
  }

  unpack_attribute(offset: number): [string, DataValue] {
    const version = struct.unpack_from('<B', this.fh, offset)[0] as number;
    let attrMap: UnpackedStruct;
    let paddingMultiple: number;

    if (version === 1) {
      attrMap = _unpack_struct_from(ATTR_MSG_HEADER_V1, this.fh, offset);
      assert(attrMap.get('version') === 1);
      offset += ATTR_MSG_HEADER_V1_SIZE;
      paddingMultiple = 8;
    } else if (version === 3) {
      attrMap = _unpack_struct_from(ATTR_MSG_HEADER_V3, this.fh, offset);
      assert(attrMap.get('version') === 3);
      offset += ATTR_MSG_HEADER_V3_SIZE;
      paddingMultiple = 1;
    } else {
      throw new Error('unsupported attribute message version: ' + version);
    }

    const nameSize = attrMap.get('name_size') as number;
    let name = struct.unpack_from('<' + nameSize.toFixed() + 's', this.fh, offset)[0] as string;
    // eslint-disable-next-line no-control-regex -- intentional null byte removal
    name = name.replace(/\x00$/, '');
    offset += _padded_size(nameSize, paddingMultiple);

    let dtype: Dtype;
    try {
      dtype = new DatatypeMessage(this.fh, offset).dtype;
    } catch (_e) {
      console.warn('Attribute ' + name + ' type not implemented, set to null.');
      return [name, null];
    }

    offset += _padded_size(attrMap.get('datatype_size') as number, paddingMultiple);

    const shape = this.determine_data_shape(this.fh, offset);
    const items = shape.reduce((a, b) => a * b, 1);
    offset += _padded_size(attrMap.get('dataspace_size') as number, paddingMultiple);

    const valueArray = this._attr_value(dtype, this.fh, items, offset);
    const value: DataValue = shape.length === 0 ? valueArray[0] : valueArray;

    return [name, value];
  }

  determine_data_shape(buf: ArrayBuffer, offset: number): number[] {
    const version = struct.unpack_from('<B', buf, offset)[0] as number;
    let header: UnpackedStruct;

    if (version === 1) {
      header = _unpack_struct_from(DATASPACE_MSG_HEADER_V1, buf, offset);
      assert(header.get('version') === 1);
      offset += DATASPACE_MSG_HEADER_V1_SIZE;
    } else if (version === 2) {
      header = _unpack_struct_from(DATASPACE_MSG_HEADER_V2, buf, offset);
      assert(header.get('version') === 2);
      offset += DATASPACE_MSG_HEADER_V2_SIZE;
    } else {
      throw new Error('unknown dataspace message version');
    }

    const ndims = header.get('dimensionality') as number;
    return struct.unpack_from('<' + ndims.toFixed() + 'Q', buf, offset) as number[];
  }

  protected _attr_value(
    dtype: Dtype,
    buf: ArrayBuffer,
    count: number,
    offset: number
  ): DataValue[] {
    const value: DataValue[] = new Array(count);

    if (dtype instanceof Array) {
      const dtypeClass = dtype[0] as string;

      for (let i = 0; i < count; i++) {
        if (dtypeClass === 'VLEN_STRING') {
          const characterSet = dtype[2] as number;
          const [_vlen, vlenData] = this._vlen_size_and_data(buf, offset);
          const encoding = characterSet === 0 ? 'ascii' : 'utf-8';
          const decoder = new TextDecoder(encoding);
          value[i] = decoder.decode(vlenData);
          offset += 16;
        } else if (dtypeClass === 'REFERENCE') {
          const address = struct.unpack_from('<Q', buf, offset) as number[];
          value[i] = address;
          offset += 8;
        } else if (dtypeClass === 'VLEN_SEQUENCE') {
          const baseDtype = dtype[1] as Dtype;
          const [vlen, vlenData] = this._vlen_size_and_data(buf, offset);
          value[i] = this._attr_value(baseDtype, vlenData, vlen, 0);
          offset += 16;
        } else {
          throw new Error('NotImplementedError');
        }
      }
    } else {
      const [getter, bigEndian, size] = dtype_getter(dtype as string);
      const view = new DataView64(buf, 0);
      const method = view[getter as keyof DataView64] as (
        offset: number,
        littleEndian: boolean,
        size: number
      ) => DataValue;

      for (let i = 0; i < count; i++) {
        value[i] = method.call(view, offset, !bigEndian, size);
        offset += size;
      }
    }

    return value;
  }

  protected _vlen_size_and_data(buf: ArrayBuffer, offset: number): [number, ArrayBuffer] {
    const vlenSize = struct.unpack_from('<I', buf, offset)[0] as number;
    const gheapId = _unpack_struct_from(GLOBAL_HEAP_ID, buf, offset + 4);
    const gheapAddress = gheapId.get('collection_address') as number;
    assert(gheapAddress < Number.MAX_SAFE_INTEGER);

    let gheap: GlobalHeap;
    if (!(gheapAddress in this._global_heaps)) {
      gheap = new GlobalHeap(this.fh, gheapAddress);
      this._global_heaps[gheapAddress] = gheap;
    }
    gheap = this._global_heaps[gheapAddress];

    const vlenData = gheap.objects.get(gheapId.get('object_index') as number)!;
    return [vlenSize, vlenData];
  }

  protected _parse_v1_objects(
    buf: ArrayBuffer,
    offset: number
  ): [UnpackedStruct[], ArrayBuffer, UnpackedStruct] {
    const header = _unpack_struct_from(OBJECT_HEADER_V1, buf, offset);
    assert(header.get('version') === 1);

    const totalHeaderMessages = header.get('total_header_messages') as number;
    let blockSize = header.get('object_header_size') as number;
    let blockOffset = offset + _structure_size(OBJECT_HEADER_V1);
    const msgData = buf.slice(blockOffset, blockOffset + blockSize);
    const objectHeaderBlocks: Array<[number, number]> = [[blockOffset, blockSize]];
    let currentBlock = 0;
    let localOffset = 0;

    const msgs: UnpackedStruct[] = new Array(totalHeaderMessages);

    for (let i = 0; i < totalHeaderMessages; i++) {
      if (localOffset >= blockSize) {
        [blockOffset, blockSize] = objectHeaderBlocks[++currentBlock];
        localOffset = 0;
      }

      const msg = _unpack_struct_from(HEADER_MSG_INFO_V1, buf, blockOffset + localOffset);
      const offsetToMessage = blockOffset + localOffset + HEADER_MSG_INFO_V1_SIZE;
      msg.set('offset_to_message', offsetToMessage);

      if (msg.get('type') === OBJECT_CONTINUATION_MSG_TYPE) {
        const [fhOff, size] = struct.unpack_from('<QQ', buf, offsetToMessage) as [number, number];
        objectHeaderBlocks.push([fhOff, size]);
      }

      localOffset += HEADER_MSG_INFO_V1_SIZE + (msg.get('size') as number);
      msgs[i] = msg;
    }

    return [msgs, msgData, header];
  }

  protected _parse_v2_objects(
    buf: ArrayBuffer,
    offset: number
  ): [UnpackedStruct[], ArrayBuffer, UnpackedStruct] {
    const [header, creationOrderSize, startOffset] = this._parse_v2_header(buf, offset);
    offset = startOffset;

    const msgs: UnpackedStruct[] = [];
    let blockSize = header.get('size_of_chunk_0') as number;
    const msgData = buf.slice(offset, offset + blockSize);

    const objectHeaderBlocks: Array<[number, number]> = [[offset, blockSize]];
    let currentBlock = 0;
    let localOffset = 0;
    let blockOffset = offset;

    while (true) {
      if (localOffset >= blockSize - HEADER_MSG_INFO_V2_SIZE) {
        const nextBlock = objectHeaderBlocks[++currentBlock];
        if (nextBlock === undefined) {
          break;
        }
        [blockOffset, blockSize] = nextBlock;
        localOffset = 0;
      }

      const msg = _unpack_struct_from(HEADER_MSG_INFO_V2, buf, blockOffset + localOffset);
      const offsetToMessage =
        blockOffset + localOffset + HEADER_MSG_INFO_V2_SIZE + creationOrderSize;
      msg.set('offset_to_message', offsetToMessage);

      if (msg.get('type') === OBJECT_CONTINUATION_MSG_TYPE) {
        const [fhOff, size] = struct.unpack_from('<QQ', buf, offsetToMessage) as [number, number];
        objectHeaderBlocks.push([fhOff + 4, size - 4]);
      }

      localOffset += HEADER_MSG_INFO_V2_SIZE + (msg.get('size') as number) + creationOrderSize;
      msgs.push(msg);
    }

    return [msgs, msgData, header];
  }

  protected _parse_v2_header(buf: ArrayBuffer, offset: number): [UnpackedStruct, number, number] {
    const header = _unpack_struct_from(OBJECT_HEADER_V2, buf, offset);
    offset += _structure_size(OBJECT_HEADER_V2);
    assert(header.get('version') === 2);

    const flags = header.get('flags') as number;
    const creationOrderSize = flags & 0b00000100 ? 2 : 0;

    assert((flags & 0b00010000) === 0);

    if (flags & 0b00100000) {
      const times = struct.unpack_from('<4I', buf, offset) as number[];
      offset += 16;
      header.set('access_time', times[0]);
      header.set('modification_time', times[1]);
      header.set('change_time', times[2]);
      header.set('birth_time', times[3]);
    }

    const chunkFmt = ['<B', '<H', '<I', '<Q'][flags & 0b00000011];
    header.set('size_of_chunk_0', struct.unpack_from(chunkFmt, buf, offset)[0] as number);
    offset += struct.calcsize(chunkFmt);

    return [header, creationOrderSize, offset];
  }

  get_links(): Links {
    return Object.fromEntries(this.iter_links());
  }

  *iter_links(): Generator<[string, string | number]> {
    for (const msg of this.msgs) {
      if (msg.get('type') === SYMBOL_TABLE_MSG_TYPE) {
        yield* this._iter_links_from_symbol_tables(msg);
      } else if (msg.get('type') === LINK_MSG_TYPE) {
        yield this._get_link_from_link_msg(msg);
      } else if (msg.get('type') === LINK_INFO_MSG_TYPE) {
        yield* this._iter_link_from_link_info_msg(msg);
      }
    }
  }

  protected *_iter_links_from_symbol_tables(
    symTblMsg: UnpackedStruct
  ): Generator<[string, string | number]> {
    assert(symTblMsg.get('size') === 16);
    const data = _unpack_struct_from(
      SYMBOL_TABLE_MSG,
      this.fh,
      symTblMsg.get('offset_to_message') as number
    );
    yield* this._iter_links_btree_v1(
      data.get('btree_address') as number,
      data.get('heap_address') as number
    );
  }

  protected *_iter_links_btree_v1(
    btreeAddress: number,
    heapAddress: number
  ): Generator<[string, string | number]> {
    const btree = new BTreeV1Groups(this.fh, btreeAddress);
    const heap = new Heap(this.fh, heapAddress);

    for (const symbolTableAddress of btree.symbol_table_addresses()) {
      const table = new SymbolTable(this.fh, symbolTableAddress);
      table.assign_name(heap);
      yield* Object.entries(table.get_links(heap)) as Array<[string, string | number]>;
    }
  }

  protected _get_link_from_link_msg(linkMsg: UnpackedStruct): [string, string | number] {
    const offset = linkMsg.get('offset_to_message') as number;
    return this._decode_link_msg(this.fh, offset)[1];
  }

  protected _decode_link_msg(
    data: ArrayBuffer,
    offset: number
  ): [number | undefined, [string, string | number]] {
    const [version, flags] = struct.unpack_from('<BB', data, offset) as [number, number];
    offset += 2;
    assert(version === 1);

    const sizeOfLengthOfLinkName = 2 ** (flags & 3);
    const linkTypeFieldPresent = (flags & (2 ** 3)) > 0;
    const linkNameCharacterSetFieldPresent = (flags & (2 ** 4)) > 0;
    const ordered = (flags & (2 ** 2)) > 0;

    let linkType: number;
    if (linkTypeFieldPresent) {
      linkType = struct.unpack_from('<B', data, offset)[0] as number;
      offset += 1;
    } else {
      linkType = 0;
    }
    assert([0, 1].includes(linkType));

    let creationorder: number | undefined;
    if (ordered) {
      creationorder = struct.unpack_from('<Q', data, offset)[0] as number;
      offset += 8;
    }

    let linkNameCharacterSet = 0;
    if (linkNameCharacterSetFieldPresent) {
      linkNameCharacterSet = struct.unpack_from('<B', data, offset)[0] as number;
      offset += 1;
    }

    const encoding = linkNameCharacterSet === 0 ? 'ascii' : 'utf-8';
    const nameSizeFmt = ['<B', '<H', '<I', '<Q'][flags & 3];
    const nameSize = struct.unpack_from(nameSizeFmt, data, offset)[0] as number;
    offset += sizeOfLengthOfLinkName;

    const name = new TextDecoder(encoding).decode(data.slice(offset, offset + nameSize));
    offset += nameSize;

    let address: string | number;
    if (linkType === 0) {
      address = struct.unpack_from('<Q', data, offset)[0] as number;
    } else if (linkType === 1) {
      const lengthOfSoftLinkValue = struct.unpack_from('<H', data, offset)[0] as number;
      offset += 2;
      address = new TextDecoder(encoding).decode(
        data.slice(offset, offset + lengthOfSoftLinkValue)
      );
    } else {
      address = 0;
    }

    return [creationorder, [name, address]];
  }

  protected *_iter_link_from_link_info_msg(
    infoMsg: UnpackedStruct
  ): Generator<[string, string | number]> {
    const offset = infoMsg.get('offset_to_message') as number;
    const data = this._decode_link_info_msg(this.fh, offset);

    const heapAddress = data.get('heap_address') as number | null;
    const nameBtreeAddress = data.get('name_btree_address') as number | null;
    const orderBtreeAddress = data.get('order_btree_address') as number | null;

    if (nameBtreeAddress !== null) {
      yield* this._iter_links_btree_v2(nameBtreeAddress, orderBtreeAddress, heapAddress!);
    }
  }

  protected *_iter_links_btree_v2(
    nameBtreeAddress: number,
    orderBtreeAddress: number | null,
    heapAddress: number
  ): Generator<[string, string | number]> {
    const heap = new FractalHeap(this.fh, heapAddress);
    let btree: BTreeV2GroupNames | BTreeV2GroupOrders;
    const ordered = orderBtreeAddress !== null;

    if (ordered) {
      btree = new BTreeV2GroupOrders(this.fh, orderBtreeAddress!);
    } else {
      btree = new BTreeV2GroupNames(this.fh, nameBtreeAddress);
    }

    const items = new Map<string | number, [string, string | number]>();

    for (const record of btree.iter_records()) {
      const data = heap.get_data(record.get('heapid') as ArrayBuffer);
      const [creationorder, item] = this._decode_link_msg(data, 0);
      const key = ordered ? (creationorder as number) : item[0];
      items.set(key, item);
    }

    const sortedKeys = Array.from(items.keys()).sort((a, b) => {
      if (typeof a === 'number' && typeof b === 'number') return a - b;
      return String(a).localeCompare(String(b));
    });

    for (const key of sortedKeys) {
      yield items.get(key)!;
    }
  }

  protected _decode_link_info_msg(data: ArrayBuffer, offset: number): UnpackedStruct {
    const [version, flags] = struct.unpack_from('<BB', data, offset) as [number, number];
    assert(version === 0);
    offset += 2;

    if ((flags & 1) > 0) {
      offset += 8;
    }

    const fmt = (flags & 2) > 0 ? LINK_INFO_MSG2 : LINK_INFO_MSG1;
    const linkInfo = _unpack_struct_from(fmt, data, offset);

    const output: UnpackedStruct = new Map();
    for (const [k, v] of linkInfo.entries()) {
      output.set(k, v === UNDEFINED_ADDRESS ? null : v);
    }

    return output;
  }

  get is_dataset(): boolean {
    return this.find_msg_type(DATASPACE_MSG_TYPE).length > 0;
  }

  get_data(): DataValue[] {
    const msg = this.find_msg_type(DATA_STORAGE_MSG_TYPE)[0];
    const msgOffset = msg.get('offset_to_message') as number;
    const [_version, _dims, layoutClass, propertyOffset] =
      this._get_data_message_properties(msgOffset);

    if (layoutClass === 0) {
      throw new Error('Compact storage of DataObject not implemented');
    } else if (layoutClass === 1) {
      return this._get_contiguous_data(propertyOffset);
    } else if (layoutClass === 2) {
      return this._get_chunked_data(msgOffset);
    }

    throw new Error(`Unknown layout class: ${layoutClass}`);
  }

  protected _get_data_message_properties(msgOffset: number): [number, number, number, number] {
    let dims: number;
    let layoutClass: number;
    let propertyOffset: number;

    const [version, arg1, arg2] = struct.unpack_from('<BBB', this.fh, msgOffset) as [
      number,
      number,
      number,
    ];

    if (version === 1 || version === 2) {
      dims = arg1;
      layoutClass = arg2;
      propertyOffset = msgOffset;
      propertyOffset += struct.calcsize('<BBB');
      propertyOffset += struct.calcsize('<BI');
      assert(layoutClass === 1 || layoutClass === 2);
    } else if (version === 3 || version === 4) {
      layoutClass = arg1;
      propertyOffset = msgOffset;
      propertyOffset += struct.calcsize('<BB');
      dims = 0;
    } else {
      throw new Error(`Unsupported data message version: ${version}`);
    }

    assert(version >= 1 && version <= 4);
    return [version, dims, layoutClass, propertyOffset];
  }

  protected _get_contiguous_data(propertyOffset: number): DataValue[] {
    const [dataOffset] = struct.unpack_from('<Q', this.fh, propertyOffset) as [number];

    if (dataOffset === UNDEFINED_ADDRESS) {
      const size = this.shape.reduce((a, b) => a * b, 1);
      return new Array(size);
    }

    const fullsize = this.shape.reduce((a, b) => a * b, 1);
    const dtype = this.dtype;

    if (!(dtype instanceof Array)) {
      if (/[<>=!@|]?(i|u|f|S)(\d*)/.test(dtype as string)) {
        const [itemGetter, itemIsBigEndian, itemSize] = dtype_getter(dtype as string);
        const output: DataValue[] = new Array(fullsize);
        const view = new DataView64(this.fh);
        const method = view[itemGetter as keyof DataView64] as (
          offset: number,
          littleEndian: boolean,
          size: number
        ) => DataValue;

        for (let i = 0; i < fullsize; i++) {
          output[i] = method.call(view, dataOffset + i * itemSize, !itemIsBigEndian, itemSize);
        }
        return output;
      } else {
        throw new Error('not Implemented - no proper dtype defined');
      }
    } else {
      const dtypeClass = dtype[0] as string;

      if (dtypeClass === 'REFERENCE') {
        const size = dtype[1] as number;
        if (size !== 8) {
          throw new Error("NotImplementedError('Unsupported Reference type')");
        }
        const refAddresses = this.fh.slice(dataOffset, dataOffset + fullsize);
        return [refAddresses] as DataValue[];
      } else if (dtypeClass === 'VLEN_STRING') {
        const characterSet = dtype[2] as number;
        const encoding = characterSet === 0 ? 'ascii' : 'utf-8';
        const decoder = new TextDecoder(encoding);
        const value: DataValue[] = [];
        let offset = dataOffset;

        for (let i = 0; i < fullsize; i++) {
          const [_vlen, vlenData] = this._vlen_size_and_data(this.fh, offset);
          value[i] = decoder.decode(vlenData);
          offset += 16;
        }
        return value;
      } else {
        throw new Error("NotImplementedError('datatype not implemented')");
      }
    }
  }

  protected _get_chunked_data(_offset: number): DataValue[] {
    this._get_chunk_params();

    if (this._chunk_address === UNDEFINED_ADDRESS) {
      return [];
    }

    const chunkBtree = new BTreeV1RawDataChunks(this.fh, this._chunk_address!, this._chunk_dims!);
    const data = chunkBtree.construct_data_from_chunks(
      this.chunks!,
      this.shape,
      this.dtype,
      this.filter_pipeline
    );

    if (this.dtype instanceof Array && /^VLEN/.test(this.dtype[0] as string)) {
      const dtypeClass = this.dtype[0] as string;

      for (let i = 0; i < data.length; i++) {
        const [_itemSize, gheapAddress, objectIndex] = data[i] as [number, number, number];
        let gheap: GlobalHeap;

        if (!(gheapAddress in this._global_heaps)) {
          gheap = new GlobalHeap(this.fh, gheapAddress);
          this._global_heaps[gheapAddress] = gheap;
        } else {
          gheap = this._global_heaps[gheapAddress];
        }

        const vlenData = gheap.objects.get(objectIndex)!;

        if (dtypeClass === 'VLEN_STRING') {
          const characterSet = this.dtype[2] as number;
          const encoding = characterSet === 0 ? 'ascii' : 'utf-8';
          const decoder = new TextDecoder(encoding);
          data[i] = decoder.decode(vlenData);
        }
      }
    }

    return data as DataValue[];
  }

  protected _get_chunk_params(): void {
    if (this._chunk_params_set) {
      return;
    }

    this._chunk_params_set = true;
    const msg = this.find_msg_type(DATA_STORAGE_MSG_TYPE)[0];
    const offset = msg.get('offset_to_message') as number;
    const [version, dims, layoutClass, propertyOffset] = this._get_data_message_properties(offset);

    if (layoutClass !== 2) {
      return;
    }

    let address: number;
    let dataOffset: number;
    let chunkDims: number;

    if (version === 1 || version === 2) {
      address = struct.unpack_from('<Q', this.fh, propertyOffset)[0] as number;
      dataOffset = propertyOffset + struct.calcsize('<Q');
      chunkDims = dims;
    } else if (version === 3) {
      [chunkDims, address] = struct.unpack_from('<BQ', this.fh, propertyOffset) as [number, number];
      dataOffset = propertyOffset + struct.calcsize('<BQ');
    } else {
      throw new Error(`Unsupported version: ${version}`);
    }

    assert(version >= 1 && version <= 3);

    const fmt = '<' + (chunkDims - 1).toFixed() + 'I';
    const chunkShape = struct.unpack_from(fmt, this.fh, dataOffset) as number[];

    this._chunks = chunkShape;
    this._chunk_dims = chunkDims;
    this._chunk_address = address;
  }
}

// Export message type constants for use in async-dataobjects
export {
  SYMBOL_TABLE_MSG_TYPE,
  LINK_MSG_TYPE,
  LINK_INFO_MSG_TYPE,
  DATA_STORAGE_MSG_TYPE,
  OBJECT_CONTINUATION_MSG_TYPE,
  UNDEFINED_ADDRESS,
};
