/**
 * Core utilities for binary data parsing
 * Provides struct unpacking similar to Python's struct module
 */

import type { StructureDefinition, UnpackedStruct } from './types/hdf5.js';

// ============================================================================
// Format Character Mappings
// ============================================================================

const GETTERS: Record<string, string> = {
  s: 'getUint8',
  b: 'getInt8',
  B: 'getUint8',
  h: 'getInt16',
  H: 'getUint16',
  i: 'getInt32',
  I: 'getUint32',
  l: 'getInt32',
  L: 'getUint32',
  q: 'getInt64',
  Q: 'getUint64',
  e: 'getFloat16',
  f: 'getFloat32',
  d: 'getFloat64',
};

const BYTE_LENGTHS: Record<string, number> = {
  s: 1,
  b: 1,
  B: 1,
  h: 2,
  H: 2,
  i: 4,
  I: 4,
  l: 4,
  L: 4,
  q: 8,
  Q: 8,
  e: 2,
  f: 4,
  d: 8,
};

// ============================================================================
// Struct Class
// ============================================================================

class Struct {
  private bigEndian: boolean;
  private fmtSizeRegex: string;

  constructor() {
    this.bigEndian = isBigEndian();
    const allFormats = Object.keys(BYTE_LENGTHS).join('');
    this.fmtSizeRegex = '(\\d*)([' + allFormats + '])';
  }

  calcsize(fmt: string): number {
    let size = 0;
    const regex = new RegExp(this.fmtSizeRegex, 'g');
    let match: RegExpExecArray | null;

    while ((match = regex.exec(fmt)) !== null) {
      const n = parseInt(match[1] || '1', 10);
      const f = match[2];
      const subsize = BYTE_LENGTHS[f];
      size += n * subsize;
    }
    return size;
  }

  _is_big_endian(fmt: string): boolean {
    if (/^</.test(fmt)) {
      return false;
    } else if (/^(!|>)/.test(fmt)) {
      return true;
    }
    return this.bigEndian;
  }

  unpack_from(fmt: string, buffer: ArrayBuffer, offset: number = 0): unknown[] {
    offset = Number(offset || 0);
    const view = new DataView64(buffer, 0);
    const output: unknown[] = [];
    const bigEndian = this._is_big_endian(fmt);
    const regex = new RegExp(this.fmtSizeRegex, 'g');
    let match: RegExpExecArray | null;

    while ((match = regex.exec(fmt)) !== null) {
      const n = parseInt(match[1] || '1', 10);
      const f = match[2];
      const getter = GETTERS[f] as keyof DataView64;
      const size = BYTE_LENGTHS[f];

      if (f === 's') {
        output.push(new TextDecoder().decode(buffer.slice(offset, offset + n)));
        offset += n;
      } else {
        for (let i = 0; i < n; i++) {
          const method = view[getter] as (offset: number, littleEndian: boolean) => unknown;
          output.push(method.call(view, offset, !bigEndian));
          offset += size;
        }
      }
    }
    return output;
  }
}

export const struct = new Struct();

// ============================================================================
// Helper Functions
// ============================================================================

function isBigEndian(): boolean {
  const array = new Uint8Array(4);
  const view = new Uint32Array(array.buffer);
  return !((view[0] = 1) & array[0]);
}

export function assert(thing: unknown): asserts thing {
  if (!thing) {
    throw new Error('Assertion failed');
  }
}

// ============================================================================
// Structure Helpers
// ============================================================================

export function _unpack_struct_from(
  structure: StructureDefinition,
  buf: ArrayBuffer,
  offset: number = 0
): UnpackedStruct {
  const output: UnpackedStruct = new Map();

  for (const [key, fmt] of structure.entries()) {
    const value = struct.unpack_from('<' + fmt, buf, offset);
    offset += struct.calcsize(fmt);
    if (value.length === 1) {
      output.set(key, value[0]);
    } else {
      output.set(key, value);
    }
  }
  return output;
}

export function _structure_size(structure: StructureDefinition): number {
  const fmt = '<' + Array.from(structure.values()).join('');
  return struct.calcsize(fmt);
}

export function _padded_size(size: number, paddingMultiple: number = 8): number {
  return Math.ceil(size / paddingMultiple) * paddingMultiple;
}

// ============================================================================
// Dtype Helpers
// ============================================================================

const DTYPE_TO_FORMAT: Record<string, string> = {
  u: 'Uint',
  i: 'Int',
  f: 'Float',
};

export function dtype_getter(dtype_str: string): [string, boolean, number] {
  const bigEndian = struct._is_big_endian(dtype_str);
  let getter: string;
  let nbytes: number;

  if (/S/.test(dtype_str)) {
    // string type
    getter = 'getString';
    const match = dtype_str.match(/S(\d*)/);
    nbytes = parseInt((match && match[1]) || '1', 10);
  } else {
    const match = dtype_str.match(/[<>=!@]?(i|u|f)(\d*)/);
    if (!match) {
      throw new Error(`Invalid dtype string: ${dtype_str}`);
    }
    const fstr = match[1];
    const bytestr = match[2];
    nbytes = parseInt(bytestr || '4', 10);
    const nbits = nbytes * 8;
    getter = 'get' + DTYPE_TO_FORMAT[fstr] + nbits.toFixed();
  }

  return [getter, bigEndian, nbytes];
}

// ============================================================================
// Reference Class
// ============================================================================

export class Reference {
  address_of_reference: number;

  constructor(address_of_reference: number) {
    this.address_of_reference = address_of_reference;
  }

  __bool__(): boolean {
    return this.address_of_reference !== 0;
  }
}

// ============================================================================
// DataView64 - Extended DataView with 64-bit and Float16 support
// ============================================================================

const WARN_OVERFLOW = false;
const MAX_INT64 = 1n << (63n - 1n);
const MIN_INT64 = -1n << 63n;
const MAX_UINT64 = 1n << 64n;
const MIN_UINT64 = 0n;

function decodeFloat16(low: number, high: number): number {
  const sign = (high & 0b10000000) >> 7;
  const exponent = (high & 0b01111100) >> 2;
  const fraction = ((high & 0b00000011) << 8) + low;

  let magnitude: number;
  if (exponent === 0b11111) {
    magnitude = fraction === 0 ? Infinity : NaN;
  } else if (exponent === 0) {
    magnitude = Math.pow(2, -14) * (fraction / 1024);
  } else {
    magnitude = Math.pow(2, exponent - 15) * (1 + fraction / 1024);
  }

  return sign ? -magnitude : magnitude;
}

export class DataView64 {
  private _view: DataView;
  private _buffer: ArrayBuffer;

  constructor(buffer: ArrayBufferLike, byteOffset?: number, byteLength?: number) {
    this._buffer = buffer as ArrayBuffer;
    this._view = new DataView(buffer, byteOffset, byteLength);
  }

  get buffer(): ArrayBuffer {
    return this._buffer;
  }

  getInt8(byteOffset: number): number {
    return this._view.getInt8(byteOffset);
  }

  getUint8(byteOffset: number): number {
    return this._view.getUint8(byteOffset);
  }

  getInt16(byteOffset: number, littleEndian?: boolean): number {
    return this._view.getInt16(byteOffset, littleEndian);
  }

  getUint16(byteOffset: number, littleEndian?: boolean): number {
    return this._view.getUint16(byteOffset, littleEndian);
  }

  getInt32(byteOffset: number, littleEndian?: boolean): number {
    return this._view.getInt32(byteOffset, littleEndian);
  }

  getUint32(byteOffset: number, littleEndian?: boolean): number {
    return this._view.getUint32(byteOffset, littleEndian);
  }

  getFloat32(byteOffset: number, littleEndian?: boolean): number {
    return this._view.getFloat32(byteOffset, littleEndian);
  }

  getFloat64(byteOffset: number, littleEndian?: boolean): number {
    return this._view.getFloat64(byteOffset, littleEndian);
  }

  getFloat16(byteOffset: number, littleEndian: boolean = true): number {
    const bytes = [this._view.getUint8(byteOffset), this._view.getUint8(byteOffset + 1)];
    if (!littleEndian) bytes.reverse();
    const [low, high] = bytes;
    return decodeFloat16(low, high);
  }

  getUint64(byteOffset: number, littleEndian: boolean = true): number {
    const left = BigInt(this._view.getUint32(byteOffset, littleEndian));
    const right = BigInt(this._view.getUint32(byteOffset + 4, littleEndian));
    const combined = littleEndian ? left + (right << 32n) : (left << 32n) + right;

    if (WARN_OVERFLOW && (combined < MIN_UINT64 || combined > MAX_UINT64)) {
      console.warn(combined, 'exceeds range of 64-bit unsigned int');
    }

    return Number(combined);
  }

  getInt64(byteOffset: number, littleEndian: boolean = true): number {
    let low: number, high: number;
    if (littleEndian) {
      low = this._view.getUint32(byteOffset, true);
      high = this._view.getInt32(byteOffset + 4, true);
    } else {
      high = this._view.getInt32(byteOffset, false);
      low = this._view.getUint32(byteOffset + 4, false);
    }

    const combined = BigInt(low) + (BigInt(high) << 32n);

    if (WARN_OVERFLOW && (combined < MIN_INT64 || combined > MAX_INT64)) {
      console.warn(combined, 'exceeds range of 64-bit signed int');
    }

    return Number(combined);
  }

  getString(byteOffset: number, _littleEndian: boolean, length: number): string {
    const strBuffer = this._buffer.slice(byteOffset, byteOffset + length);
    const decoder = new TextDecoder();
    return decoder.decode(strBuffer);
  }

  getVLENStruct(
    byteOffset: number,
    littleEndian: boolean = true,
    _length?: number
  ): [number, number, number] {
    const itemSize = this._view.getUint32(byteOffset, littleEndian);
    const collectionAddress = this.getUint64(byteOffset + 4, littleEndian);
    const objectIndex = this._view.getUint32(byteOffset + 12, littleEndian);
    return [itemSize, collectionAddress, objectIndex];
  }
}

// ============================================================================
// Integer Unpacking
// ============================================================================

export function bitSize(integer: number): number {
  return integer.toString(2).length;
}

export function _unpack_integer(
  nbytes: number,
  fh: ArrayBuffer,
  offset: number = 0,
  littleEndian: boolean = true
): number {
  const bytes = new Uint8Array(fh.slice(offset, offset + nbytes));
  if (!littleEndian) {
    bytes.reverse();
  }
  return bytes.reduce(
    (accumulator, currentValue, index) => accumulator + (currentValue << (index * 8)),
    0
  );
}

// ============================================================================
// VLEN Address Structure
// ============================================================================

export const VLEN_ADDRESS = new Map<string, string>([
  ['item_size', 'I'],
  ['collection_address', 'Q'],
  ['object_index', 'I'],
]);
