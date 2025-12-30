/**
 * HDF5 Datatype Definitions
 */

// ============================================================================
// Byte Order
// ============================================================================

export type ByteOrder = '<' | '>'; // little-endian | big-endian

// ============================================================================
// Basic Dtype String Types (NumPy-compatible)
// ============================================================================

/** Fixed-point integer dtype (e.g., '<i4', '>u8') */
export type IntegerDtype = `${ByteOrder}${'i' | 'u'}${1 | 2 | 4 | 8}`;

/** Floating-point dtype (e.g., '<f4', '>f8') */
export type FloatDtype = `${ByteOrder}f${1 | 2 | 4 | 8}`;

/** Fixed-length string dtype (e.g., 'S10') */
export type FixedStringDtype = `S${number}`;

/** Numeric dtype */
export type NumericDtype = IntegerDtype | FloatDtype;

// ============================================================================
// Special Dtype Types
// ============================================================================

/** Variable-length string marker */
export type VLENStringDtype = 'VLEN_STRING';

/** Object reference marker */
export type ReferenceDtype = 'REFERENCE';

/** VLEN sequence marker with base type */
export type VLENSequenceDtype = ['VLEN_SEQUENCE', Dtype];

/** Reference with size */
export type ReferenceWithSize = ['REFERENCE', number];

/** VLEN string with padding and charset info */
export type VLENStringInfo = ['VLEN_STRING', number, number];

/** VLEN sequence placeholder */
export type VLENSequenceInfo = ['VLEN_SEQUENCE', number, number];

// ============================================================================
// Compound Dtype
// ============================================================================

export interface CompoundDtype {
  names: string[];
  formats: Dtype[];
  offsets: number[];
  itemsize: number;
}

// ============================================================================
// Combined Dtype Type
// ============================================================================

/** All possible dtype representations */
export type Dtype =
  | NumericDtype
  | FixedStringDtype
  | VLENStringDtype
  | ReferenceDtype
  | VLENSequenceDtype
  | ReferenceWithSize
  | VLENStringInfo
  | VLENSequenceInfo
  | CompoundDtype
  | string; // fallback for dynamic dtypes

// ============================================================================
// Dtype Parsing Results
// ============================================================================

export interface DtypeGetterResult {
  getter: string;
  bigEndian: boolean;
  nbytes: number;
}

// ============================================================================
// TypedArray Types
// ============================================================================

export type TypedArray =
  | Int8Array
  | Uint8Array
  | Int16Array
  | Uint16Array
  | Int32Array
  | Uint32Array
  | Float32Array
  | Float64Array
  | BigInt64Array
  | BigUint64Array;

export type TypedArrayConstructor =
  | Int8ArrayConstructor
  | Uint8ArrayConstructor
  | Int16ArrayConstructor
  | Uint16ArrayConstructor
  | Int32ArrayConstructor
  | Uint32ArrayConstructor
  | Float32ArrayConstructor
  | Float64ArrayConstructor
  | BigInt64ArrayConstructor
  | BigUint64ArrayConstructor;

// ============================================================================
// Struct Format Characters
// ============================================================================

/** Struct format character to DataView getter mapping */
export interface FormatCharMapping {
  getter: string;
  byteLength: number;
}

export const FORMAT_CHAR_MAP: Record<string, FormatCharMapping> = {
  s: { getter: 'getUint8', byteLength: 1 },
  b: { getter: 'getInt8', byteLength: 1 },
  B: { getter: 'getUint8', byteLength: 1 },
  h: { getter: 'getInt16', byteLength: 2 },
  H: { getter: 'getUint16', byteLength: 2 },
  i: { getter: 'getInt32', byteLength: 4 },
  I: { getter: 'getUint32', byteLength: 4 },
  l: { getter: 'getInt32', byteLength: 4 },
  L: { getter: 'getUint32', byteLength: 4 },
  q: { getter: 'getInt64', byteLength: 8 },
  Q: { getter: 'getUint64', byteLength: 8 },
  e: { getter: 'getFloat16', byteLength: 2 },
  f: { getter: 'getFloat32', byteLength: 4 },
  d: { getter: 'getFloat64', byteLength: 8 },
};

// ============================================================================
// Data Value Types
// ============================================================================

/** Base primitive data values */
export type PrimitiveDataValue =
  | number
  | bigint
  | string
  | boolean
  | null
  | ArrayBuffer
  | TypedArray;

/** Possible data values from HDF5 datasets (recursive) */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface DataValueArray extends Array<DataValue> {}
// eslint-disable-next-line @typescript-eslint/no-empty-object-type
export interface DataValueRecord extends Record<string, DataValue> {}

export type DataValue = PrimitiveDataValue | DataValueArray | DataValueRecord;

/** Dataset value type */
export type DatasetValue = TypedArray | DataValue[];
