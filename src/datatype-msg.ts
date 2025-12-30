/**
 * HDF5 Datatype Message Parser
 * Handles parsing of datatype information from HDF5 object headers
 */

import { _structure_size, _unpack_struct_from } from './core.js';
import type { StructureDefinition, UnpackedStruct } from './types/hdf5.js';
import type { Dtype } from './types/dtype.js';
import { DatatypeClass } from './types/hdf5.js';

// ============================================================================
// Datatype Constants
// ============================================================================

const DATATYPE_FIXED_POINT = DatatypeClass.FIXED_POINT;
const DATATYPE_FLOATING_POINT = DatatypeClass.FLOATING_POINT;
const DATATYPE_TIME = DatatypeClass.TIME;
const DATATYPE_STRING = DatatypeClass.STRING;
const DATATYPE_BITFIELD = DatatypeClass.BITFIELD;
const DATATYPE_OPAQUE = DatatypeClass.OPAQUE;
const DATATYPE_COMPOUND = DatatypeClass.COMPOUND;
const DATATYPE_REFERENCE = DatatypeClass.REFERENCE;
const DATATYPE_ENUMERATED = DatatypeClass.ENUMERATED;
const DATATYPE_VARIABLE_LENGTH = DatatypeClass.VARIABLE_LENGTH;
const DATATYPE_ARRAY = DatatypeClass.ARRAY;

// ============================================================================
// Structure Definitions
// ============================================================================

const DATATYPE_MSG: StructureDefinition = new Map([
  ['class_and_version', 'B'],
  ['class_bit_field_0', 'B'],
  ['class_bit_field_1', 'B'],
  ['class_bit_field_2', 'B'],
  ['size', 'I'],
]);
const DATATYPE_MSG_SIZE = _structure_size(DATATYPE_MSG);

const COMPOUND_PROP_DESC_V1: StructureDefinition = new Map([
  ['offset', 'I'],
  ['dimensionality', 'B'],
  ['reserved_0', 'B'],
  ['reserved_1', 'B'],
  ['reserved_2', 'B'],
  ['permutation', 'I'],
  ['reserved_3', 'I'],
  ['dim_size_1', 'I'],
  ['dim_size_2', 'I'],
  ['dim_size_3', 'I'],
  ['dim_size_4', 'I'],
]);
const _COMPOUND_PROP_DESC_V1_SIZE = _structure_size(COMPOUND_PROP_DESC_V1);

// ============================================================================
// DatatypeMessage Class
// ============================================================================

/**
 * Representation of a HDF5 Datatype Message
 * Contents and layout defined in IV.A.2.d of the HDF5 specification
 */
export class DatatypeMessage {
  buf: ArrayBuffer;
  offset: number;
  dtype: Dtype;

  constructor(buf: ArrayBuffer, offset: number) {
    this.buf = buf;
    this.offset = offset;
    this.dtype = this.determine_dtype();
  }

  /**
   * Return the dtype (often numpy-like) for the datatype message
   */
  determine_dtype(): Dtype {
    const datatypeMsg = _unpack_struct_from(DATATYPE_MSG, this.buf, this.offset);
    this.offset += DATATYPE_MSG_SIZE;

    // Last 4 bits determine datatype class
    const datatypeClass = (datatypeMsg.get('class_and_version') as number) & 0x0f;

    switch (datatypeClass) {
      case DATATYPE_FIXED_POINT:
        return this._determine_dtype_fixed_point(datatypeMsg);

      case DATATYPE_FLOATING_POINT:
        return this._determine_dtype_floating_point(datatypeMsg);

      case DATATYPE_TIME:
        throw new Error('Time datatype class not supported.');

      case DATATYPE_STRING:
        return this._determine_dtype_string(datatypeMsg);

      case DATATYPE_BITFIELD:
        throw new Error('Bitfield datatype class not supported.');

      case DATATYPE_OPAQUE:
        throw new Error('Opaque datatype class not supported.');

      case DATATYPE_COMPOUND:
        return this._determine_dtype_compound(datatypeMsg);

      case DATATYPE_REFERENCE:
        return ['REFERENCE', datatypeMsg.get('size') as number];

      case DATATYPE_ENUMERATED:
        // Enumerated base class datatype message starts at end of
        // enum datatype message, and offset is already advanced above,
        // so just run the same function again to get base class
        return this.determine_dtype();

      case DATATYPE_VARIABLE_LENGTH: {
        let vlenType = this._determine_dtype_vlen(datatypeMsg);
        if (Array.isArray(vlenType) && vlenType[0] === 'VLEN_SEQUENCE') {
          const baseType = this.determine_dtype();
          vlenType = ['VLEN_SEQUENCE', baseType];
        }
        return vlenType;
      }

      case DATATYPE_ARRAY:
        throw new Error('Array datatype class not supported.');

      default:
        throw new Error('Invalid datatype class ' + datatypeClass);
    }
  }

  /**
   * Return the NumPy dtype for a fixed point class
   */
  private _determine_dtype_fixed_point(datatypeMsg: UnpackedStruct): string {
    const lengthInBytes = datatypeMsg.get('size') as number;
    if (![1, 2, 4, 8].includes(lengthInBytes)) {
      throw new Error('Unsupported datatype size');
    }

    const signed = (datatypeMsg.get('class_bit_field_0') as number) & 0x08;
    const dtypeChar = signed > 0 ? 'i' : 'u';

    const byteOrder = (datatypeMsg.get('class_bit_field_0') as number) & 0x01;
    const byteOrderChar = byteOrder === 0 ? '<' : '>'; // little-endian : big-endian

    // 4-byte fixed-point property description (not read, assumed IEEE standard)
    this.offset += 4;

    return byteOrderChar + dtypeChar + lengthInBytes.toFixed();
  }

  /**
   * Return the NumPy dtype for a floating point class
   */
  private _determine_dtype_floating_point(datatypeMsg: UnpackedStruct): string {
    const lengthInBytes = datatypeMsg.get('size') as number;
    if (![1, 2, 4, 8].includes(lengthInBytes)) {
      throw new Error('Unsupported datatype size');
    }

    const dtypeChar = 'f';

    const byteOrder = (datatypeMsg.get('class_bit_field_0') as number) & 0x01;
    const byteOrderChar = byteOrder === 0 ? '<' : '>'; // little-endian : big-endian

    // 12-bytes floating-point property description (not read, assumed IEEE standard)
    this.offset += 12;

    return byteOrderChar + dtypeChar + lengthInBytes.toFixed();
  }

  /**
   * Return the NumPy dtype for a string class
   */
  private _determine_dtype_string(datatypeMsg: UnpackedStruct): string {
    return 'S' + (datatypeMsg.get('size') as number).toFixed();
  }

  /**
   * Return the dtype information for a variable length class
   */
  private _determine_dtype_vlen(datatypeMsg: UnpackedStruct): Dtype {
    const vlenType = (datatypeMsg.get('class_bit_field_0') as number) & 0x01;
    if (vlenType !== 1) {
      return ['VLEN_SEQUENCE', 0, 0];
    }
    const paddingType = (datatypeMsg.get('class_bit_field_0') as number) >> 4; // bits 4-7
    const characterSet = (datatypeMsg.get('class_bit_field_1') as number) & 0x01;
    return ['VLEN_STRING', paddingType, characterSet];
  }

  /**
   * Return the dtype for a compound class
   */
  private _determine_dtype_compound(_datatypeMsg: UnpackedStruct): Dtype {
    throw new Error('Compound type not yet implemented!');
  }
}
