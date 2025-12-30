/**
 * HDF5 Filter Pipeline Implementation
 * Supports GZIP, Shuffle, and Fletcher32 filters
 */

import * as pako from 'pako';
import { struct } from './core.js';
import type { FilterFunction } from './types/index.js';
import { FilterId } from './types/hdf5.js';

// ============================================================================
// Filter Implementations
// ============================================================================

/**
 * GZIP/Deflate decompression filter
 */
const zlibDecompress: FilterFunction = (buf: ArrayBuffer, _itemsize?: number): ArrayBuffer => {
  const inputArray = new Uint8Array(buf);
  return pako.inflate(inputArray).buffer;
};

/**
 * Shuffle filter - reverses the byte shuffling applied during compression
 */
const unshuffle: FilterFunction = (buf: ArrayBuffer, itemsize: number = 1): ArrayBuffer => {
  const bufferSize = buf.byteLength;
  const unshuffledView = new Uint8Array(bufferSize);
  const step = Math.floor(bufferSize / itemsize);
  const shuffledView = new DataView(buf);

  for (let j = 0; j < itemsize; j++) {
    for (let i = 0; i < step; i++) {
      unshuffledView[j + i * itemsize] = shuffledView.getUint8(j * step + i);
    }
  }

  return unshuffledView.buffer;
};

/**
 * Fletcher32 checksum verification and removal
 */
const fletch32: FilterFunction = (buf: ArrayBuffer, _itemsize?: number): ArrayBuffer => {
  verifyFletcher32(buf);
  // Strip off 4-byte checksum from end of buffer
  return buf.slice(0, -4);
};

/**
 * Verify Fletcher32 checksum
 * @throws If checksum verification fails
 */
function verifyFletcher32(chunkBuffer: ArrayBuffer): boolean {
  const oddChunkBuffer = chunkBuffer.byteLength % 2 !== 0;
  const dataLength = chunkBuffer.byteLength - 4;
  const view = new DataView(chunkBuffer);

  let sum1 = 0;
  let sum2 = 0;

  for (let offset = 0; offset < dataLength - 1; offset += 2) {
    const datum = view.getUint16(offset, true); // little-endian
    sum1 = (sum1 + datum) % 65535;
    sum2 = (sum2 + sum1) % 65535;
  }

  if (oddChunkBuffer) {
    // Process the last item
    const datum = view.getUint8(dataLength - 1);
    sum1 = (sum1 + datum) % 65535;
    sum2 = (sum2 + sum1) % 65535;
  }

  // Extract stored checksums (big-endian)
  const [refSum1, refSum2] = struct.unpack_from('>HH', chunkBuffer, dataLength) as [number, number];
  const normalizedRefSum1 = refSum1 % 65535;
  const normalizedRefSum2 = refSum2 % 65535;

  if (sum1 !== normalizedRefSum1 || sum2 !== normalizedRefSum2) {
    throw new Error('ValueError("fletcher32 checksum invalid")');
  }

  return true;
}

// ============================================================================
// Filter Registry
// ============================================================================

/**
 * Map of filter ID to filter function
 * To register a new filter, add a function (ArrayBuffer, itemsize?) => ArrayBuffer
 */
export const Filters = new Map<number, FilterFunction>([
  [FilterId.GZIP_DEFLATE, zlibDecompress],
  [FilterId.SHUFFLE, unshuffle],
  [FilterId.FLETCHER32, fletch32],
]);

// ============================================================================
// Filter Constants (for external reference)
// ============================================================================

export const RESERVED_FILTER = FilterId.RESERVED;
export const GZIP_DEFLATE_FILTER = FilterId.GZIP_DEFLATE;
export const SHUFFLE_FILTER = FilterId.SHUFFLE;
export const FLETCH32_FILTER = FilterId.FLETCHER32;
export const SZIP_FILTER = FilterId.SZIP;
export const NBIT_FILTER = FilterId.NBIT;
export const SCALEOFFSET_FILTER = FilterId.SCALEOFFSET;
