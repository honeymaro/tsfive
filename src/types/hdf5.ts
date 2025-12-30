/**
 * HDF5 Core Type Definitions
 */

// ============================================================================
// Constants
// ============================================================================

/** Undefined/invalid address marker (0xFFFFFFFFFFFFFFFF as number) */
export const UNDEFINED_ADDRESS = Number.MAX_SAFE_INTEGER;

/** HDF5 format signature */
export const FORMAT_SIGNATURE = '\x89HDF\r\n\x1a\n';

// ============================================================================
// Datatype Classes (IV.A.2.d)
// ============================================================================

export const enum DatatypeClass {
  FIXED_POINT = 0,
  FLOATING_POINT = 1,
  TIME = 2,
  STRING = 3,
  BITFIELD = 4,
  OPAQUE = 5,
  COMPOUND = 6,
  REFERENCE = 7,
  ENUMERATED = 8,
  VARIABLE_LENGTH = 9,
  ARRAY = 10,
}

// ============================================================================
// Message Types (IV.A.2)
// ============================================================================

export const enum MessageType {
  NIL = 0x0000,
  DATASPACE = 0x0001,
  LINK_INFO = 0x0002,
  DATATYPE = 0x0003,
  FILL_VALUE_OLD = 0x0004,
  FILL_VALUE = 0x0005,
  LINK = 0x0006,
  EXTERNAL_FILE_LIST = 0x0007,
  DATA_LAYOUT = 0x0008,
  BOGUS = 0x0009,
  GROUP_INFO = 0x000a,
  FILTER_PIPELINE = 0x000b,
  ATTRIBUTE = 0x000c,
  OBJECT_COMMENT = 0x000d,
  OBJECT_MODIFICATION_TIME_OLD = 0x000e,
  SHARED_MESSAGE_TABLE = 0x000f,
  OBJECT_CONTINUATION = 0x0010,
  SYMBOL_TABLE = 0x0011,
  OBJECT_MODIFICATION_TIME = 0x0012,
  BTREE_K_VALUES = 0x0013,
  DRIVER_INFO = 0x0014,
  ATTRIBUTE_INFO = 0x0015,
  OBJECT_REFERENCE_COUNT = 0x0016,
}

// ============================================================================
// Data Layout Classes
// ============================================================================

export const enum LayoutClass {
  COMPACT = 0,
  CONTIGUOUS = 1,
  CHUNKED = 2,
  VIRTUAL = 3,
}

// ============================================================================
// Filter IDs (IV.A.2.l)
// ============================================================================

export const enum FilterId {
  RESERVED = 0,
  GZIP_DEFLATE = 1,
  SHUFFLE = 2,
  FLETCHER32 = 3,
  SZIP = 4,
  NBIT = 5,
  SCALEOFFSET = 6,
}

// ============================================================================
// Structure Definitions (for struct unpacking)
// ============================================================================

/** Structure field definition: Map of field name to format character */
export type StructureDefinition = Map<string, string>;

/** Unpacked structure result */
export type UnpackedStruct = Map<string, unknown>;

// ============================================================================
// SuperBlock Types
// ============================================================================

export interface SuperBlockV0Fields {
  format_signature: string;
  superblock_version: number;
  free_storage_version: number;
  root_group_version: number;
  reserved_0: number;
  shared_header_version: number;
  offset_size: number;
  length_size: number;
  reserved_1: number;
  group_leaf_node_k: number;
  group_internal_node_k: number;
  file_consistency_flags: number;
  base_address_lower: number;
  free_space_address: number;
  end_of_file_address: number;
  driver_information_address: number;
}

export interface SuperBlockV2V3Fields {
  format_signature: string;
  superblock_version: number;
  offset_size: number;
  length_size: number;
  file_consistency_flags: number;
  base_address: number;
  superblock_extension_address: number;
  end_of_file_address: number;
  root_group_address: number;
  superblock_checksum: number;
}

// ============================================================================
// Symbol Table Types
// ============================================================================

export interface SymbolTableEntry {
  link_name_offset: number;
  object_header_address: number;
  cache_type: number;
  reserved: number;
  scratch: string;
  link_name?: string;
}

export interface SymbolTableNode {
  signature: string;
  version: number;
  reserved_0: number;
  symbols: number;
}

// ============================================================================
// Heap Types
// ============================================================================

export interface LocalHeapFields {
  signature: string;
  version: number;
  reserved: string;
  data_segment_size: number;
  offset_to_free_list: number;
  address_of_data_segment: number;
  heap_data?: ArrayBuffer;
}

export interface GlobalHeapHeader {
  signature: string;
  version: number;
  reserved: string;
  collection_size: number;
}

export interface GlobalHeapObject {
  object_index: number;
  reference_count: number;
  reserved: number;
  object_size: number;
}

export interface FractalHeapHeader {
  signature: string;
  version: number;
  object_index_size: number;
  filter_info_size: number;
  flags: number;
  max_managed_object_size: number;
  next_huge_object_index: number;
  btree_address_huge_objects: number | null;
  managed_freespace_size: number;
  freespace_manager_address: number;
  managed_space_size: number;
  managed_alloc_size: number;
  next_directblock_iterator_address: number;
  managed_object_count: number;
  huge_objects_total_size: number;
  huge_object_count: number;
  tiny_objects_total_size: number;
  tiny_object_count: number;
  table_width: number;
  starting_block_size: number;
  maximum_direct_block_size: number;
  log2_maximum_heap_size: number;
  indirect_starting_rows_count: number;
  root_block_address: number | null;
  indirect_current_rows_count: number;
}

// ============================================================================
// B-tree Types
// ============================================================================

export interface BTreeNodeHeader {
  signature: string;
  node_type: number;
  node_level: number;
  entries_used: number;
  left_sibling: number;
  right_sibling: number;
}

export interface BTreeV2Header {
  signature: string;
  version: number;
  type: number;
  node_size: number;
  record_size: number;
  depth: number;
  split_percent: number;
  merge_percent: number;
  root_node_address: number;
  root_node_nrecords: number;
  total_records: number;
}

// ============================================================================
// Data Layout Types
// ============================================================================

export interface DataLayoutCompact {
  layoutClass: LayoutClass.COMPACT;
  size: number;
  data: ArrayBuffer;
}

export interface DataLayoutContiguous {
  layoutClass: LayoutClass.CONTIGUOUS;
  address: number;
  size: number;
}

export interface DataLayoutChunked {
  layoutClass: LayoutClass.CHUNKED;
  dimensionality: number;
  address: number;
  chunkShape: number[];
  dataSize?: number;
}

export type DataLayout = DataLayoutCompact | DataLayoutContiguous | DataLayoutChunked;

// ============================================================================
// Filter Pipeline Types
// ============================================================================

export interface FilterInfo {
  id: FilterId | number;
  name?: string;
  flags: number;
  clientData: number[];
}

export interface FilterPipeline {
  version: number;
  filters: FilterInfo[];
}

// ============================================================================
// Dataspace Types
// ============================================================================

export interface Dataspace {
  version: number;
  dimensionality: number;
  flags: number;
  type: number;
  dimensions: number[];
  maxDimensions?: number[];
}

// ============================================================================
// Object Header Types
// ============================================================================

export interface ObjectHeaderMessage {
  type: MessageType | number;
  size: number;
  flags: number;
  data: ArrayBuffer;
}

export interface ObjectHeader {
  version: number;
  messages: ObjectHeaderMessage[];
}

// ============================================================================
// Link Types
// ============================================================================

export type LinkTarget = number | string; // address or soft link path

export interface Links {
  [name: string]: LinkTarget;
}

// ============================================================================
// VLEN Address Structure
// ============================================================================

export interface VLENAddress {
  item_size: number;
  collection_address: number;
  object_index: number;
}
