# tsfive: A pure TypeScript HDF5 file reader

tsfive is a library for reading (not writing) HDF5 files using pure TypeScript/JavaScript, such as in the browser. It is based on the [pyfive](https://github.com/jjhelmus/pyfive) pure-python implementation of an HDF5 reader.

Not all features of HDF5 are supported, but some key ones that are:

* data chunking
* data compression (zlib via pako)
* async lazy loading for large files
* HTTP range requests for remote files

It is only for reading HDF5 files as an ArrayBuffer representation of the file.

If you need to write HDF5 files in javascript consider using h5wasm ([github](https://github.com/usnistgov/h5wasm), [npm](https://www.npmjs.com/package/h5wasm)) instead (also provides efficient slicing of large datasets, and uses direct filesystem access in nodejs).

## Dependencies
* ES6 module support (current versions of Firefox and Chrome work)
* zlib from [pako](https://github.com/nodeca/pako)

## Limitations
* not all datatypes that are supported by pyfive (through numpy) are supported (yet), though dtypes like u8, f4, S12, i4 are supported.
* datafiles larger than javascript's Number.MAX_SAFE_INTEGER (in bytes) will result in corrupted reads, as the input ArrayBuffer can't be indexed above that.
    * currently this gives an upper limit of 9007199254740991 bytes, which is a lot. (~10^7 GB)

## Installation

### NPM
```bash
npm install tsfive
```

### CDN
```html
<script src="https://cdn.jsdelivr.net/npm/tsfive/dist/browser/hdf5.js"></script>
```

## Usage

### Basic Usage (Browser)
```javascript
import { File } from 'tsfive';

fetch(file_url)
  .then(response => response.arrayBuffer())
  .then(buffer => {
    const f = new File(buffer, filename);
    const g = f.get('group');
    const d = f.get('group/dataset');
    const v = d.value;
    const a = d.attrs;
  });
```

### Async Lazy Loading (Recommended for Large Files)
```javascript
import { openFile } from 'tsfive';

// From URL with lazy loading
const file = await openFile('https://example.com/large-file.h5', { lazy: true });

// Get a dataset
const dataset = await file.getAsync('path/to/dataset');

// Read data slice (efficient for large datasets)
const slice = await dataset.sliceAsync(0, 1000);

// Or iterate in chunks
for await (const chunk of dataset.iterChunks({ chunkSize: 10000 })) {
  console.log(chunk.data);
}

// Don't forget to close when done
await file.close();
```

### Node.js
```javascript
import { openFile } from 'tsfive';

// From file path (Node.js only)
const file = await openFile('/path/to/file.h5');
const dataset = await file.getAsync('data');
const value = await dataset.valueAsync();
await file.close();
```

### File Upload (Browser)
```javascript
import { File } from 'tsfive';

function loadData() {
  const fileInput = document.getElementById('datafile');
  const file = fileInput.files[0];
  const reader = new FileReader();

  reader.onloadend = function(evt) {
    const buffer = evt.target.result;
    const f = new File(buffer, file.name);
    // do something with f...
  }

  reader.readAsArrayBuffer(file);
}
```

## API

### Synchronous API
- `File` - HDF5 file reader (loads entire file into memory)
- `Group` - HDF5 group container
- `Dataset` - HDF5 dataset with `.value`, `.shape`, `.dtype`, `.attrs`

### Asynchronous API (Recommended)
- `openFile(source, options)` - Open a file with lazy loading support
- `AsyncFile` - Async file reader
- `AsyncGroup` - Async group with `getAsync()` method
- `AsyncDataset` - Async dataset with `sliceAsync()`, `valueAsync()`, `iterChunks()`

### Options for `openFile()`
```typescript
interface OpenFileOptions {
  lazy?: boolean;           // Enable lazy loading (default: false)
  cacheSize?: number;       // Cache size in bytes (default: 64MB)
  cacheBlockSize?: number;  // Block size for caching (default: 64KB)
  filename?: string;        // Optional filename for reference
  initialMetadataSize?: number; // Initial metadata buffer size
}
```

## TypeScript Support

tsfive is written in TypeScript and includes full type definitions:

```typescript
import { File, Dataset, Group, openFile, AsyncFile, AsyncDataset } from 'tsfive';
import type { Dtype, DataValue, OpenFileOptions } from 'tsfive';
```

## License

See LICENSE.txt
