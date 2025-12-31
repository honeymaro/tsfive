import assert from 'node:assert';
import { test } from 'node:test';
import { readFileSync } from "node:fs";

import * as hdf5 from "../dist/esm/index.mjs";

function loadFile(filename) {
    const ab = readFileSync(filename);
    return new hdf5.File(ab.buffer, filename);
}

function loadFileAsArrayBuffer(filename) {
    const ab = readFileSync(filename);
    return ab.buffer;
}

test('check dtypes', () => {
  const dtypes = ['f2', 'f4', 'f8', 'i1', 'i2', 'i4'];
  const values = [3.0, 4.0, 5.0];
  const f = loadFile("test/test.h5");

  for (const dtype of dtypes) {
    const dset = f.get(dtype);
    assert.strictEqual(dset.dtype, `<${dtype}`);
    assert.deepEqual(dset.value, values);
  }  
});

test('strings', () => {
  const f = loadFile("test/test.h5");
  const dset = f.get('string');

  assert.strictEqual(dset.dtype, 'S5');
  assert.deepEqual(dset.value, ['hello']);

  const vlen_dset = f.get('vlen_string');
  assert.deepEqual(vlen_dset.dtype, ['VLEN_STRING', 0, 1]);
  assert.deepEqual(vlen_dset.value, ['hello']);
});

// ============================================================================
// Async API Tests
// ============================================================================

test('openFile with ArrayBuffer', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");
  const file = await hdf5.openFile(buffer, { filename: 'test.h5' });

  assert.ok(file);
  assert.ok(file.keys.length > 0);

  await file.close();
});

test('AsyncFile getAsync', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");
  const file = await hdf5.openFile(buffer);

  const dataset = await file.getAsync('f4');
  assert.ok(dataset);
  assert.strictEqual(dataset.dtype, '<f4');
  assert.deepEqual(dataset.shape, [3]);

  await file.close();
});

test('AsyncDataset sliceAsync', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");
  const file = await hdf5.openFile(buffer);

  const dataset = await file.getAsync('f4');
  const slice = await dataset.sliceAsync(0, 2);

  assert.strictEqual(slice.length, 2);
  assert.deepEqual(slice, [3.0, 4.0]);

  await file.close();
});

test('AsyncDataset valueAsync', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");
  const file = await hdf5.openFile(buffer);

  const dataset = await file.getAsync('f4');
  const value = await dataset.valueAsync();

  assert.deepEqual(value, [3.0, 4.0, 5.0]);

  await file.close();
});

test('AsyncDataset iterChunks', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");
  const file = await hdf5.openFile(buffer);

  const dataset = await file.getAsync('f4');
  const chunks = [];

  for await (const chunk of dataset.iterChunks({ chunkSize: 2 })) {
    chunks.push(chunk);
  }

  assert.ok(chunks.length >= 1);
  assert.ok(chunks[0].data);
  assert.strictEqual(typeof chunks[0].offset, 'number');
  assert.strictEqual(typeof chunks[0].size, 'number');
  assert.strictEqual(typeof chunks[0].isLast, 'boolean');

  await file.close();
});

test('AsyncGroup navigation', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");
  const file = await hdf5.openFile(buffer);

  // Test keys
  assert.ok(Array.isArray(file.keys));
  assert.ok(file.keys.includes('f4'));

  // Test attrs
  const attrs = file.attrs;
  assert.ok(typeof attrs === 'object');

  await file.close();
});

// ============================================================================
// Debug Option Tests
// ============================================================================

test('debug option disabled by default (no logs)', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");

  const originalLog = console.log;
  const logs = [];
  console.log = (...args) => logs.push(args);

  try {
    const file = await hdf5.openFile(buffer);
    await file.getAsync('f4');
    await file.close();

    // Filter for tsfive logs only
    const tsfiveLogs = logs.filter(log =>
      String(log[0]).includes('tsfive')
    );
    assert.strictEqual(tsfiveLogs.length, 0, 'No debug logs when disabled');
  } finally {
    console.log = originalLog;
  }
});

test('debug option enabled produces logs', async () => {
  const buffer = loadFileAsArrayBuffer("test/test.h5");

  const originalLog = console.log;
  const logs = [];
  console.log = (...args) => logs.push(args);

  try {
    const file = await hdf5.openFile(buffer, { debug: true });
    await file.getAsync('f4');
    await file.close();

    // Filter for tsfive logs
    const tsfiveLogs = logs.filter(log =>
      String(log[0]).includes('tsfive')
    );
    assert.ok(tsfiveLogs.length > 0, 'Debug logs should be present when enabled');
  } finally {
    console.log = originalLog;
  }
});