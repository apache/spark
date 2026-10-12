/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * @jest-environment jsdom
 */

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import '../../core/src/main/resources/org/apache/spark/ui/static/jquery.min.js';

/* global $ */

// table.js is a classic (non-module) script. Running it registers its delegated click listeners
// on the document once jQuery is ready.
const __dirname = dirname(fileURLToPath(import.meta.url));
const tablePath = join(
  __dirname, '../../core/src/main/resources/org/apache/spark/ui/static/table.js');
new Function(readFileSync(tablePath, 'utf8'))();

// A thread name containing '#' is followed by more threads. A data: URI download is truncated at
// the first '#', which would drop everything after it.
const THREAD_DUMP =
  '"Executor task idle worker#55" daemon prio=5 Id=55 TIMED_WAITING\n' +
  '    at java.base/java.lang.Thread.run(Thread.java:840)\n' +
  '\n' +
  '"main" prio=5 Id=1 RUNNABLE\n' +
  '    at org.apache.spark.SparkContext.<init>(SparkContext.scala:100)\n';

// The page embeds the dump HTML-escaped, as Scala XML renders it.
function escapeHtml(text) {
  return text.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

function readBlob(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(reader.result);
    reader.onerror = () => reject(reader.error);
    reader.readAsText(blob);
  });
}

// jsdom implements neither URL.createObjectURL nor downloads, so record the Blob and the link
// the helper clicks to start the download, and the Blob URLs it revokes.
let downloads;
let revokedUrls;
let originalAnchorClick;

beforeAll(() => new Promise(resolve => $(resolve)));

beforeEach(() => {
  downloads = [];
  revokedUrls = [];
  let lastBlob;
  URL.createObjectURL = blob => {
    lastBlob = blob;
    return 'blob:thread-dump';
  };
  URL.revokeObjectURL = url => revokedUrls.push(url);
  originalAnchorClick = HTMLAnchorElement.prototype.click;
  HTMLAnchorElement.prototype.click = function () {
    downloads.push({
      blob: lastBlob, href: this.href, fileName: this.download, revokedAtClick: revokedUrls.length
    });
  };
});

afterEach(async () => {
  // Let a pending Blob URL revoke run before the stubs are restored.
  await new Promise(resolve => setTimeout(resolve, 200));
  HTMLAnchorElement.prototype.click = originalAnchorClick;
  delete URL.createObjectURL;
  delete URL.revokeObjectURL;
});

test('Download saves the complete thread dump, including threads after a "#"', async function () {
  document.body.innerHTML =
    '<a class="downloadbutton" data-action="downloadThreadDump" ' +
    'data-filename="threaddump_1.txt">Download</a>' +
    `<pre id="thread-dump-text" class="d-none">${escapeHtml(THREAD_DUMP)}</pre>`;

  // Dispatch the click: HTMLAnchorElement.prototype.click is replaced to record downloads.
  document.querySelector('a.downloadbutton')
    .dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));

  expect(downloads.length).toBe(1);
  const { blob, href, fileName } = downloads[0];
  expect(fileName).toBe('threaddump_1.txt');
  expect(href).toBe('blob:thread-dump');
  expect(blob.type).toBe('text/plain;charset=utf-8');
  expect(await readBlob(blob)).toBe(THREAD_DUMP);
});

test('Download releases the Blob URL after starting the download', async function () {
  document.body.innerHTML =
    '<a class="downloadbutton" data-action="downloadThreadDump" ' +
    'data-filename="threaddump_1.txt">Download</a>' +
    `<pre id="thread-dump-text" class="d-none">${escapeHtml(THREAD_DUMP)}</pre>`;

  document.querySelector('a.downloadbutton')
    .dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
  await new Promise(resolve => setTimeout(resolve, 200));

  expect(downloads.length).toBe(1);
  // Revoking before the click would cancel the download.
  expect(downloads[0].revokedAtClick).toBe(0);
  expect(revokedUrls).toEqual(['blob:thread-dump']);
});

test('the legacy data: URI link is left to the browser', function () {
  document.body.innerHTML =
    '<a class="downloadbutton" href="data:text/plain;charset=utf-8,dump" ' +
    'download="threaddump_1.txt">Download</a>';
  const link = document.querySelector('a.downloadbutton');
  // jsdom does not implement navigation, so cancel the link's default action.
  link.addEventListener('click', event => event.preventDefault());

  link.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));

  expect(downloads.length).toBe(0);
});
