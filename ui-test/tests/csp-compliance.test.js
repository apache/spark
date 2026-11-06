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

import { readFile } from 'fs/promises';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';
import { glob } from 'glob';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

// Per-test timeout for the repository-wide file scans below.
const SCAN_TIMEOUT_MS = 60000;

test('CSS files with data URI images do not violate CSP img-src policy', async function () {
  const cssFiles = await glob('**/*.css', {
    cwd: join(__dirname, '../..'),
    ignore: ['**/target/**', '**/node_modules/**']
  });

  const dataUriPattern = /url\(['"]?data:image\//;
  const contents = await Promise.all(
    cssFiles.map((file) => readFile(join(__dirname, '../..', file), 'utf8'))
  );
  const hasDataUri = contents.some((content) => dataUriPattern.test(content));

  // If any CSS uses data URIs, verify our CSP policy allows them
  if (hasDataUri) {
    const httpSecurityFilterPath = join(__dirname, '../../core/src/main/scala/org/apache/spark/ui/HttpSecurityFilter.scala');
    const httpSecurityFilter = await readFile(httpSecurityFilterPath, 'utf8');

    // Verify that CSP includes img-src with data: support
    expect(httpSecurityFilter).toMatch(/img-src\s+[^;]*data:/);
  }
}, SCAN_TIMEOUT_MS);

test('All inline scripts use CSP nonce', async function () {
  const scalaFiles = await glob('**/*.scala', {
    cwd: join(__dirname, '../..'),
    ignore: ['**/target/**', '**/node_modules/**']
  });

  const violations = [];

  await Promise.all(scalaFiles.map(async (file) => {
    const content = await readFile(join(__dirname, '../..', file), 'utf8');

    // Find inline script tags: <script ...>...</script> or <script .../>
    const scriptTagPattern = /<script\s+([^>]*)>/g;
    let match;

    while ((match = scriptTagPattern.exec(content)) !== null) {
      const attributes = match[1];
      
      // Skip external scripts (those with src attribute)
      if (/src\s*=/.test(attributes)) {
        continue;
      }

      // Inline script must have nonce attribute
      if (!/nonce\s*=\s*\{CspNonce\.get\}/.test(attributes)) {
        violations.push({
          file: file,
          line: content.substring(0, match.index).split('\n').length,
          tag: match[0]
        });
      }
    }
  }));

  if (violations.length > 0) {
    const message = violations.map(v => 
      `${v.file}:${v.line} - Missing nonce: ${v.tag}`
    ).join('\n');
    throw new Error(`Found inline scripts without CSP nonce:\n${message}`);
  }
}, SCAN_TIMEOUT_MS);
