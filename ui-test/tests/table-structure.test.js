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

import { readFileSync } from 'fs';
import { join, dirname } from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);
const staticDir = join(__dirname, '../../core/src/main/resources/org/apache/spark/ui/static');

function readStatic(filename) {
  return readFileSync(join(staticDir, filename), 'utf8');
}

function extractTemplateContent(html, id) {
  const re = new RegExp(`<script[^>]*id="${id}"[^>]*>([\\s\\S]*?)<\\/script>`);
  return html.match(re)[1];
}

function extractThTexts(html) {
  return [...html.matchAll(/<th[\s>][\s\S]*?<\/th>/g)]
    .map(m => m[0].replace(/<[^>]+>/g, '').replace(/\s+/g, ' ').trim());
}

function extractTableIds(html) {
  return [...html.matchAll(/<table[^>]*id="([^"]+)"/g)].map(m => m[1]);
}

test('historypage template has expected column structure', function () {
  const html = readStatic('historypage-template.html');
  const template = extractTemplateContent(html, 'history-summary-template');
  const headers = extractThTexts(template);

  // All columns must always be present in the template.
  // Attempt ID is always shown (empty for apps without attempts).
  // historypage.js removes Completed and Duration <th> headers at runtime
  // for incomplete apps, matching the data column removal.
  expect(headers).toEqual([
    'Version',
    'App ID',
    'App Name',
    'Log Source',
    'Attempt ID',
    'Started',
    'Completed',
    'Duration',
    'Spark User',
    'Last Updated',
    'Event Log',
  ]);

  expect(extractTableIds(template)).toContain('history-summary-table');
});

test('historypage JS column config matches template header count', function () {
  // Verify that historypage.js defines the same number of data columns as <th> headers
  // in the template. This catches the SPARK-56259 regression where removeColumnByName
  // removed data columns without removing the corresponding <th> headers.
  const jsSource = readStatic('historypage.js');

  // Count column definitions by looking for 'name:' keys in the columns array.
  // Each column has a unique name field.
  const columnNames = [...jsSource.matchAll(/name:\s*['"]?(\w+)['"]?/g)]
    .map(m => m[1])
    .filter(n => !['columnName', 'name'].includes(n)); // exclude helper function params

  // The JS defines 11 named columns (same as 11 <th> headers in the template)
  expect(columnNames.length).toBe(11);
});

test('executorspage template has expected table structure', function () {
  const html = readStatic('executorspage-template.html');
  const template = extractTemplateContent(html, 'executors-summary-template');
  const tableIds = extractTableIds(template);

  expect(tableIds).toContain('summary-execs-table');
  expect(tableIds).toContain('active-executors-table');

  // Summary table columns
  const summaryTable = template.match(
    /id="summary-execs-table"[\s\S]*?<thead>([\s\S]*?)<\/thead>/
  )[1];
  const summaryHeaders = extractThTexts(summaryTable);
  expect(summaryHeaders).toEqual([
    '',  // row label column (Active/Dead/Total)
    'RDD Blocks',
    'Storage Memory',
    'On Heap Storage Memory',
    'Off Heap Storage Memory',
    'Disk Used',
    'Cores',
    'Active Tasks',
    'Failed Tasks',
    'Complete Tasks',
    'Total Tasks',
    'Task Time (GC Time)',
    'Input',
    'Shuffle Read',
    'Shuffle Write',
    'Excluded',
  ]);
});

test('stagespage template has expected table structure', function () {
  const html = readStatic('stagespage-template.html');
  const template = extractTemplateContent(html, 'stages-summary-template');
  const tableIds = extractTableIds(template);

  expect(tableIds).toContain('summary-metrics-table');
  expect(tableIds).toContain('speculation-metrics-table');
  expect(tableIds).toContain('summary-executor-table');
  expect(tableIds).toContain('accumulator-table');
  expect(tableIds).toContain('active-tasks-table');

  // Task table columns
  const taskTable = template.match(
    /id="active-tasks-table"[\s\S]*?<thead>([\s\S]*?)<\/thead>/
  )[1];
  const taskHeaders = extractThTexts(taskTable);
  expect(taskHeaders).toEqual([
    'Index', 'Task ID', 'Attempt', 'Status', 'Locality level',
    'Executor ID', 'Host', 'Logs', 'Launch Time', 'Duration',
    'GC Time', 'Scheduler Delay', 'Task Deserialization Time',
    'Shuffle Read Fetch Wait Time', 'Shuffle Remote Reads',
    'Result Serialization Time', 'Getting Result Time',
    'Peak Execution Memory', 'Accumulators',
    'Input Size / Records', 'Output Size / Records',
    'Shuffle Write Time', 'Shuffle Write Size / Records',
    'Shuffle Read Size / Records', 'Spill (Memory)', 'Spill (Disk)', 'Errors',
  ]);
});
