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

// spark-sql-viz.js is a classic (non-module) script, so read it and return the helpers under test.
const __dirname = dirname(fileURLToPath(import.meta.url));
const vizPath = join(
  __dirname,
  '../../sql/core/src/main/resources/org/apache/spark/sql/execution/ui/static/spark-sql-viz.js');
const src = readFileSync(vizPath, 'utf8');

window.$ = function () {};
document.body.innerHTML = '<button id="plan-viz-download-btn"></button>';
const { buildMetricsTable, filterMetricRows } = new Function(
  src + '\nreturn { buildMetricsTable, filterMetricRows };')();

// A size metric renders a nested Total/Min/Med/Max stat sub-table inside its value cell.
const SIZE_VALUE =
  'total (min, med, max (stageId: taskId))\n25.0 MiB (3.8 MiB, N/A, 4.8 MiB (stage 2.0: task 4))';

function renderPanel(metrics) {
  document.body.innerHTML = buildMetricsTable(metrics, false, true);
  return document.body;
}

test('filterMetricRows keeps the matched metric value visible', function () {
  const body = renderPanel([
    { name: 'shuffle bytes written', value: SIZE_VALUE, type: 'size' },
    { name: 'number of output rows', value: '10', type: 'sum' }
  ]);

  filterMetricRows(body, 'shuffle');

  const outerRows = [...body.querySelectorAll('table.sortable > tbody > tr')];
  const matched = outerRows.find(r => /shuffle bytes written/.test(r.cells[0].textContent));
  const other = outerRows.find(r => /output rows/.test(r.cells[0].textContent));

  expect(matched.style.display).toBe('');
  expect(other.style.display).toBe('none');

  // The nested stat sub-table rows must not be hidden, else the value renders blank.
  const statRows = matched.querySelectorAll('td table tr');
  expect(statRows.length).toBeGreaterThan(0);
  statRows.forEach(r => expect(r.style.display).not.toBe('none'));
  expect(matched.cells[1].textContent).toContain('25.0 MiB');
});

test('filterMetricRows restores all rows when the query is cleared', function () {
  const body = renderPanel([
    { name: 'shuffle bytes written', value: SIZE_VALUE, type: 'size' },
    { name: 'number of output rows', value: '10', type: 'sum' }
  ]);

  filterMetricRows(body, 'shuffle');
  filterMetricRows(body, '');

  body.querySelectorAll('table.sortable > tbody > tr')
    .forEach(r => expect(r.style.display).toBe(''));
});
