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

import '../../core/src/main/resources/org/apache/spark/ui/static/jquery.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/jquery.dataTables.min.js';
import { formatLossReason } from '../../core/src/main/resources/org/apache/spark/ui/static/executorspage.js';

/* global $ */

/**
 * @jest-environment jsdom
 */

// Injects a tag if the reason is not HTML escaped.
const payload = '"><img src=x onerror=alert(1)>';

test('formatLossReason', function () {
  expect(formatLossReason("")).toBe("");
  expect(formatLossReason(null)).toBe("");
  expect(formatLossReason("Command exited with code 1")).toBe("Command exited with code 1");
  expect(formatLossReason(payload)).toBe(
    "&quot;&gt;&lt;img src=x onerror=alert(1)&gt;");
});

// The removal reason originates outside the driver: it can carry an executor-side exception
// message, YARN container diagnostics, or a Kubernetes pod status message. DataTables assigns the
// render output to the cell's innerHTML, so an unescaped reason would be parsed as markup.
test('formatLossReason escapes the reason DataTables renders as HTML', function () {
  document.body.innerHTML =
    '<table id="t"><thead><tr><th>Reason</th></tr></thead></table>';

  $('#t').DataTable({
    data: [{removeReason: payload}],
    columns: [{data: 'removeReason', render: formatLossReason}]
  });

  const cell = document.querySelector('#t tbody td');
  expect(cell.querySelector('img')).toBeNull();
  // The reason is still shown to the user, as text rather than markup.
  expect(cell.textContent).toBe(payload);
});
