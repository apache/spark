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

// log-view.js is a classic (non-module) script, so read it and return the helper under test.
// The script reads the global uiRoot, which webui.js sets from the server-rendered UI root.
const __dirname = dirname(fileURLToPath(import.meta.url));
const logViewPath = join(
  __dirname, '../../core/src/main/resources/org/apache/spark/ui/static/log-view.js');
const { getRESTEndPoint } = new Function(
  readFileSync(logViewPath, 'utf8') + '\nreturn { getRESTEndPoint };')();

function openLogPage(root, path) {
  window.uiRoot = root;
  window.history.replaceState(null, '', path);
}

// A History Server behind an ingress (spark.ui.proxyBase) or Knox (X-Forwarded-Context) is only
// reachable under the UI root.
test('getRESTEndPoint prefixes the UI root', function () {
  openLogPage('/gateway/default/sparkhistoryui',
    '/gateway/default/sparkhistoryui/logPage/?self&logType=out');
  expect(getRESTEndPoint()).toBe('/gateway/default/sparkhistoryui/log');
});

// A worker's UI root is the reverse proxy URL without the /proxy/<workerId> part.
test('getRESTEndPoint keeps the reverse proxy path to a worker', function () {
  openLogPage('/path/to/spark',
    '/path/to/spark/proxy/worker-1/logPage/?appId=app-1&executorId=0&logType=stdout');
  expect(getRESTEndPoint()).toBe('http://localhost/path/to/spark/proxy/worker-1/log');
});
