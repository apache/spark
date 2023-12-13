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


import '../../core/src/main/resources/org/apache/spark/ui/static/d3.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/d3-flamegraph.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/flamegraph.js';
import {drawFlamegraph} from '../../core/src/main/resources/org/apache/spark/ui/static/flamegraph.js';
import $ from 'jquery';

/**
 * @jest-environment jsdom
 *
 * eslint-disable no-unused-vars
 */
test('drawFlamegraph', function () {
  document.body.innerHTML = `'<div>' +
    '<div id="executor-flamegraph-data" class="d-none">{"name":"apache","value":2,"children":[{"name":"spark","value":1,"children":[ ]}, {"name":"kyuubi","value":1,"children":[ ]} ]}</div>' +
    '<div id="executor-flamegraph-chart"></div></div>'`

  drawFlamegraph();

  expect($('#executor-flamegraph-chart').find('svg').length).toBe(1);
  expect($("[name=apache] title").html()).toBe('apache (100.000%, 2 samples)');
  expect($("[name=spark] title").html()).toBe('spark (50.000%, 1 samples)');
});
