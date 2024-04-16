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


import '../../core/src/main/resources/org/apache/spark/ui/static/jquery-3.5.1.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/d3.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/d3-flamegraph.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/flamegraph.js';
import {drawFlamegraph, toggleFlamegraph} from '../../core/src/main/resources/org/apache/spark/ui/static/flamegraph.js';

/* global $, d3 */

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

test('toggleFlamegraph', function () {
  document.body.innerHTML = `'<div>' +
    '<div id="executor-flamegraph-header" class="arrow-open"></div>' +
    '<div id="executor-flamegraph-arrow" class="arrow-open"></div>' +
    '<div id="executor-flamegraph-chart" style="display: block"></div></div>'`

  toggleFlamegraph();

  d3.select("#executor-flamegraph-header").dispatch("click");
  expect($('#executor-flamegraph-arrow').hasClass('arrow-closed')).toBe(true);
  expect($('#executor-flamegraph-chart').css('display')).toBe('none');

  d3.select("#executor-flamegraph-header").dispatch("click");
  expect($('#executor-flamegraph-arrow').hasClass('arrow-closed')).toBe(false);
  expect($('#executor-flamegraph-chart').css('display')).toBe('block');
});
