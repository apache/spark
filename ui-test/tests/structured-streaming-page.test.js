/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import '../../core/src/main/resources/org/apache/spark/ui/static/d3.min.js';
import '../../core/src/main/resources/org/apache/spark/ui/static/jquery-3.5.1.min.js';
import { drawAreaStack } from '../../core/src/main/resources/org/apache/spark/ui/static/structured-streaming-page.js';

/* global $ */

/**
 * @jest-environment jsdom
 *
 * eslint-disable no-unused-vars
 */
test('drawAreaStack', function () {

  document.body.innerHTML = `'<div id="duration-area-stack-parent">' +
    '<div id="duration-area-stack"></div>' + 
    '</div>'`

  const labels = [
    "addBatch",
    "commitOffsets",
    "getBatch",
    "latestOffset",
    "queryPlanning",
    "walCommit"
  ];
  const values= [
    {
      "x": "12:22:58.756",
      "addBatch": "24925.0",
      "commitOffsets": "135.0",
      "getBatch": "1.0",
      "latestOffset": "0.0",
      "queryPlanning": "162.0",
      "walCommit": "130.0"
    },
    {
      "x": "12:23:24.129",
      "addBatch": "21899.0",
      "commitOffsets": "135.0",
      "getBatch": "0.0",
      "latestOffset": "0.0",
      "queryPlanning": "10.0",
      "walCommit": "133.0"
    },
    {
      "x": "12:23:46.309",
      "addBatch": "21873.0",
      "commitOffsets": "159.0",
      "getBatch": "0.0",
      "latestOffset": "0.0",
      "queryPlanning": "6.0",
      "walCommit": "134.0"
    }
  ];

  drawAreaStack("#duration-area-stack", labels, values);

  expect($('#duration-area-stack-parent').attr("style")).toBe('padding: 8px 0px 8px 8px; border-right: 0px solid white;');
  expect($('#duration-area-stack').find('svg').length).toBe(1);

  // test x axis
  var xAxis = $('[class="x axis"] text');
  expect(xAxis.length).toBe(2);
  xAxis.each((index, e) => {
    var xAixsText = "";
    if (index === 0) {
      xAixsText = "12:22:58.756";
    } else {
      xAixsText = "12:23:46.309";
    }
    expect(e.innerHTML).toBe(xAixsText)
  });

  // test y axis
  var yAxis = $('[class="y axis"] text');
  expect(yAxis.length).toBe(7);
  yAxis.each((index, e) => {
    if (index < yAxis.length - 1) {
      expect(e.innerHTML).toBe(5000 * index + "")
    } else {
      expect(e.innerHTML).toBe("ms")
    }
  });

  expect($('[class="cost"]').find('rect').length).toBe(18);
  expect($('[class="legend"]').find('rect').length).toBe(6);
});
