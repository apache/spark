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
import { addScrollButton } from '../../core/src/main/resources/org/apache/spark/ui/static/scroll-button.js';

/* global $ */

/**
 * @jest-environment jsdom
 *
 * eslint-disable no-unused-vars
 */
test('addScrollButton', function ()  {
  addScrollButton();
  expect($('.scroll-btn-container').length).toBe(1);
  expect($('.scroll-btn-half').length).toBe(2);
  expect($('.scroll-btn-top').length).toBe(1);
  expect($('.scroll-btn-bottom').length).toBe(1);
});
