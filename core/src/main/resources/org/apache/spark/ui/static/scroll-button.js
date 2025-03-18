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

/* global $ */

export { addScrollButton };

function addScrollButton() {
  const container = $('<div></div>', {
    'class': 'scroll-btn-container'
  });

  const topBtn = $('<div></div>', {
    'class': 'scroll-btn-half scroll-btn-top'
  }).click(function () {
    $('html, body').animate({scrollTop: 0}, 'slow');
  });

  const bottomBtn = $('<div></div>', {
    'class': 'scroll-btn-half scroll-btn-bottom'
  }).click(function () {
    $('html, body').animate({scrollTop: $(document).height()}, 'slow');
  });
  container.append(topBtn, bottomBtn);
  $('body').append(container);
}

$(document).ready(function () { addScrollButton(); });


