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

// This is copied from `utils.js`. Please sync this with it.
function formatTimeMillis(timeMillis) {
  if (timeMillis <= 0) {
    return "-";
  } else {
    var dt = new Date(timeMillis);
    return formatDateString(dt);
  }
}

// This is copied from `utils.js`. Please sync this with it.
function padZeroes(num) {
  return ("0" + num).slice(-2);
}

// This is copied from `utils.js`. Please sync this with it.
function formatDateString(dt) {
  return dt.getFullYear() + "-" +
    padZeroes(dt.getMonth() + 1) + "-" +
    padZeroes(dt.getDate()) + " " +
    padZeroes(dt.getHours()) + ":" +
    padZeroes(dt.getMinutes()) + ":" +
    padZeroes(dt.getSeconds());
}

// This is copied from `utils.js`. Please sync this with it.
function getTimeZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch(_ignored_ex) {
    // Get time zone from a string representing the date,
    // e.g. "Thu Nov 16 2017 01:13:32 GMT+0800 (CST)" -> "CST"
    return new Date().toString().match(/\((.*)\)/)[1];
  }
}

$(document).ready(function() {
  if ($('#last-updated').length) {
    var lastUpdatedMillis = Number($('#last-updated').text());
    $('#last-updated').text(formatTimeMillis(lastUpdatedMillis));
  }

  $('#time-zone').text(getTimeZone());
});
