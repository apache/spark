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

// this function works exactly the same as UIUtils.formatDuration
function formatDuration(milliseconds) {
    if (milliseconds < 100) {
        return milliseconds + " ms";
    }
    var seconds = milliseconds * 1.0 / 1000;
    if (seconds < 1) {
        return seconds.toFixed(1) + " s";
    }
    if (seconds < 60) {
        return seconds.toFixed(0) + " s";
    }
    var minutes = seconds / 60;
    if (minutes < 10) {
        return minutes.toFixed(1) + " min";
    } else if (minutes < 60) {
        return minutes.toFixed(0) + " min";
    }
    var hours = minutes / 60;
    return hours.toFixed(1) + " h";
}

function formatBytes(bytes, type) {
    if (type !== 'display') return bytes;
    if (bytes == 0) return '0.0 B';
    var k = 1000;
    var dm = 1;
    var sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

function padZeroes(num) {
  return ("0" + num).slice(-2);
}

function formatTimeMillis(timeMillis) {
  if (timeMillis <= 0) {
    return "-";
  } else {
    var dt = new Date(timeMillis);
    return dt.getFullYear() + "-" +
      padZeroes(dt.getMonth() + 1) + "-" +
      padZeroes(dt.getDate()) + " " +
      padZeroes(dt.getHours()) + ":" +
      padZeroes(dt.getMinutes()) + ":" +
      padZeroes(dt.getSeconds());
  }
}

function getTimeZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch(ex) {
    // Get time zone from a string representing the date,
    // eg. "Thu Nov 16 2017 01:13:32 GMT+0800 (CST)" -> "CST"
    return new Date().toString().match(/\((.*)\)/)[1];
  }
}
