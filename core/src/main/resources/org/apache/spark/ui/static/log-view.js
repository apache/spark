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

var baseParams

var curLogLength
var startByte
var endByte
var totalLogLength

var byteLength
var btnHeight = 30

function setLogScroll(oldHeight) {
  $(".log-content").scrollTop($(".log-content")[0].scrollHeight - oldHeight);
}

function tailLog() {
  $(".log-content").scrollTop($(".log-content")[0].scrollHeight);
}

function setLogData() {
  $('#log-data').html("Showing " + curLogLength + " Bytes: " + startByte
    + " - " + endByte + " of " + totalLogLength);
}

function disableMoreButton() {
  $(".log-more-btn").attr("disabled", "disabled");;
  $(".log-more-btn").html("Top of Log");
}

function noNewAlert() {
  $(".no-new-alert").css("display", "block");
  window.setTimeout(function () {$(".no-new-alert").css("display", "none");}, 4000);
}

function loadMore() {
  var offset = Math.max(startByte - byteLength, 0);
  var newLogLength = Math.min(curLogLength + byteLength, totalLogLength);

  $.ajax({
    type: "GET",
    url: "/log" + baseParams + "&offset=" + offset + "&byteLength=" + byteLength,
    success: function (data) {
      var oldHeight = $(".log-content")[0].scrollHeight;
      var dataInfo = data.substring(0, data.indexOf('\n')).match(/\d+/g);
      var retStartByte = dataInfo[0];
      var retLogLength = dataInfo[2];

      var cleanData = data.substring(data.indexOf('\n') + 1).trim();
      if (retStartByte == 0) {
        cleanData = cleanData.substring(0, startByte);
        disableMoreButton();
      }
      $("pre", ".log-content").prepend(cleanData);

      curLogLength = curLogLength + (startByte - retStartByte);
      startByte = retStartByte;
      totalLogLength = retLogLength;
      setLogScroll(oldHeight);
      setLogData();
    }
  });
}

function loadNew() {
  $.ajax({
    type: "GET",
    url: "/log" + baseParams,
    success: function (data) {
      var dataInfo = data.substring(0, data.indexOf('\n')).match(/\d+/g);
      var newDataLen = dataInfo[2] - totalLogLength;
      if (newDataLen != 0) {
        $.ajax({
          type: "GET",
          url: "/log" + baseParams + "&byteLength=" + newDataLen,
          success: function (data) {
            var dataInfo = data.substring(0, data.indexOf('\n')).match(/\d+/g);
            var retStartByte = dataInfo[0];
            var retEndByte = dataInfo[1];
            var retLogLength = dataInfo[2];

            var cleanData = data.substring(data.indexOf('\n') + 1).trim();
            $("pre", ".log-content").append(cleanData);

            curLogLength = curLogLength + (retEndByte - retStartByte);
            endByte = retEndByte;
            totalLogLength = retLogLength;
            tailLog();
            setLogData();
          }
        });
      } else {
        noNewAlert();
      }
    }
  });
}

function initLogPage(params, logLen, start, end, totLogLen, defaultLen) {
  baseParams = params;
  curLogLength = logLen;
  startByte = start;
  endByte = end;
  totalLogLength = totLogLen;
  byteLength = defaultLen;
  tailLog();
  if (startByte == 0) {
    disableMoreButton();
  }
}