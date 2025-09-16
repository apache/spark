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

export {
  ConvertDurationString, createRESTEndPointForExecutorsPage, createRESTEndPointForMiscellaneousProcess, createTemplateURI,
  errorMessageCell, errorSummary,
  formatBytes, formatDate, formatDuration, formatLogsCells, formatTimeMillis,
  getBaseURI, getStandAloneAppId, getTimeZone,
  setDataTableDefaults, stringAbbreviate
};

/* global $, uiRoot */
// this function works exactly the same as UIUtils.formatDuration
function formatDuration(milliseconds) {
  if (milliseconds < 100) {
    return parseInt(milliseconds).toFixed(1) + " ms";
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
  if (bytes <= 0) return '0.0 B';
  var k = 1024;
  var dm = 1;
  var sizes = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB', 'EiB', 'ZiB', 'YiB'];
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
    return formatDateString(dt);
  }
}

function formatDateString(dt) {
  return dt.getFullYear() + "-" +
    padZeroes(dt.getMonth() + 1) + "-" +
    padZeroes(dt.getDate()) + " " +
    padZeroes(dt.getHours()) + ":" +
    padZeroes(dt.getMinutes()) + ":" +
    padZeroes(dt.getSeconds());
}

function getTimeZone() {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone;
  } catch(_ignored_ex) {
    // Get time zone from a string representing the date,
    // e.g. "Thu Nov 16 2017 01:13:32 GMT+0800 (CST)" -> "CST"
    return new Date().toString().match(/\((.*)\)/)[1];
  }
}

function formatLogsCells(execLogs, type) {
  if (type !== 'display') return Object.keys(execLogs);
  if (!execLogs) return;
  var result = '';
  $.each(execLogs, function (logName, logUrl) {
    result += '<div><a href=' + logUrl + '>' + logName + '</a></div>'
  });
  return result;
}

function getStandAloneAppId(cb) {
  var words = getBaseURI().split('/');
  var ind = words.indexOf("proxy");
  var appId;
  if (ind > 0) {
    appId = words[ind + 1];
    cb(appId);
    return;
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    cb(appId);
    return;
  }
  // Looks like Web UI is running in standalone mode
  // Let's get application-id using REST End Point
  $.getJSON(uiRoot + "/api/v1/applications", function(response, _ignored_status, _ignored_jqXHR) {
    if (response && response.length > 0) {
      var appId = response[0].id;
      cb(appId);
      return;
    }
  });
}

// This function is a helper function for sorting in datatable.
// When the data is in duration (e.g. 12ms 2s 2min 2h )
// It will convert the string into integer for correct ordering
function ConvertDurationString(data) {
  data = data.toString();
  var units = data.replace(/[\d.]/g, '' )
                  .replace(' ', '')
                  .toLowerCase();
  var multiplier = 1;

  switch(units) {
    case 's':
      multiplier = 1000;
      break;
    case 'min':
      multiplier = 600000;
      break;
    case 'h':
      multiplier = 3600000;
      break;
    default:
      break;
  }
  return parseFloat(data) * multiplier;
}

function createTemplateURI(appId, templateName) {
  var words = getBaseURI().split('/');
  var ind = words.indexOf("proxy");
  var baseURI;
  if (ind > 0) {
    baseURI = words.slice(0, ind + 1).join('/') + '/' + appId + '/static/' + templateName + '-template.html';
    return baseURI;
  }
  ind = words.indexOf("history");
  if(ind > 0) {
    baseURI = words.slice(0, ind).join('/') + '/static/' + templateName + '-template.html';
    return baseURI;
  }
  return uiRoot + "/static/" + templateName + "-template.html";
}

function setDataTableDefaults() {
  $.extend($.fn.dataTable.defaults, {
    stateSave: true,
    stateSaveParams: function(_, data) {
      data.search.search = "";
    },
    lengthMenu: [[20, 40, 60, 100, -1], [20, 40, 60, 100, "All"]],
    pageLength: 20
  });
}

function formatDate(date) {
  if (!date || date <= 0) return "-";
  else {
    var dt = new Date(date.replace("GMT", "Z"));
    return formatDateString(dt);
  }
}

function createRESTEndPointForExecutorsPage(appId) {
  var words = getBaseURI().split('/');
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join('/');
    return newBaseURI + "/api/v1/applications/" + appId + "/allexecutors";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join('/');
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId + "/allexecutors";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/allexecutors";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId + "/allexecutors";
}

function createRESTEndPointForMiscellaneousProcess(appId) {
  var words = getBaseURI().split('/');
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join('/');
    return newBaseURI + "/api/v1/applications/" + appId + "/allmiscellaneousprocess";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join('/');
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId + "/allmiscellaneousprocess";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/allmiscellaneousprocess";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId + "/allmiscellaneousprocess";
}

function getBaseURI() {
  return document.baseURI || document.URL;
}

function detailsUINode(isMultiline, message) {
  if (isMultiline) {
    const span = '<span onclick="this.parentNode.querySelector(\'.stacktrace-details\').classList.toggle(\'collapsed\')" class="expand-details">+details</span>';
    const pre = '<pre>' + message + '</pre>';
    const div = '<div class="stacktrace-details collapsed">' + pre + '</div>';
    return span + div;
  } else {
    return '';
  }
}

const ERROR_CLASS_REGEX = /\[([A-Z][A-Z_.]+[A-Z])]/;

/* This function works exactly the same as UIUtils.errorSummary, it shall be
 * remained the same whichever changed */
function errorSummary(errorMessage) {
  let isMultiline = true;
  const maybeErrorClass = errorMessage.match(ERROR_CLASS_REGEX);
  let errorClassOrBrief;
  if (maybeErrorClass) {
    errorClassOrBrief = maybeErrorClass[1];
  } else if (errorMessage.indexOf('\n') >= 0) {
    errorClassOrBrief = errorMessage.substring(0, errorMessage.indexOf('\n'));
  } else if (errorMessage.indexOf(":") >= 0) {
    errorClassOrBrief = errorMessage.substring(0, errorMessage.indexOf(":"));
  } else {
    isMultiline = false;
    errorClassOrBrief = errorMessage;
  }
  return [errorClassOrBrief, isMultiline];
}

function errorMessageCell(errorMessage) {
  const [summary, isMultiline] = errorSummary(errorMessage);
  const details = detailsUINode(isMultiline, errorMessage);
  return summary + details;
}

function stringAbbreviate(content, limit) {
  if (content && content.length > limit) {
    const summary = content.substring(0, limit) + '...';
    // TODO: Reused stacktrace-details* style for convenience, but it's not really a stacktrace
    // Consider creating a new style for this case if stacktrace-details is not appropriate in
    // the future.
    const details = detailsUINode(true, content);
    return summary + details;
  } else {
    return content;
  }
}
