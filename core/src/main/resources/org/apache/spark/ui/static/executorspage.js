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

/* global $, Mustache */

import {
  createRESTEndPointForExecutorsPage, createRESTEndPointForMiscellaneousProcess, createTemplateURI,
  formatBytes, formatDate, formatDuration, formatLogsCells,
  getStandAloneAppId,
  setDataTableDefaults
} from './utils.js';

export { setHeapHistogramEnabled, setThreadDumpEnabled };

var threadDumpEnabled = false;
var heapHistogramEnabled = false;

/* eslint-disable no-unused-vars */
function setThreadDumpEnabled(val) {
  threadDumpEnabled = val;
}
function setHeapHistogramEnabled(val) {
  heapHistogramEnabled = val;
}
/* eslint-enable no-unused-vars */

function getThreadDumpEnabled() {
  return threadDumpEnabled;
}

function getHeapHistogramEnabled() {
  return heapHistogramEnabled;
}

function formatLossReason(removeReason) {
  if (removeReason) {
    return removeReason
  } else {
    return ""
  }
}

function formatStatus(status, type, row) {
  if (row.isExcluded) {
    return "Excluded";
  }

  if (status) {
    if (typeof row.excludedInStages === "undefined" || row.excludedInStages.length == 0) {
      return "Active"
    }
    return "Active (Excluded in Stages: [" + row.excludedInStages.join(", ") + "])";
  }
  return "Dead"
}

function formatProcessStatus(activeStatus) {
  if (activeStatus) {
    return "Active"
  }
  return "Dead"
}

function formatResourceCells(resources) {
  var result = ""
  var count = 0
  $.each(resources, function (name, resInfo) {
    if (count > 0) {
      result += ", "
    }
    result += name + ': [' + resInfo.addresses.join(", ") + ']';
    count += 1
  });
  return result
}

$.extend($.fn.dataTableExt.oSort, {
  "title-numeric-pre": function (a) {
    var x = a.match(/title="*(-?[0-9.]+)/)[1];
    return parseFloat(x);
  },

  "title-numeric-asc": function (a, b) {
    return ((a < b) ? -1 : ((a > b) ? 1 : 0));
  },

  "title-numeric-desc": function (a, b) {
    return ((a < b) ? 1 : ((a > b) ? -1 : 0));
  }
});

$.extend($.fn.dataTableExt.oSort, {
  "executor-id-asc": function ( a, b ) {
    if ($.isNumeric(a) && $.isNumeric(b)) {
      return parseFloat(a) - parseFloat(b);
    } else if (!$.isNumeric(a) && $.isNumeric(b)) {
      return -1;
    } else if ($.isNumeric(a) && !$.isNumeric(b)) {
      return 1;
    } else {
      return a.localeCompare(b);
    }
  },

  "executor-id-desc": function ( a, b ) {
    if ($.isNumeric(a) && $.isNumeric(b)) {
      return parseFloat(b) - parseFloat(a);
    } else if (!$.isNumeric(a) && $.isNumeric(b)) {
      return 1;
    } else if ($.isNumeric(a) && !$.isNumeric(b)) {
      return -1;
    } else {
      return b.localeCompare(a);
    }
  }
});

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function () {
  $.blockUI({message: '<h3>Loading Executors Page...</h3>'});
});

function logsExist(execs) {
  return execs.some(function(exec) {
    return !($.isEmptyObject(exec["executorLogs"]));
  });
}

// Determine Color Opacity from 0.5-1
// activeTasks range from 0 to maxTasks
function activeTasksAlpha(activeTasks, maxTasks) {
  return maxTasks > 0 ? ((activeTasks / maxTasks) * 0.5 + 0.5) : 1;
}

function activeTasksStyle(activeTasks, maxTasks) {
  return activeTasks > 0 ? ("hsla(240, 100%, 50%, " + activeTasksAlpha(activeTasks, maxTasks) + ")") : "";
}

// failedTasks range max at 10% failure, alpha max = 1
function failedTasksAlpha(failedTasks, totalTasks) {
  return totalTasks > 0 ?
    (Math.min(10 * failedTasks / totalTasks, 1) * 0.5 + 0.5) : 1;
}

function failedTasksStyle(failedTasks, totalTasks) {
  return failedTasks > 0 ?
    ("hsla(0, 100%, 50%, " + failedTasksAlpha(failedTasks, totalTasks) + ")") : "";
}

// totalDuration range from 0 to 50% GC time, alpha max = 1
function totalDurationAlpha(totalGCTime, totalDuration) {
  return totalDuration > 0 ?
    (Math.min(totalGCTime / totalDuration + 0.5, 1)) : 1;
}

var GCTimePercent = 0.1;

function totalDurationStyle(totalGCTime, totalDuration) {
  // Red if GC time over GCTimePercent of total time
  return (totalGCTime > GCTimePercent * totalDuration) ?
    ("hsla(0, 100%, 50%, " + totalDurationAlpha(totalGCTime, totalDuration) + ")") : "";
}

function totalDurationColor(totalGCTime, totalDuration) {
  return (totalGCTime > GCTimePercent * totalDuration) ? "white" : "black";
}

var sumOptionalColumns = [3, 4];
var execOptionalColumns = [5, 6, 7, 8, 9, 10, 13, 14, 26];
var execDataTable;
var sumDataTable;

function reselectCheckboxesBasedOnTaskTableState() {
  var allChecked = true;
  if (typeof execDataTable !== "undefined") {
    for (var k = 0; k < execOptionalColumns.length; k++) {
      if (execDataTable.column(execOptionalColumns[k]).visible()) {
        $("[data-exec-col-idx=" + execOptionalColumns[k] + "]").prop("checked", true);
      } else {
        allChecked = false;
      }
    }
  }
  if (allChecked) {
    $("#select-all-box").prop("checked", true);
  }
}

$(document).ready(function () {
  setDataTableDefaults();

  var executorsSummary = $("#active-executors");

  getStandAloneAppId(function (appId) {

    var endPoint = createRESTEndPointForExecutorsPage(appId);
    $.getJSON(endPoint, function (response, _ignored_status, _ignored_jqXHR) {
      var allExecCnt = 0;
      var allRDDBlocks = 0;
      var allMemoryUsed = 0;
      var allMaxMemory = 0;
      var allOnHeapMemoryUsed = 0;
      var allOnHeapMaxMemory = 0;
      var allOffHeapMemoryUsed = 0;
      var allOffHeapMaxMemory = 0;
      var allDiskUsed = 0;
      var allTotalCores = 0;
      var allMaxTasks = 0;
      var allActiveTasks = 0;
      var allFailedTasks = 0;
      var allCompletedTasks = 0;
      var allTotalTasks = 0;
      var allTotalDuration = 0;
      var allTotalGCTime = 0;
      var allTotalInputBytes = 0;
      var allTotalShuffleRead = 0;
      var allTotalShuffleWrite = 0;
      var allTotalExcluded = 0;

      var activeExecCnt = 0;
      var activeRDDBlocks = 0;
      var activeMemoryUsed = 0;
      var activeMaxMemory = 0;
      var activeOnHeapMemoryUsed = 0;
      var activeOnHeapMaxMemory = 0;
      var activeOffHeapMemoryUsed = 0;
      var activeOffHeapMaxMemory = 0;
      var activeDiskUsed = 0;
      var activeTotalCores = 0;
      var activeMaxTasks = 0;
      var activeActiveTasks = 0;
      var activeFailedTasks = 0;
      var activeCompletedTasks = 0;
      var activeTotalTasks = 0;
      var activeTotalDuration = 0;
      var activeTotalGCTime = 0;
      var activeTotalInputBytes = 0;
      var activeTotalShuffleRead = 0;
      var activeTotalShuffleWrite = 0;
      var activeTotalExcluded = 0;

      var deadExecCnt = 0;
      var deadRDDBlocks = 0;
      var deadMemoryUsed = 0;
      var deadMaxMemory = 0;
      var deadOnHeapMemoryUsed = 0;
      var deadOnHeapMaxMemory = 0;
      var deadOffHeapMemoryUsed = 0;
      var deadOffHeapMaxMemory = 0;
      var deadDiskUsed = 0;
      var deadTotalCores = 0;
      var deadMaxTasks = 0;
      var deadActiveTasks = 0;
      var deadFailedTasks = 0;
      var deadCompletedTasks = 0;
      var deadTotalTasks = 0;
      var deadTotalDuration = 0;
      var deadTotalGCTime = 0;
      var deadTotalInputBytes = 0;
      var deadTotalShuffleRead = 0;
      var deadTotalShuffleWrite = 0;
      var deadTotalExcluded = 0;

      response.forEach(function (exec) {
        var memoryMetrics = {
          usedOnHeapStorageMemory: 0,
          usedOffHeapStorageMemory: 0,
          totalOnHeapStorageMemory: 0,
          totalOffHeapStorageMemory: 0
        };

        // TODO: Replace hasOwnProperty with prototype.hasOwnProperty after we find it's safe to do.
        /* eslint-disable no-prototype-builtins */
        exec.memoryMetrics = exec.hasOwnProperty('memoryMetrics') ? exec.memoryMetrics : memoryMetrics;
      });

      response.forEach(function (exec) {
        allExecCnt += 1;
        allRDDBlocks += exec.rddBlocks;
        allMemoryUsed += exec.memoryUsed;
        allMaxMemory += exec.maxMemory;
        allOnHeapMemoryUsed += exec.memoryMetrics.usedOnHeapStorageMemory;
        allOnHeapMaxMemory += exec.memoryMetrics.totalOnHeapStorageMemory;
        allOffHeapMemoryUsed += exec.memoryMetrics.usedOffHeapStorageMemory;
        allOffHeapMaxMemory += exec.memoryMetrics.totalOffHeapStorageMemory;
        allDiskUsed += exec.diskUsed;
        allTotalCores += exec.totalCores;
        allMaxTasks += exec.maxTasks;
        allActiveTasks += exec.activeTasks;
        allFailedTasks += exec.failedTasks;
        allCompletedTasks += exec.completedTasks;
        allTotalTasks += exec.totalTasks;
        allTotalDuration += exec.totalDuration;
        allTotalGCTime += exec.totalGCTime;
        allTotalInputBytes += exec.totalInputBytes;
        allTotalShuffleRead += exec.totalShuffleRead;
        allTotalShuffleWrite += exec.totalShuffleWrite;
        allTotalExcluded += exec.isExcluded ? 1 : 0;
        if (exec.isActive) {
          activeExecCnt += 1;
          activeRDDBlocks += exec.rddBlocks;
          activeMemoryUsed += exec.memoryUsed;
          activeMaxMemory += exec.maxMemory;
          activeOnHeapMemoryUsed += exec.memoryMetrics.usedOnHeapStorageMemory;
          activeOnHeapMaxMemory += exec.memoryMetrics.totalOnHeapStorageMemory;
          activeOffHeapMemoryUsed += exec.memoryMetrics.usedOffHeapStorageMemory;
          activeOffHeapMaxMemory += exec.memoryMetrics.totalOffHeapStorageMemory;
          activeDiskUsed += exec.diskUsed;
          activeTotalCores += exec.totalCores;
          activeMaxTasks += exec.maxTasks;
          activeActiveTasks += exec.activeTasks;
          activeFailedTasks += exec.failedTasks;
          activeCompletedTasks += exec.completedTasks;
          activeTotalTasks += exec.totalTasks;
          activeTotalDuration += exec.totalDuration;
          activeTotalGCTime += exec.totalGCTime;
          activeTotalInputBytes += exec.totalInputBytes;
          activeTotalShuffleRead += exec.totalShuffleRead;
          activeTotalShuffleWrite += exec.totalShuffleWrite;
          activeTotalExcluded += exec.isExcluded ? 1 : 0;
        } else {
          deadExecCnt += 1;
          deadRDDBlocks += exec.rddBlocks;
          deadMemoryUsed += exec.memoryUsed;
          deadMaxMemory += exec.maxMemory;
          deadOnHeapMemoryUsed += exec.memoryMetrics.usedOnHeapStorageMemory;
          deadOnHeapMaxMemory += exec.memoryMetrics.totalOnHeapStorageMemory;
          deadOffHeapMemoryUsed += exec.memoryMetrics.usedOffHeapStorageMemory;
          deadOffHeapMaxMemory += exec.memoryMetrics.totalOffHeapStorageMemory;
          deadDiskUsed += exec.diskUsed;
          deadTotalCores += exec.totalCores;
          deadMaxTasks += exec.maxTasks;
          deadActiveTasks += exec.activeTasks;
          deadFailedTasks += exec.failedTasks;
          deadCompletedTasks += exec.completedTasks;
          deadTotalTasks += exec.totalTasks;
          deadTotalDuration += exec.totalDuration;
          deadTotalGCTime += exec.totalGCTime;
          deadTotalInputBytes += exec.totalInputBytes;
          deadTotalShuffleRead += exec.totalShuffleRead;
          deadTotalShuffleWrite += exec.totalShuffleWrite;
          deadTotalExcluded += exec.isExcluded ? 1 : 0; // todo - TEST BACKWARDS compatibility history?
        }
      });

      var totalSummary = {
        "execCnt": ( "Total(" + allExecCnt + ")"),
        "allRDDBlocks": allRDDBlocks,
        "allMemoryUsed": allMemoryUsed,
        "allMaxMemory": allMaxMemory,
        "allOnHeapMemoryUsed": allOnHeapMemoryUsed,
        "allOnHeapMaxMemory": allOnHeapMaxMemory,
        "allOffHeapMemoryUsed": allOffHeapMemoryUsed,
        "allOffHeapMaxMemory": allOffHeapMaxMemory,
        "allDiskUsed": allDiskUsed,
        "allTotalCores": allTotalCores,
        "allMaxTasks": allMaxTasks,
        "allActiveTasks": allActiveTasks,
        "allFailedTasks": allFailedTasks,
        "allCompletedTasks": allCompletedTasks,
        "allTotalTasks": allTotalTasks,
        "allTotalDuration": allTotalDuration,
        "allTotalGCTime": allTotalGCTime,
        "allTotalInputBytes": allTotalInputBytes,
        "allTotalShuffleRead": allTotalShuffleRead,
        "allTotalShuffleWrite": allTotalShuffleWrite,
        "allTotalExcluded": allTotalExcluded
      };
      var activeSummary = {
        "execCnt": ( "Active(" + activeExecCnt + ")"),
        "allRDDBlocks": activeRDDBlocks,
        "allMemoryUsed": activeMemoryUsed,
        "allMaxMemory": activeMaxMemory,
        "allOnHeapMemoryUsed": activeOnHeapMemoryUsed,
        "allOnHeapMaxMemory": activeOnHeapMaxMemory,
        "allOffHeapMemoryUsed": activeOffHeapMemoryUsed,
        "allOffHeapMaxMemory": activeOffHeapMaxMemory,
        "allDiskUsed": activeDiskUsed,
        "allTotalCores": activeTotalCores,
        "allMaxTasks": activeMaxTasks,
        "allActiveTasks": activeActiveTasks,
        "allFailedTasks": activeFailedTasks,
        "allCompletedTasks": activeCompletedTasks,
        "allTotalTasks": activeTotalTasks,
        "allTotalDuration": activeTotalDuration,
        "allTotalGCTime": activeTotalGCTime,
        "allTotalInputBytes": activeTotalInputBytes,
        "allTotalShuffleRead": activeTotalShuffleRead,
        "allTotalShuffleWrite": activeTotalShuffleWrite,
        "allTotalExcluded": activeTotalExcluded
      };
      var deadSummary = {
        "execCnt": ( "Dead(" + deadExecCnt + ")" ),
        "allRDDBlocks": deadRDDBlocks,
        "allMemoryUsed": deadMemoryUsed,
        "allMaxMemory": deadMaxMemory,
        "allOnHeapMemoryUsed": deadOnHeapMemoryUsed,
        "allOnHeapMaxMemory": deadOnHeapMaxMemory,
        "allOffHeapMemoryUsed": deadOffHeapMemoryUsed,
        "allOffHeapMaxMemory": deadOffHeapMaxMemory,
        "allDiskUsed": deadDiskUsed,
        "allTotalCores": deadTotalCores,
        "allMaxTasks": deadMaxTasks,
        "allActiveTasks": deadActiveTasks,
        "allFailedTasks": deadFailedTasks,
        "allCompletedTasks": deadCompletedTasks,
        "allTotalTasks": deadTotalTasks,
        "allTotalDuration": deadTotalDuration,
        "allTotalGCTime": deadTotalGCTime,
        "allTotalInputBytes": deadTotalInputBytes,
        "allTotalShuffleRead": deadTotalShuffleRead,
        "allTotalShuffleWrite": deadTotalShuffleWrite,
        "allTotalExcluded": deadTotalExcluded
      };

      var data = {executors: response, "execSummary": [activeSummary, deadSummary, totalSummary]};
      $.get(createTemplateURI(appId, "executorspage"), function (template) {

        executorsSummary.append(Mustache.render($(template).filter("#executors-summary-template").html(), data));
        var selector = "#active-executors-table";
        var conf = {
          "data": response,
          "columns": [
            {
              data: "id",
              type: "executor-id"
            },
            {data: 'hostPort'},
            {
              data: 'isActive',
              render: function (data, type, row) {
                return formatStatus (data, type, row);
              }
            },
            {data: 'rddBlocks'},
            {
              data: function (row, type) {
                if (type !== 'display')
                  return row.memoryUsed;
                else
                  return (formatBytes(row.memoryUsed, type) + ' / ' +
                    formatBytes(row.maxMemory, type));
              }
            },
            {
              data: function (row, type) {
                if (type !== 'display')
                  return row.memoryMetrics.usedOnHeapStorageMemory;
                else
                  return (formatBytes(row.memoryMetrics.usedOnHeapStorageMemory, type) + ' / ' +
                    formatBytes(row.memoryMetrics.totalOnHeapStorageMemory, type));
              }
            },
            {
              data: function (row, type) {
                if (type !== 'display')
                  return row.memoryMetrics.usedOffHeapStorageMemory;
                else
                  return (formatBytes(row.memoryMetrics.usedOffHeapStorageMemory, type) + ' / ' +
                    formatBytes(row.memoryMetrics.totalOffHeapStorageMemory, type));
              }
            },
            {
              data: function (row, type) {
                var peakMemoryMetrics = row.peakMemoryMetrics;
                if (typeof peakMemoryMetrics !== 'undefined') {
                  if (type !== 'display')
                    return peakMemoryMetrics.JVMHeapMemory;
                  else
                    return (formatBytes(peakMemoryMetrics.JVMHeapMemory, type) + ' / ' +
                      formatBytes(peakMemoryMetrics.JVMOffHeapMemory, type));
                } else {
                  if (type !== 'display') {
                    return 0;
                  } else {
                    return '0.0 B / 0.0 B';
                  }
                }
              }
            },
            {
              data: function (row, type) {
                var peakMemoryMetrics = row.peakMemoryMetrics;
                if (typeof peakMemoryMetrics !== 'undefined') {
                  if (type !== 'display')
                    return peakMemoryMetrics.OnHeapExecutionMemory;
                  else
                    return (formatBytes(peakMemoryMetrics.OnHeapExecutionMemory, type) + ' / ' +
                      formatBytes(peakMemoryMetrics.OffHeapExecutionMemory, type));
                } else {
                  if (type !== 'display') {
                    return 0;
                  } else {
                    return '0.0 B / 0.0 B';
                  }
                }
              }
            },
            {
              data: function (row, type) {
                var peakMemoryMetrics = row.peakMemoryMetrics;
                if (typeof peakMemoryMetrics !== 'undefined') {
                  if (type !== 'display')
                    return peakMemoryMetrics.OnHeapStorageMemory;
                  else
                    return (formatBytes(peakMemoryMetrics.OnHeapStorageMemory, type) + ' / ' +
                      formatBytes(peakMemoryMetrics.OffHeapStorageMemory, type));
                } else {
                  if (type !== 'display') {
                    return 0;
                  } else {
                    return '0.0 B / 0.0 B';
                  }
                }
              }
            },
            {
              data: function (row, type) {
                var peakMemoryMetrics = row.peakMemoryMetrics;
                if (typeof peakMemoryMetrics !== 'undefined') {
                  if (type !== 'display')
                    return peakMemoryMetrics.DirectPoolMemory;
                  else
                    return (formatBytes(peakMemoryMetrics.DirectPoolMemory, type) + ' / ' +
                      formatBytes(peakMemoryMetrics.MappedPoolMemory, type));
                } else {
                  if (type !== 'display') {
                    return 0;
                  } else {
                    return '0.0 B / 0.0 B';
                  }
                }
              }
            },
            {data: 'diskUsed', render: formatBytes},
            {data: 'totalCores'},
            {name: 'resourcesCol', data: 'resources', render: formatResourceCells, orderable: false},
            {name: 'resourceProfileIdCol', data: 'resourceProfileId'},
            {
              data: 'activeTasks',
              "fnCreatedCell": function (nTd, sData, oData, _ignored_iRow, _ignored_iCol) {
                if (sData > 0) {
                  $(nTd).css('color', 'white');
                  $(nTd).css('background', activeTasksStyle(oData.activeTasks, oData.maxTasks));
                }
              }
            },
            {
              data: 'failedTasks',
              "fnCreatedCell": function (nTd, sData, oData, _ignored_iRow, _ignored_iCol) {
                if (sData > 0) {
                  $(nTd).css('color', 'white');
                  $(nTd).css('background', failedTasksStyle(oData.failedTasks, oData.totalTasks));
                }
              }
            },
            {data: 'completedTasks'},
            {data: 'totalTasks'},
            {
              data: function (row, type) {
                return type === 'display' ? (formatDuration(row.totalDuration) + ' (' + formatDuration(row.totalGCTime) + ')') : row.totalDuration
              },
              "fnCreatedCell": function (nTd, sData, oData, _ignored_iRow, _ignored_iCol) {
                if (oData.totalDuration > 0) {
                  $(nTd).css('color', totalDurationColor(oData.totalGCTime, oData.totalDuration));
                  $(nTd).css('background', totalDurationStyle(oData.totalGCTime, oData.totalDuration));
                }
              }
            },
            {data: 'totalInputBytes', render: formatBytes},
            {data: 'totalShuffleRead', render: formatBytes},
            {data: 'totalShuffleWrite', render: formatBytes},
            {name: 'executorLogsCol', data: 'executorLogs', render: formatLogsCells},
            {
              name: 'threadDumpCol',
              data: function (row) { return row.isActive ? row.id : '' },
              render: function (data, type) {
                return data != '' && type === 'display' ? ("<a href='threadDump/?executorId=" + data + "'>Thread Dump</a>" ) : data;
              }
            },
            {
              name: 'heapHistogramCol',
              data: function (row) { return row.isActive ? row.id : '' },
              render: function (data, type) {
                return data != '' && type === 'display' ? ("<a href='heapHistogram/?executorId=" + data + "'>Heap Histogram</a>") : data;
              }
            },
            {
              data: 'removeReason',
              render: formatLossReason
            },
            {
              data: 'addTime',
              render: formatDate
            },
            {
              data: 'removeTime',
              render: formatDate
            }
          ],
          "order": [[0, "asc"]],
          "columnDefs": [
            {"visible": false, "targets": execOptionalColumns}
          ],
          "deferRender": true
        };

        execDataTable = $(selector).DataTable(conf);
        execDataTable.column('executorLogsCol:name').visible(logsExist(response));
        execDataTable.column('threadDumpCol:name').visible(getThreadDumpEnabled());
        execDataTable.column('heapHistogramCol:name').visible(getHeapHistogramEnabled());
        $('#active-executors [data-toggle="tooltip"]').tooltip();
    
        // This section should be visible once API gives the response.
        $('.active-process-container').hide();
        var endPoint = createRESTEndPointForMiscellaneousProcess(appId);
        $.getJSON(endPoint, function( response, _ignored_status, _ignored_jqXHR ) {
          if (response.length) {
            var processSummaryResponse = response;
            var processSummaryConf = {
              "data": processSummaryResponse,
              "columns": [{
                data: "id"
              },
              {
                data: "hostPort"
              },
              {
                data: function(row) {
                  return formatProcessStatus(row.isActive);
                }
              },
              {
                data: "totalCores"
              },
              {
                data: "processLogs",
                render: formatLogsCells
              },
              ],
              "deferRender": true,
              "order": [
                [0, "asc"]
              ],
              "bAutoWidth": false,
              "oLanguage": {
                "sEmptyTable": "No data to show yet"
              }
            };
            $("#active-process-table").DataTable(processSummaryConf);
            $('.active-process-container').show()
          }
        });

        var sumSelector = "#summary-execs-table";
        var sumConf = {
          "data": [activeSummary, deadSummary, totalSummary],
          "columns": [
            {
              data: 'execCnt',
              "fnCreatedCell": function (nTd, _ignored_sData, _ignored_oData, _ignored_iRow, _ignored_iCol) {
                $(nTd).css('font-weight', 'bold');
              }
            },
            {data: 'allRDDBlocks'},
            {
              data: function (row, type) {
                if (type !== 'display')
                  return row.allMemoryUsed;
                else
                  return (formatBytes(row.allMemoryUsed, type) + ' / ' +
                  formatBytes(row.allMaxMemory, type));
              }
            },
            {
              data: function (row, type) {
                if (type !== 'display')
                  return row.allOnHeapMemoryUsed;
                else
                  return (formatBytes(row.allOnHeapMemoryUsed, type) + ' / ' +
                    formatBytes(row.allOnHeapMaxMemory, type));
              }
            },
            {
              data: function (row, type) {
                if (type !== 'display')
                  return row.allOffHeapMemoryUsed;
                else
                  return (formatBytes(row.allOffHeapMemoryUsed, type) + ' / ' +
                    formatBytes(row.allOffHeapMaxMemory, type));
              }
            },
            {data: 'allDiskUsed', render: formatBytes},
            {data: 'allTotalCores'},
            {
              data: 'allActiveTasks',
              "fnCreatedCell": function (nTd, sData, oData, _ignored_iRow, _ignored_iCol) {
                if (sData > 0) {
                  $(nTd).css('color', 'white');
                  $(nTd).css('background', activeTasksStyle(oData.allActiveTasks, oData.allMaxTasks));
                }
              }
            },
            {
              data: 'allFailedTasks',
              "fnCreatedCell": function (nTd, sData, oData, _ignored_iRow, _ignored_iCol) {
                if (sData > 0) {
                  $(nTd).css('color', 'white');
                  $(nTd).css('background', failedTasksStyle(oData.allFailedTasks, oData.allTotalTasks));
                }
              }
            },
            {data: 'allCompletedTasks'},
            {data: 'allTotalTasks'},
            {
              data: function (row, type) {
                return type === 'display' ? (formatDuration(row.allTotalDuration) + ' (' + formatDuration(row.allTotalGCTime) + ')') : row.allTotalDuration
              },
              "fnCreatedCell": function (nTd, sData, oData, _ignored_iRow, _ignored_iCol) {
                if (oData.allTotalDuration > 0) {
                  $(nTd).css('color', totalDurationColor(oData.allTotalGCTime, oData.allTotalDuration));
                  $(nTd).css('background', totalDurationStyle(oData.allTotalGCTime, oData.allTotalDuration));
                }
              }
            },
            {data: 'allTotalInputBytes', render: formatBytes},
            {data: 'allTotalShuffleRead', render: formatBytes},
            {data: 'allTotalShuffleWrite', render: formatBytes},
            {data: 'allTotalExcluded'}
          ],
          "paging": false,
          "searching": false,
          "info": false,
          "columnDefs": [
            {"visible": false, "targets": sumOptionalColumns}
          ]
        };

        sumDataTable = $(sumSelector).DataTable(sumConf);
        $('#execSummary [data-toggle="tooltip"]').tooltip();
  
        $("#showAdditionalMetrics").append(
          "<div><a id='additionalMetrics' class='collapse-table'>" +
          "<span class='expand-input-rate-arrow arrow-closed' id='arrowtoggle-optional-metrics'></span>" +
          "Show Additional Metrics" +
          "</a></div>" +
          "<div class='container-fluid-div ml-4 d-none' id='toggle-metrics'>" +
          "<div><input type='checkbox' class='toggle-vis' id='select-all-box'> Select All</div>" +
          "<div id='on_heap_memory' class='on-heap-memory-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='3' data-exec-col-idx='5'> On Heap Memory</div>" +
          "<div id='off_heap_memory' class='off-heap-memory-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='4' data-exec-col-idx='6'> Off Heap Memory</div>" +
          "<div id='jvm_on_off_heap_memory' class='jvm_on_off_heap_memory-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='7'> Peak JVM Memory OnHeap / OffHeap</div>" +
          "<div id='on_off_heap_execution_memory' class='on_off_heap_execution_memory-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='8'> Peak Execution Memory OnHeap / OffHeap</div>" +
          "<div id='on_off_heap_storage_memory' class='on_off_heap_storage_memory'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='9'> Peak Storage Memory OnHeap / OffHeap</div>" +
          "<div id='direct_mapped_pool_memory' class='direct_mapped_pool_memory-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='10'> Peak Pool Memory Direct / Mapped</div>" +
          "<div id='extra_resources' class='resources-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='13'> Resources</div>" +
          "<div id='resource_prof_id' class='resource-prof-id-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='14'> Resource Profile Id</div>" +
          "<div id='exec_loss_reason' class='exec-loss-reason-checkbox-div'><input type='checkbox' class='toggle-vis' data-sum-col-idx='' data-exec-col-idx='26'> Exec Loss Reason</div>" +
          "</div>");

        reselectCheckboxesBasedOnTaskTableState();

        $("#additionalMetrics").click(function() {
          $("#arrowtoggle-optional-metrics").toggleClass("arrow-open arrow-closed");
          $("#toggle-metrics").toggleClass("d-none");
          if (window.localStorage) {
            window.localStorage.setItem("arrowtoggle-optional-metrics-class", $("#arrowtoggle-optional-metrics").attr('class'));
          }
        });

        $(".toggle-vis").on("click", function() {
          var thisBox = $(this);
          if (thisBox.is("#select-all-box")) {
            var sumColumn = sumDataTable.columns(sumOptionalColumns);
            var execColumn = execDataTable.columns(execOptionalColumns);
            if (thisBox.is(":checked")) {
              $(".toggle-vis").prop("checked", true);
              sumColumn.visible(true);
              execColumn.visible(true);
            } else {
              $(".toggle-vis").prop("checked", false);
              sumColumn.visible(false);
              execColumn.visible(false);
            }
          } else {
            var execColIdx = thisBox.attr("data-exec-col-idx");
            var execCol = execDataTable.column(execColIdx);
            execCol.visible(!execCol.visible());
            var sumColIdx = thisBox.attr("data-sum-col-idx");
            if (sumColIdx) {
              var sumCol = sumDataTable.column(sumColIdx);
              sumCol.visible(!sumCol.visible());
            }
          }
        });

        if (window.localStorage) {
          if (window.localStorage.getItem("arrowtoggle-optional-metrics-class") != null &&
            window.localStorage.getItem("arrowtoggle-optional-metrics-class").includes("arrow-open")) {
            $("#arrowtoggle-optional-metrics").toggleClass("arrow-open arrow-closed");
            $("#toggle-metrics").toggleClass("d-none");
          }
        }
      });
    });
  });
});
