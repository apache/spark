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
function formatDuration(milliseconds, type) {
  if(type !== 'display') return milliseconds;
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

function makeIdNumeric(id) {
  var strs = id.split("_");
  if (strs.length < 3) {
    return id;
  }
  var appSeqNum = strs[2];
  var resl = strs[0] + "_" + strs[1] + "_";
  var diff = 10 - appSeqNum.length;
  while (diff > 0) {
      resl += "0"; // padding 0 before the app sequence number to make sure it has 10 characters
      diff--;
  }
  resl += appSeqNum;
  return resl;
}

function formatDate(date) {
  return date.split(".")[0].replace("T", " ");
}

function getParameterByName(name, searchString) {
  var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
  results = regex.exec(searchString);
  return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function formatStatus(status, type) {
    if(type !== 'display') return status;
    if(status) {
        return "Active"
    } else {
        return "Dead"
    }
}

function formatBytes(bytes,type) {
    if(type !== 'display') return bytes;
    if(bytes == 0) return '0 B';
    var k = 1000;
    var dm = 3;
    var sizes = ['B', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];
    var i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}


jQuery.extend( jQuery.fn.dataTableExt.oSort, {
    "title-numeric-pre": function ( a ) {
        var x = a.match(/title="*(-?[0-9\.]+)/)[1];
        return parseFloat( x );
    },

    "title-numeric-asc": function ( a, b ) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "title-numeric-desc": function ( a, b ) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

jQuery.extend( jQuery.fn.dataTableExt.oSort, {
    "appid-numeric-pre": function ( a ) {
        var x = a.match(/title="*(-?[0-9a-zA-Z\-\_]+)/)[1];
        return makeIdNumeric(x);
    },

    "appid-numeric-asc": function ( a, b ) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "appid-numeric-desc": function ( a, b ) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function(){
    $.blockUI({ message: '<h3>Loading Executors Page...</h3>'});
});

function createBaseURI() {
    var parser = document.createElement('a');
    var words = parser.baseURI.split('/');
    var ind = words.indexOf("proxy");
    var appId = words[ind + 1];
    var baseURI = words.slice(0, ind + 1).join('/') + '/' + appId;
    return baseURI;
}

function createRESTEndPoint() {
    var parser = document.createElement('a');
    var words = parser.baseURI.split('/');
    var ind = words.indexOf("proxy");
    var appId = words[ind + 1];
    var newBaseURI = words.slice(0, ind + 2).join('/');

    return newBaseURI + "/api/v1/applications/" + appId +"/allexecutors"

}

function formatLogsCells(execLogs, type) {
    if(type !== 'display') return Object.keys(execLogs);
    if(!execLogs) return;
    var result = '';
    $.each(execLogs, function(logName, logUrl) {
        result += '<div><a href=' + logUrl + '>' + logName + '</a></div>'
    });
    return result;
}

// Determine Color Opacity from 0.5-1
// activeTasks range from 0 to maxTasks
function activeTasksAlpha(activeTasks, maxTasks) {
    return maxTasks > 0 ? ((activeTasks / maxTasks) * 0.5 + 0.5) : 1;
}

function activeTasksStyle(activeTasks, maxTasks) {
    return  activeTasks > 0 ? ("hsla(240, 100%, 50%, " + activeTasksAlpha(activeTasks, maxTasks) + ")") : "";
}

function activeTasksColor(activeTasks, maxTasks) {
    return  activeTasks > 0 ? ("hsla(240, 100%, 50%, " + activeTasksAlpha(activeTasks, maxTasks) + ")") : "";
}

// failedTasks range max at 10% failure, alpha max = 1
function failedTasksAlpha(failedTasks, totalTasks) {
    return totalTasks > 0 ?
        (Math.min(10 * failedTasks / totalTasks, 1) * 0.5 + 0.5) : 1;
}

function failedTasksStyle(failedTasks, totalTasks){
    return failedTasks > 0 ?
        ("hsla(0, 100%, 50%, " + failedTasksAlpha(failedTasks, totalTasks) + ")") : "";
}

function failedTasksColor(failedTasks, totalTasks){
    return failedTasks > 0 ? "white" : "black";
}

// totalDuration range from 0 to 50% GC time, alpha max = 1
function totalDurationAlpha(totalGCTime, totalDuration) {
    return totalDuration > 0 ?
        (Math.min(totalGCTime / totalDuration + 0.5, 1)) : 1;
}

function totalDurationStyle(totalGCTime, totalDuration) {
    // Red if GC time over GCTimePercent of total time
    // When GCTimePercent is edited change ToolTips.TASK_TIME to match
    var GCTimePercent = 0.1;
    return (totalGCTime > GCTimePercent * totalDuration) ?
        ("hsla(0, 100%, 50%, " + totalDurationAlpha(totalGCTime, totalDuration) + ")") : "";
}

function totalDurationColor(totalGCTime, totalDuration) {
    // Red if GC time over GCTimePercent of total time
    // When GCTimePercent is edited change ToolTips.TASK_TIME to match
    var GCTimePercent = 0.1;
    return (totalGCTime > GCTimePercent * totalDuration) ? "white" : "black";
}

$(document).ready(function() {
    $.extend( $.fn.dataTable.defaults, {
      stateSave: true,
      lengthMenu: [[20,40,60,100,-1], [20, 40, 60, 100, "All"]],
      pageLength: 20
    });

    executorsSummary = $("#active-executors");
    searchString = executorsSummary["context"]["location"]["search"];
    requestedIncomplete = getParameterByName("showIncomplete", searchString);
    requestedIncomplete = (requestedIncomplete == "true" ? true : false);

    var endPoint = createRESTEndPoint();

    $.getJSON(endPoint, function(response,status,jqXHR) {
      var summary=[];
      var allExecCnt = 0;
      var allRDDBlocks = 0;
      var allMemoryUsed = 0;
      var allMaxMemory = 0;
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

      var activeExecCnt = 0;
      var activeRDDBlocks = 0;
      var activeMemoryUsed = 0;
      var activeMaxMemory = 0;
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

      var deadExecCnt = 0;
      var deadRDDBlocks = 0;
      var deadMemoryUsed = 0;
      var deadMaxMemory = 0;
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

        response.forEach(function (exec) {
          allExecCnt +=1;
          allRDDBlocks += exec.rddBlocks;
          allMemoryUsed += exec.memoryUsed;
          allMaxMemory += exec.maxMemory;
          allDiskUsed += exec.diskUsed;
          allTotalCores  += exec.totalCores;
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
          if(exec.isActive) {
              activeExecCnt +=1;
              activeRDDBlocks += exec.rddBlocks;
              activeMemoryUsed += exec.memoryUsed;
              activeMaxMemory += exec.maxMemory;
              activeDiskUsed += exec.diskUsed;
              activeTotalCores  += exec.totalCores;
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
          } else {
              deadExecCnt +=1;
              deadRDDBlocks += exec.rddBlocks;
              deadMemoryUsed += exec.memoryUsed;
              deadMaxMemory += exec.maxMemory;
              deadDiskUsed += exec.diskUsed;
              deadTotalCores  += exec.totalCores;
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
          }
      });

      var totalSummary = { "execCnt" : ( "Total(" + allExecCnt + ")"), "allRDDBlocks" : allRDDBlocks, "allMemoryUsed" : allMemoryUsed,
          "allMaxMemory" : allMaxMemory, "allDiskUsed" : allDiskUsed, "allTotalCores" : allTotalCores, "allMaxTasks" : allMaxTasks,
          "allActiveTasks" : allActiveTasks, "allFailedTasks" : allFailedTasks, "allCompletedTasks" :
          allCompletedTasks, "allTotalTasks" : allTotalTasks, "allTotalDuration" : allTotalDuration,
          "allTotalGCTime" : allTotalGCTime, "allTotalInputBytes" : allTotalInputBytes,
          "allTotalShuffleRead" : allTotalShuffleRead, "allTotalShuffleWrite" : allTotalShuffleWrite };
      var activeSummary = { "execCnt" : ( "Active(" + activeExecCnt + ")"), "allRDDBlocks" : activeRDDBlocks, "allMemoryUsed" : activeMemoryUsed,
            "allMaxMemory" : activeMaxMemory, "allDiskUsed" : activeDiskUsed, "allTotalCores" : activeTotalCores, "allMaxTasks" : activeMaxTasks,
            "allActiveTasks" : activeActiveTasks, "allFailedTasks" : activeFailedTasks, "allCompletedTasks" :
          activeCompletedTasks, "allTotalTasks" : activeTotalTasks, "allTotalDuration" : activeTotalDuration,
            "allTotalGCTime" : activeTotalGCTime, "allTotalInputBytes" : activeTotalInputBytes,
            "allTotalShuffleRead" : activeTotalShuffleRead, "allTotalShuffleWrite" : activeTotalShuffleWrite };
      var deadSummary = { "execCnt" : ( "Dead(" + deadExecCnt + ")" ), "allRDDBlocks" : deadRDDBlocks, "allMemoryUsed" : deadMemoryUsed,
            "allMaxMemory" : deadMaxMemory, "allDiskUsed" : deadDiskUsed, "allTotalCores" : deadTotalCores, "allMaxTasks" : deadMaxTasks,
            "allActiveTasks" : deadActiveTasks, "allFailedTasks" : deadFailedTasks, "allCompletedTasks" :
            deadCompletedTasks, "allTotalTasks" : deadTotalTasks, "allTotalDuration" : deadTotalDuration,
            "allTotalGCTime" : deadTotalGCTime, "allTotalInputBytes" : deadTotalInputBytes,
            "allTotalShuffleRead" : deadTotalShuffleRead, "allTotalShuffleWrite" : deadTotalShuffleWrite };

      var data = {executors: response, "execSummary": [activeSummary, deadSummary, totalSummary]};
      $.get(createBaseURI() + "/static/executorspage-template.html", function(template) {

        executorsSummary.append(Mustache.render($(template).filter("#executors-summary-template").html(),data));
        var selector = "#active-executors-table";
        var conf = {
                    "data": response,
                    "columns": [
                        //{data: 'id'},
                        {data: function(row, type) { return type !== 'display' ? (isNaN(row.id) ? 0 : row.id ) : row.id;} },
                        {data: 'hostPort'},
                        {data: 'isActive', render: formatStatus },
                        {data: 'rddBlocks'},
                        {data: function(row, type) { return type === 'display' ? (formatBytes(row.memoryUsed, type) + ' / ' + formatBytes(row.maxMemory, type)) : row.memoryUsed; } },
                        {data: 'diskUsed', render: formatBytes },
                        {data: 'totalCores'},
                        {data: 'activeTasks',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if ( sData >  0 ) {
                                    $(nTd).css('color', 'white');
                                    $(nTd).css('background', activeTasksStyle(oData.activeTasks, oData.maxTasks));
                                } } },
                        {data: 'failedTasks',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if ( sData > 0 ) {
                                    $(nTd).css('color', 'white');
                                    $(nTd).css('background', failedTasksStyle(oData.failedTasks, oData.totalTasks));
                                } } },
                        {data: 'completedTasks'},
                        {data: 'totalTasks'},
                        {data: function(row, type) { return type === 'display' ? (formatDuration(row.totalDuration, type) + ' (' + formatDuration(row.totalGCTime, type) + ')') : row.totalDuration },
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if ( oData.totalDuration > 0 ) {
                                    $(nTd).css('color', totalDurationColor(oData.totalGCTime, oData.totalDuration));
                                    $(nTd).css('background', totalDurationStyle(oData.totalGCTime, oData.totalDuration));
                                } } },
                        {data: 'totalInputBytes', render: formatBytes },
                        {data: 'totalShuffleRead', render: formatBytes },
                        {data: 'totalShuffleWrite', render: formatBytes },
                        {data: 'executorLogs', render: formatLogsCells },
                        {data: 'id', render: function(data, type) {return type === 'display' ? ("<a href='threadDump/?executorId=" + data + "'>Thread Dump</a>" ) : data; }}
                    ],
                    "order": [[ 0, "asc" ]]
        };

        $(selector).DataTable(conf);
        $('#active-executors [data-toggle="tooltip"]').tooltip();

        var sumSelector  = "#summary-execs-table";
          var sumConf = {
              "data": [activeSummary, deadSummary, totalSummary],
              "columns": [
                  {data: 'execCnt',
                      "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                          $(nTd).css('font-weight', 'bold');
                      } },
                  {data: 'allRDDBlocks'},
                  {data: function(row, type) { return type === 'display' ? (formatBytes(row.allMemoryUsed, type) + ' / ' + formatBytes(row.allMaxMemory, type)): row.allMemoryUsed; } },
                  {data: 'allDiskUsed', render: formatBytes},
                  {data: 'allTotalCores'},
                  {data: 'allActiveTasks',
                      "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                          if ( sData >  0 ) {
                              $(nTd).css('color', 'white');
                              $(nTd).css('background', activeTasksStyle(oData.allActiveTasks, oData.allMaxTasks));
                          } } },
                  {data: 'allFailedTasks',
                      "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                          if ( sData > 0 ) {
                              $(nTd).css('color', 'white');
                              $(nTd).css('background', failedTasksStyle(oData.allFailedTasks, oData.allTotalTasks));
                          } } },
                  {data: 'allCompletedTasks'},
                  {data: 'allTotalTasks'},
                  {data: function(row, type) { return type === 'display' ? (formatDuration(row.allTotalDuration, type) + ' (' + formatDuration(row.allTotalGCTime, type) + ')') : row.allTotalDuration },
                      "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                          if ( oData.allTotalDuration > 0 ) {
                              $(nTd).css('color', totalDurationColor(oData.allTotalGCTime, oData.allTotalDuration));
                              $(nTd).css('background', totalDurationStyle(oData.allTotalGCTime, oData.allTotalDuration));
                          } } },
                  {data: 'allTotalInputBytes', render: formatBytes },
                  {data: 'allTotalShuffleRead', render: formatBytes },
                  {data: 'allTotalShuffleWrite', render: formatBytes }
              ],
              "paging": false,
              "searching": false,
              "info": false

          };

        $(sumSelector).DataTable(sumConf);
        $('#execSummary [data-toggle="tooltip"]').tooltip();

      });
    });
});
