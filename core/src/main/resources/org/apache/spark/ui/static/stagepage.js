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

var shouldBlockUI = true;

$(document).ajaxStop(function () {
    if (shouldBlockUI) {
        $.unblockUI();
        shouldBlockUI = false;
    }
});

$(document).ajaxStart(function () {
    if (shouldBlockUI) {
        $.blockUI({message: '<h3>Loading Stage Page...</h3>'});
    }
});

$.extend( $.fn.dataTable.ext.type.order, {
    "duration-pre": ConvertDurationString,

    "duration-asc": function ( a, b ) {
        a = ConvertDurationString( a );
        b = ConvertDurationString( b );
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "duration-desc": function ( a, b ) {
        a = ConvertDurationString( a );
        b = ConvertDurationString( b );
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

// This function will only parse the URL under certain format
// e.g. (history) https://domain:50509/history/application_1536254569791_3806251/1/stages/stage/?id=4&attempt=1
// e.g. (proxy) https://domain:50505/proxy/application_1502220952225_59143/stages/stage?id=4&attempt=1
function stageEndPoint(appId) {
    var queryString = document.baseURI.split('?');
    var words = document.baseURI.split('/');
    var indexOfProxy = words.indexOf("proxy");
    var stageId = queryString[1].split("&").filter(word => word.includes("id="))[0].split("=")[1];
    if (indexOfProxy > 0) {
        var appId = words[indexOfProxy + 1];
        var newBaseURI = words.slice(0, words.indexOf("proxy") + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
    }
    var indexOfHistory = words.indexOf("history");
    if (indexOfHistory > 0) {
        var appId = words[indexOfHistory + 1];
        var appAttemptId = words[indexOfHistory + 2];
        var newBaseURI = words.slice(0, words.indexOf("history")).join('/');
        if (isNaN(appAttemptId) || appAttemptId == "0") {
            return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + appAttemptId + "/stages/" + stageId;
        }
    }
    return location.origin + "/api/v1/applications/" + appId + "/stages/" + stageId;
}

function getColumnNameForTaskMetricSummary(columnKey) {
    switch(columnKey) {
        case "executorRunTime":
            return "Duration";

        case "jvmGcTime":
            return "GC Time";

        case "gettingResultTime":
            return "Getting Result Time";

        case "inputMetrics":
            return "Input Size / Records";

        case "outputMetrics":
            return "Output Size / Records";

        case "peakExecutionMemory":
            return "Peak Execution Memory";

        case "resultSerializationTime":
            return "Result Serialization Time";

        case "schedulerDelay":
            return "Scheduler Delay";

        case "diskBytesSpilled":
            return "Spill (disk)";

        case "memoryBytesSpilled":
            return "Spill (memory)";

        case "shuffleReadMetrics":
            return "Shuffle Read Size / Records";

        case "shuffleWriteMetrics":
            return "Shuffle Write Size / Records";

        case "executorDeserializeTime":
            return "Task Deserialization Time";

        case "shuffleReadBlockedTime":
            return "Shuffle Read Blocked Time";

        case "shuffleRemoteReads":
            return "Shuffle Remote Reads";

        default:
            return "NA";
    }
}

function displayRowsForSummaryMetricsTable(row, type, columnIndex) {
    switch(row.columnKey) {
        case 'inputMetrics':
            var str = formatBytes(row.data.bytesRead[columnIndex], type) + " / " +
              row.data.recordsRead[columnIndex];
            return str;
            break;

        case 'outputMetrics':
            var str = formatBytes(row.data.bytesWritten[columnIndex], type) + " / " +
              row.data.recordsWritten[columnIndex];
            return str;
            break;

        case 'shuffleReadMetrics':
            var str = formatBytes(row.data.readBytes[columnIndex], type) + " / " +
              row.data.readRecords[columnIndex];
            return str;
            break;

        case 'shuffleReadBlockedTime':
            var str = formatDuration(row.data.fetchWaitTime[columnIndex]);
            return str;
            break;

        case 'shuffleRemoteReads':
            var str = formatBytes(row.data.remoteBytesRead[columnIndex], type);
            return str;
            break;

        case 'shuffleWriteMetrics':
            var str = formatBytes(row.data.writeBytes[columnIndex], type) + " / " +
              row.data.writeRecords[columnIndex];
            return str;
            break;

        default:
            return (row.columnKey == 'peakExecutionMemory' || row.columnKey == 'memoryBytesSpilled'
                    || row.columnKey == 'diskBytesSpilled') ? formatBytes(
                    row.data[columnIndex], type) : (formatDuration(row.data[columnIndex]));

    }
}

function createDataTableForTaskSummaryMetricsTable(taskSummaryMetricsTable) {
    var taskMetricsTable = "#summary-metrics-table";
    if ($.fn.dataTable.isDataTable(taskMetricsTable)) {
        taskSummaryMetricsDataTable.clear().draw();
        taskSummaryMetricsDataTable.rows.add(taskSummaryMetricsTable).draw();
    } else {
        var taskConf = {
            "data": taskSummaryMetricsTable,
            "columns": [
                {data : 'metric'},
                // Min
                {
                    data: function (row, type) {
                        return displayRowsForSummaryMetricsTable(row, type, 0);
                    }
                },
                // 25th percentile
                {
                    data: function (row, type) {
                        return displayRowsForSummaryMetricsTable(row, type, 1);
                    }
                },
                // Median
                {
                    data: function (row, type) {
                        return displayRowsForSummaryMetricsTable(row, type, 2);
                    }
                },
                // 75th percentile
                {
                    data: function (row, type) {
                        return displayRowsForSummaryMetricsTable(row, type, 3);
                    }
                },
                // Max
                {
                    data: function (row, type) {
                        return displayRowsForSummaryMetricsTable(row, type, 4);
                    }
                }
            ],
            "columnDefs": [
                { "type": "duration", "targets": 1 },
                { "type": "duration", "targets": 2 },
                { "type": "duration", "targets": 3 },
                { "type": "duration", "targets": 4 },
                { "type": "duration", "targets": 5 }
            ],
            "paging": false,
            "searching": false,
            "order": [[0, "asc"]],
            "bSort": false,
            "bAutoWidth": false,
            "oLanguage": {
                "sEmptyTable": "No tasks have reported metrics yet"
            }
        };
        taskSummaryMetricsDataTable = $(taskMetricsTable).DataTable(taskConf);
    }
    taskSummaryMetricsTableCurrentStateArray = taskSummaryMetricsTable.slice();
}

function createRowMetadataForColumn(colKey, data, checkboxId) {
  var row = {
      "metric": getColumnNameForTaskMetricSummary(colKey),
      "data": data,
      "checkboxId": checkboxId,
      "columnKey": colKey
  };
  return row;
}

function reselectCheckboxesBasedOnTaskTableState() {
    var allChecked = true;
    var taskSummaryMetricsTableCurrentFilteredArray = taskSummaryMetricsTableCurrentStateArray.slice();
    if (typeof taskTableSelector !== 'undefined' && taskSummaryMetricsTableCurrentStateArray.length > 0) {
        for (var k = 0; k < optionalColumns.length; k++) {
            if (taskTableSelector.column(optionalColumns[k]).visible()) {
                $("#box-"+optionalColumns[k]).prop('checked', true);
                taskSummaryMetricsTableCurrentStateArray.push(taskSummaryMetricsTableArray.filter(row => (row.checkboxId).toString() == optionalColumns[k])[0]);
                taskSummaryMetricsTableCurrentFilteredArray = taskSummaryMetricsTableCurrentStateArray.slice();
            } else {
                allChecked = false;
            }
        }
        if (allChecked) {
            $("#box-0").prop('checked', true);
        }
        createDataTableForTaskSummaryMetricsTable(taskSummaryMetricsTableCurrentFilteredArray);
    }
}

function getStageAttemptId() {
  var words = document.baseURI.split('?');
  var digitsRegex = /[0-9]+/;
  // We are using regex here to extract the stage attempt id as there might be certain url's with format
  // like /proxy/application_1539986433979_27115/stages/stage/?id=0&attempt=0#tasksTitle
  var stgAttemptId = words[1].split("&").filter(
      word => word.includes("attempt="))[0].split("=")[1].match(digitsRegex);
  return stgAttemptId;
}

var taskSummaryMetricsTableArray = [];
var taskSummaryMetricsTableCurrentStateArray = [];
var taskSummaryMetricsDataTable;
var optionalColumns = [11, 12, 13, 14, 15, 16, 17];
var taskTableSelector;

$(document).ready(function () {
    setDataTableDefaults();

    $("#showAdditionalMetrics").append(
        "<div><a id='additionalMetrics'>" +
        "<span class='expand-input-rate-arrow arrow-closed' id='arrowtoggle1'></span>" +
        " Show Additional Metrics" +
        "</a></div>" +
        "<div class='container-fluid container-fluid-div' id='toggle-metrics' hidden>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-0' data-column='0'> Select All</div>" +
        "<div id='scheduler_delay' class='scheduler-delay-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-11' data-column='11'> Scheduler Delay</div>" +
        "<div id='task_deserialization_time' class='task-deserialization-time-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-12' data-column='12'> Task Deserialization Time</div>" +
        "<div id='shuffle_read_blocked_time' class='shuffle-read-blocked-time-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-13' data-column='13'> Shuffle Read Blocked Time</div>" +
        "<div id='shuffle_remote_reads' class='shuffle-remote-reads-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-14' data-column='14'> Shuffle Remote Reads</div>" +
        "<div id='result_serialization_time' class='result-serialization-time-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-15' data-column='15'> Result Serialization Time</div>" +
        "<div id='getting_result_time' class='getting-result-time-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-16' data-column='16'> Getting Result Time</div>" +
        "<div id='peak_execution_memory' class='peak-execution-memory-checkbox-div'><input type='checkbox' class='toggle-vis' id='box-17' data-column='17'> Peak Execution Memory</div>" +
        "</div>");

    $('#scheduler_delay').attr("data-toggle", "tooltip")
        .attr("data-placement", "right")
        .attr("title", "Scheduler delay includes time to ship the task from the scheduler to the executor, and time to send " +
            "the task result from the executor to the scheduler. If scheduler delay is large, consider decreasing the size of tasks or decreasing the size of task results.");
    $('#task_deserialization_time').attr("data-toggle", "tooltip")
        .attr("data-placement", "right")
        .attr("title", "Time spent deserializing the task closure on the executor, including the time to read the broadcasted task.");
    $('#shuffle_read_blocked_time').attr("data-toggle", "tooltip")
        .attr("data-placement", "right")
        .attr("title", "Time that the task spent blocked waiting for shuffle data to be read from remote machines.");
    $('#shuffle_remote_reads').attr("data-toggle", "tooltip")
        .attr("data-placement", "right")
        .attr("title", "Total shuffle bytes read from remote executors. This is a subset of the shuffle read bytes; the remaining shuffle data is read locally. ");
    $('#result_serialization_time').attr("data-toggle", "tooltip")
            .attr("data-placement", "right")
            .attr("title", "Time spent serializing the task result on the executor before sending it back to the driver.");
    $('#getting_result_time').attr("data-toggle", "tooltip")
            .attr("data-placement", "right")
            .attr("title", "Time that the driver spends fetching task results from workers. If this is large, consider decreasing the amount of data returned from each task.");
    $('#peak_execution_memory').attr("data-toggle", "tooltip")
            .attr("data-placement", "right")
            .attr("title", "Execution memory refers to the memory used by internal data structures created during " +
                "shuffles, aggregations and joins when Tungsten is enabled. The value of this accumulator " +
                "should be approximately the sum of the peak sizes across all such data structures created " +
                "in this task. For SQL jobs, this only tracks all unsafe operators, broadcast joins, and " +
                "external sort.");
    $('[data-toggle="tooltip"]').tooltip();
    tasksSummary = $("#parent-container");
    getStandAloneAppId(function (appId) {

        var endPoint = stageEndPoint(appId);
        var stageAttemptId = getStageAttemptId();
        $.getJSON(endPoint + "/" + stageAttemptId, function(response, status, jqXHR) {

            var responseBody = response;
            var dataToShow = {};
            dataToShow.showInputData = responseBody.inputBytes > 0;
            dataToShow.showOutputData = responseBody.outputBytes > 0;
            dataToShow.showShuffleReadData = responseBody.shuffleReadBytes > 0;
            dataToShow.showShuffleWriteData = responseBody.shuffleWriteBytes > 0;
            dataToShow.showBytesSpilledData =
                (responseBody.diskBytesSpilled > 0 || responseBody.memoryBytesSpilled > 0);

            if (!dataToShow.showShuffleReadData) {
                $('#shuffle_read_blocked_time').remove();
                $('#shuffle_remote_reads').remove();
                optionalColumns.splice(2, 2);
            }

            // prepare data for executor summary table
            stageExecutorSummaryInfoKeys = Object.keys(responseBody.executorSummary);
            $.getJSON(createRESTEndPointForExecutorsPage(appId),
              function(executorSummaryResponse, status, jqXHR) {
                var executorDetailsMap = {};
                executorSummaryResponse.forEach(function (executorDetail) {
                    executorDetailsMap[executorDetail.id] = executorDetail;
                });

                var executorSummaryTable = [];
                stageExecutorSummaryInfoKeys.forEach(function (columnKeyIndex) {
                    var executorSummary = responseBody.executorSummary[columnKeyIndex];
                    var executorDetail = executorDetailsMap[columnKeyIndex.toString()];
                    executorSummary.id = columnKeyIndex;
                    executorSummary.executorLogs = {};
                    executorSummary.hostPort = "CANNOT FIND ADDRESS";

                    if (executorDetail) {
                        if (executorDetail["executorLogs"]) {
                            responseBody.executorSummary[columnKeyIndex].executorLogs =
                                executorDetail["executorLogs"];
                            }
                        if (executorDetail["hostPort"]) {
                            responseBody.executorSummary[columnKeyIndex].hostPort =
                                executorDetail["hostPort"];
                        }
                    }
                    executorSummaryTable.push(responseBody.executorSummary[columnKeyIndex]);
                });
                // building task aggregated metrics by executor table
                var executorSummaryConf = {
                    "data": executorSummaryTable,
                    "columns": [
                        {data : "id"},
                        {data : "executorLogs", render: formatLogsCells},
                        {data : "hostPort"},
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskTime) : row.taskTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                var totaltasks = row.succeededTasks + row.failedTasks + row.killedTasks;
                                return type === 'display' ? totaltasks : totaltasks.toString();
                            }
                        },
                        {data : "failedTasks"},
                        {data : "killedTasks"},
                        {data : "succeededTasks"},
                        {data : "isBlacklistedForStage"},
                        {
                            data : function (row, type) {
                                return row.inputRecords != 0 ? formatBytes(row.inputBytes, type) + " / " + row.inputRecords : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.outputRecords != 0 ? formatBytes(row.outputBytes, type) + " / " + row.outputRecords : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleReadRecords != 0 ? formatBytes(row.shuffleRead, type) + " / " + row.shuffleReadRecords : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleWriteRecords != 0 ? formatBytes(row.shuffleWrite, type) + " / " + row.shuffleWriteRecords : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return typeof row.memoryBytesSpilled != 'undefined' ? formatBytes(row.memoryBytesSpilled, type) : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return typeof row.diskBytesSpilled != 'undefined' ? formatBytes(row.diskBytesSpilled, type) : "";
                            }
                        }
                    ],
                    "order": [[0, "asc"]],
                    "bAutoWidth": false,
                    "oLanguage": {
                        "sEmptyTable": "No data to show yet"
                    }
                };
                var executorSummaryTableSelector =
                    $("#summary-executor-table").DataTable(executorSummaryConf);
                $('#parent-container [data-toggle="tooltip"]').tooltip();

                executorSummaryTableSelector.column(9).visible(dataToShow.showInputData);
                if (dataToShow.showInputData) {
                    $('#executor-summary-input').attr("data-toggle", "tooltip")
                        .attr("data-placement", "top")
                        .attr("title", "Bytes and records read from Hadoop or from Spark storage.");
                    $('#executor-summary-input').tooltip(true);
                }
                executorSummaryTableSelector.column(10).visible(dataToShow.showOutputData);
                if (dataToShow.showOutputData) {
                    $('#executor-summary-output').attr("data-toggle", "tooltip")
                        .attr("data-placement", "top")
                        .attr("title", "Bytes and records written to Hadoop.");
                    $('#executor-summary-output').tooltip(true);
                }
                executorSummaryTableSelector.column(11).visible(dataToShow.showShuffleReadData);
                if (dataToShow.showShuffleReadData) {
                    $('#executor-summary-shuffle-read').attr("data-toggle", "tooltip")
                        .attr("data-placement", "top")
                        .attr("title", "Total shuffle bytes and records read (includes both data read locally and data read from remote executors).");
                    $('#executor-summary-shuffle-read').tooltip(true);
                }
                executorSummaryTableSelector.column(12).visible(dataToShow.showShuffleWriteData);
                if (dataToShow.showShuffleWriteData) {
                    $('#executor-summary-shuffle-write').attr("data-toggle", "tooltip")
                        .attr("data-placement", "top")
                        .attr("title", "Bytes and records written to disk in order to be read by a shuffle in a future stage.");
                    $('#executor-summary-shuffle-write').tooltip(true);
                }
                executorSummaryTableSelector.column(13).visible(dataToShow.showBytesSpilledData);
                executorSummaryTableSelector.column(14).visible(dataToShow.showBytesSpilledData);
            });

            // prepare data for accumulatorUpdates
            var accumulatorTable = responseBody.accumulatorUpdates.filter(accumUpdate =>
                !(accumUpdate.name).toString().includes("internal."));

            // rendering the UI page
            var data = {"executors": response};
            $.get(createTemplateURI(appId, "stagespage"), function(template) {
                tasksSummary.append(Mustache.render($(template).filter("#stages-summary-template").html(), data));

                $("#additionalMetrics").click(function(){
                    $("#arrowtoggle1").toggleClass("arrow-open arrow-closed");
                    $("#toggle-metrics").toggle();
                    if (window.localStorage) {
                        window.localStorage.setItem("arrowtoggle1class", $("#arrowtoggle1").attr('class'));
                    }
                });

                $("#aggregatedMetrics").click(function(){
                    $("#arrowtoggle2").toggleClass("arrow-open arrow-closed");
                    $("#toggle-aggregatedMetrics").toggle();
                    if (window.localStorage) {
                        window.localStorage.setItem("arrowtoggle2class", $("#arrowtoggle2").attr('class'));
                    }
                });

                var quantiles = "0,0.25,0.5,0.75,1.0";
                $.getJSON(endPoint + "/" + stageAttemptId + "/taskSummary?quantiles=" + quantiles,
                  function(taskMetricsResponse, status, jqXHR) {
                    var taskMetricKeys = Object.keys(taskMetricsResponse);
                    taskMetricKeys.forEach(function (columnKey) {
                        switch(columnKey) {
                            case "shuffleReadMetrics":
                                var row1 = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 3);
                                var row2 = createRowMetadataForColumn(
                                    "shuffleReadBlockedTime", taskMetricsResponse[columnKey], 13);
                                var row3 = createRowMetadataForColumn(
                                    "shuffleRemoteReads", taskMetricsResponse[columnKey], 14);
                                if (dataToShow.showShuffleReadData) {
                                    taskSummaryMetricsTableArray.push(row1);
                                    taskSummaryMetricsTableArray.push(row2);
                                    taskSummaryMetricsTableArray.push(row3);
                                }
                                break;

                            case "schedulerDelay":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 11);
                                taskSummaryMetricsTableArray.push(row);
                                break;

                            case "executorDeserializeTime":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 12);
                                taskSummaryMetricsTableArray.push(row);
                                break;

                            case "resultSerializationTime":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 15);
                                taskSummaryMetricsTableArray.push(row);
                                break;

                            case "gettingResultTime":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 16);
                                taskSummaryMetricsTableArray.push(row);
                                break;

                            case "peakExecutionMemory":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 17);
                                taskSummaryMetricsTableArray.push(row);
                                break;

                            case "inputMetrics":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 1);
                                if (dataToShow.showInputData) {
                                    taskSummaryMetricsTableArray.push(row);
                                }
                                break;

                            case "outputMetrics":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 2);
                                if (dataToShow.showOutputData) {
                                    taskSummaryMetricsTableArray.push(row);
                                }
                                break;

                            case "shuffleWriteMetrics":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 4);
                                if (dataToShow.showShuffleWriteData) {
                                    taskSummaryMetricsTableArray.push(row);
                                }
                                break;

                            case "diskBytesSpilled":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 5);
                                if (dataToShow.showBytesSpilledData) {
                                    taskSummaryMetricsTableArray.push(row);
                                }
                                break;

                            case "memoryBytesSpilled":
                                var row = createRowMetadataForColumn(
                                    columnKey, taskMetricsResponse[columnKey], 6);
                                if (dataToShow.showBytesSpilledData) {
                                    taskSummaryMetricsTableArray.push(row);
                                }
                                break;

                            default:
                                if (getColumnNameForTaskMetricSummary(columnKey) != "NA") {
                                    var row = createRowMetadataForColumn(
                                        columnKey, taskMetricsResponse[columnKey], 0);
                                    taskSummaryMetricsTableArray.push(row);
                                }
                                break;
                        }
                    });
                    var taskSummaryMetricsTableFilteredArray =
                        taskSummaryMetricsTableArray.filter(row => row.checkboxId < 11);
                    taskSummaryMetricsTableCurrentStateArray = taskSummaryMetricsTableFilteredArray.slice();
                    reselectCheckboxesBasedOnTaskTableState();
                });

                // building accumulator update table
                var accumulatorConf = {
                    "data": accumulatorTable,
                    "columns": [
                        {data : "id"},
                        {data : "name"},
                        {data : "value"}
                    ],
                    "paging": false,
                    "searching": false,
                    "order": [[0, "asc"]],
                    "bAutoWidth": false
                };
                $("#accumulator-table").DataTable(accumulatorConf);

                // building tasks table that uses server side functionality
                var totalTasksToShow = responseBody.numCompleteTasks + responseBody.numActiveTasks +
                    responseBody.numKilledTasks + responseBody.numFailedTasks;
                var taskTable = "#active-tasks-table";
                var taskConf = {
                    "serverSide": true,
                    "paging": true,
                    "info": true,
                    "processing": true,
                    "lengthMenu": [[20, 40, 60, 100, totalTasksToShow], [20, 40, 60, 100, "All"]],
                    "orderMulti": false,
                    "bAutoWidth": false,
                    "ajax": {
                        "url": endPoint + "/" + stageAttemptId + "/taskTable",
                        "data": function (data) {
                            var columnIndexToSort = 0;
                            var columnNameToSort = "Index";
                            if (data.order[0].column && data.order[0].column != "") {
                                columnIndexToSort = parseInt(data.order[0].column);
                                columnNameToSort = data.columns[columnIndexToSort].name;
                            }
                            delete data.columns;
                            data.numTasks = totalTasksToShow;
                            data.columnIndexToSort = columnIndexToSort;
                            data.columnNameToSort = columnNameToSort;
                        },
                        "dataSrc": function (jsons) {
                            var jsonStr = JSON.stringify(jsons);
                            var tasksToShow = JSON.parse(jsonStr);
                            return tasksToShow.aaData;
                        },
                        "error": function (jqXHR, textStatus, errorThrown) {
                            alert("Unable to connect to the server. Looks like the Spark " +
                              "application must have ended. Please Switch to the history UI.");
                            $("#active-tasks-table_processing").css("display","none");
                        }
                    },
                    "columns": [
                        {data: function (row, type) {
                            return type !== 'display' ? (isNaN(row.index) ? 0 : row.index ) : row.index;
                            },
                            name: "Index"
                        },
                        {data : "taskId", name: "ID"},
                        {data : "attempt", name: "Attempt"},
                        {data : "status", name: "Status"},
                        {data : "taskLocality", name: "Locality Level"},
                        {data : "executorId", name: "Executor ID"},
                        {data : "host", name: "Host"},
                        {data : "executorLogs", name: "Logs", render: formatLogsCells},
                        {data : "launchTime", name: "Launch Time", render: formatDate},
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.executorRunTime) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.executorRunTime) : row.taskMetrics.executorRunTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "Duration"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.jvmGcTime) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.jvmGcTime) : row.taskMetrics.jvmGcTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "GC Time"
                        },
                        {
                            data : function (row, type) {
                                if (row.schedulerDelay) {
                                    return type === 'display' ? formatDuration(row.schedulerDelay) : row.schedulerDelay;
                                } else {
                                    return "";
                                }
                            },
                            name: "Scheduler Delay"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.executorDeserializeTime) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.executorDeserializeTime) : row.taskMetrics.executorDeserializeTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "Task Deserialization Time"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.shuffleReadMetrics) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.shuffleReadMetrics.fetchWaitTime) : row.taskMetrics.shuffleReadMetrics.fetchWaitTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Read Blocked Time"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.shuffleReadMetrics) {
                                    return type === 'display' ? formatBytes(row.taskMetrics.shuffleReadMetrics.remoteBytesRead, type) : row.taskMetrics.shuffleReadMetrics.remoteBytesRead;
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Remote Reads"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.resultSerializationTime) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.resultSerializationTime) : row.taskMetrics.resultSerializationTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "Result Serialization Time"
                        },
                        {
                            data : function (row, type) {
                                if (row.gettingResultTime) {
                                    return type === 'display' ? formatDuration(row.gettingResultTime) : row.gettingResultTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "Getting Result Time"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.peakExecutionMemory) {
                                    return type === 'display' ? formatBytes(row.taskMetrics.peakExecutionMemory, type) : row.taskMetrics.peakExecutionMemory;
                                } else {
                                    return "";
                                }
                            },
                            name: "Peak Execution Memory"
                        },
                        {
                            data : function (row, type) {
                                if (accumulatorTable.length > 0 && row.accumulatorUpdates.length > 0) {
                                    var accIndex = row.accumulatorUpdates.length - 1;
                                    return row.accumulatorUpdates[accIndex].name + ' : ' + row.accumulatorUpdates[accIndex].update;
                                } else {
                                    return "";
                                }
                            },
                            name: "Accumulators"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.inputMetrics && row.taskMetrics.inputMetrics.bytesRead > 0) {
                                    if (type === 'display') {
                                        return formatBytes(row.taskMetrics.inputMetrics.bytesRead, type) + " / " + row.taskMetrics.inputMetrics.recordsRead;
                                    } else {
                                        return row.taskMetrics.inputMetrics.bytesRead + " / " + row.taskMetrics.inputMetrics.recordsRead;
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Input Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.outputMetrics && row.taskMetrics.outputMetrics.bytesWritten > 0) {
                                    if (type === 'display') {
                                        return formatBytes(row.taskMetrics.outputMetrics.bytesWritten, type) + " / " + row.taskMetrics.outputMetrics.recordsWritten;
                                    } else {
                                        return row.taskMetrics.outputMetrics.bytesWritten + " / " + row.taskMetrics.outputMetrics.recordsWritten;
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Output Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.shuffleWriteMetrics && row.taskMetrics.shuffleWriteMetrics.writeTime > 0) {
                                    return type === 'display' ? formatDuration(parseInt(row.taskMetrics.shuffleWriteMetrics.writeTime) / 1000000) : row.taskMetrics.shuffleWriteMetrics.writeTime;
                                } else {
                                    return "";
                                }
                            },
                            name: "Write Time"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.shuffleWriteMetrics && row.taskMetrics.shuffleWriteMetrics.bytesWritten > 0) {
                                    if (type === 'display') {
                                        return formatBytes(row.taskMetrics.shuffleWriteMetrics.bytesWritten, type) + " / " + row.taskMetrics.shuffleWriteMetrics.recordsWritten;
                                    } else {
                                        return row.taskMetrics.shuffleWriteMetrics.bytesWritten + " / " + row.taskMetrics.shuffleWriteMetrics.recordsWritten;
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Write Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.shuffleReadMetrics && row.taskMetrics.shuffleReadMetrics.localBytesRead > 0) {
                                    var totalBytesRead = parseInt(row.taskMetrics.shuffleReadMetrics.localBytesRead) + parseInt(row.taskMetrics.shuffleReadMetrics.remoteBytesRead);
                                    if (type === 'display') {
                                        return formatBytes(totalBytesRead, type) + " / " + row.taskMetrics.shuffleReadMetrics.recordsRead;
                                    } else {
                                        return totalBytesRead + " / " + row.taskMetrics.shuffleReadMetrics.recordsRead;
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Read Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.memoryBytesSpilled && row.taskMetrics.memoryBytesSpilled > 0) {
                                    return type === 'display' ? formatBytes(row.taskMetrics.memoryBytesSpilled, type) : row.taskMetrics.memoryBytesSpilled;
                                } else {
                                    return "";
                                }
                            },
                            name: "Spill (Memory)"
                        },
                        {
                            data : function (row, type) {
                                if (row.taskMetrics && row.taskMetrics.diskBytesSpilled && row.taskMetrics.diskBytesSpilled > 0) {
                                    return type === 'display' ? formatBytes(row.taskMetrics.diskBytesSpilled, type) : row.taskMetrics.diskBytesSpilled;
                                } else {
                                    return "";
                                }
                            },
                            name: "Spill (Disk)"
                        },
                        {
                            data : function (row, type) {
                                var msg = row.errorMessage;
                                if (typeof msg === 'undefined') {
                                    return "";
                                } else {
                                    var formHead = msg.substring(0, msg.indexOf("at"));
                                    var form = "<span onclick=\"this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')\" class=\"expand-details\">+details</span>";
                                    var formMsg = "<div class=\"stacktrace-details collapsed\"><pre>" + row.errorMessage + "</pre></div>";
                                    return formHead + form + formMsg;
                                }
                            },
                            name: "Errors"
                        }
                    ],
                    "columnDefs": [
                        { "visible": false, "targets": 11 },
                        { "visible": false, "targets": 12 },
                        { "visible": false, "targets": 13 },
                        { "visible": false, "targets": 14 },
                        { "visible": false, "targets": 15 },
                        { "visible": false, "targets": 16 },
                        { "visible": false, "targets": 17 },
                        { "visible": false, "targets": 18 }
                    ],
                };
                taskTableSelector = $(taskTable).DataTable(taskConf);
                $('#active-tasks-table_filter input').unbind();
                var searchEvent;
                $('#active-tasks-table_filter input').bind('keyup', function(e) {
                  if (typeof searchEvent !== 'undefined') {
                    window.clearTimeout(searchEvent);
                  }
                  var value = this.value;
                  searchEvent = window.setTimeout(function(){
                    taskTableSelector.search( value ).draw();}, 500);
                });
                reselectCheckboxesBasedOnTaskTableState();

                // hide or show columns dynamically event
                $('input.toggle-vis').on('click', function(e){
                    // Get the column
                    var para = $(this).attr('data-column');
                    if (para == "0") {
                        var column = taskTableSelector.column(optionalColumns);
                        if ($(this).is(":checked")) {
                            $(".toggle-vis").prop('checked', true);
                            column.visible(true);
                            createDataTableForTaskSummaryMetricsTable(taskSummaryMetricsTableArray);
                        } else {
                            $(".toggle-vis").prop('checked', false);
                            column.visible(false);
                            var taskSummaryMetricsTableFilteredArray =
                                taskSummaryMetricsTableArray.filter(row => row.checkboxId < 11);
                            createDataTableForTaskSummaryMetricsTable(taskSummaryMetricsTableFilteredArray);
                        }
                    } else {
                        var column = taskTableSelector.column(para);
                        // Toggle the visibility
                        column.visible(!column.visible());
                        var taskSummaryMetricsTableFilteredArray = [];
                        if ($(this).is(":checked")) {
                            taskSummaryMetricsTableCurrentStateArray.push(taskSummaryMetricsTableArray.filter(row => (row.checkboxId).toString() == para)[0]);
                            taskSummaryMetricsTableFilteredArray = taskSummaryMetricsTableCurrentStateArray.slice();
                        } else {
                            taskSummaryMetricsTableFilteredArray =
                                taskSummaryMetricsTableCurrentStateArray.filter(row => (row.checkboxId).toString() != para);
                        }
                        createDataTableForTaskSummaryMetricsTable(taskSummaryMetricsTableFilteredArray);
                    }
                });

                // title number and toggle list
                $("#summaryMetricsTitle").html("Summary Metrics for " + "<a href='#tasksTitle'>" + responseBody.numCompleteTasks + " Completed Tasks" + "</a>");
                $("#tasksTitle").html("Tasks (" + totalTasksToShow + ")");

                // hide or show the accumulate update table
                if (accumulatorTable.length == 0) {
                    $("#accumulator-update-table").hide();
                } else {
                    taskTableSelector.column(18).visible(true);
                    $("#accumulator-update-table").show();
                }
                // Showing relevant stage data depending on stage type for task table and executor
                // summary table
                taskTableSelector.column(19).visible(dataToShow.showInputData);
                taskTableSelector.column(20).visible(dataToShow.showOutputData);
                taskTableSelector.column(21).visible(dataToShow.showShuffleWriteData);
                taskTableSelector.column(22).visible(dataToShow.showShuffleWriteData);
                taskTableSelector.column(23).visible(dataToShow.showShuffleReadData);
                taskTableSelector.column(24).visible(dataToShow.showBytesSpilledData);
                taskTableSelector.column(25).visible(dataToShow.showBytesSpilledData);

                if (window.localStorage) {
                    if (window.localStorage.getItem("arrowtoggle1class") !== null &&
                        window.localStorage.getItem("arrowtoggle1class").includes("arrow-open")) {
                        $("#arrowtoggle1").toggleClass("arrow-open arrow-closed");
                        $("#toggle-metrics").toggle();
                    }
                    if (window.localStorage.getItem("arrowtoggle2class") !== null &&
                        window.localStorage.getItem("arrowtoggle2class").includes("arrow-open")) {
                        $("#arrowtoggle2").toggleClass("arrow-open arrow-closed");
                        $("#toggle-aggregatedMetrics").toggle();
                    }
                }
            });
        });
    });
});
