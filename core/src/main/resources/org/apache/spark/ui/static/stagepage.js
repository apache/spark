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

var blockUICount = 0;

$(document).ajaxStop(function () {
    if (blockUICount == 0) {
        $.unblockUI();
        blockUICount++;
    }
});

$(document).ajaxStart(function () {
    if (blockUICount == 0) {
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
    var urlRegex = /https\:\/\/[^\/]+\/([^\/]+)\/([^\/]+)\/([^\/]+)?\/?([^\/]+)?\/?([^\/]+)?\/?([^\/]+)?/gm;
    var urlArray = urlRegex.exec(document.baseURI);
    var ind = urlArray.indexOf("proxy");
    var queryString = document.baseURI.split('?');
    var words = document.baseURI.split('/');
    if (ind > 0) {
        var appId = urlArray[2];
        var stageId = queryString[1].split("&").filter(word => word.includes("id="))[0].split("=")[1];
        var indexOfProxy = words.indexOf("proxy");
        var newBaseURI = words.slice(0, indexOfProxy + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
    }
    ind = urlArray.indexOf("history");
    if (ind > 0) {
        var appId = urlArray[2];
        var appAttemptId = urlArray[ind + 2];
        var stageId = queryString[1].split("&").filter(word => word.includes("id="))[0].split("=")[1];
        var indexOfHistory = words.indexOf("history");
        var newBaseURI = words.slice(0, indexOfHistory).join('/');
        if (isNaN(appAttemptId) || appAttemptId == "0") {
            return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + appAttemptId + "/stages/" + stageId;
        }
    }
    var stageId = queryString[1].split("&").filter(word => word.includes("id="))[0].split("=")[1];
    return location.origin + "/api/v1/applications/" + appId + "/stages/" + stageId;
}

function getColumnNameForTaskMetricSummary(columnKey) {
    switch(columnKey) {
        case "executorRunTime":
            return "Duration";
            break;

        case "jvmGcTime":
            return "GC Time";
            break;

        case "gettingResultTime":
            return "Getting Result Time";
            break;

        case "inputMetrics":
            return "Input Size / Records";
            break;

        case "outputMetrics":
            return "Output Size / Records";
            break;

        case "peakExecutionMemory":
            return "Peak Execution Memory";
            break;

        case "resultSerializationTime":
            return "Result Serialization Time";
            break;

        case "schedulerDelay":
            return "Scheduler Delay";
            break;

        case "diskBytesSpilled":
            return "Shuffle spill (disk)";
            break;

        case "memoryBytesSpilled":
            return "Shuffle spill (memory)";
            break;

        case "shuffleReadMetrics":
            return "Shuffle Read Size / Records";
            break;

        case "shuffleWriteMetrics":
            return "Shuffle Write Size / Records";
            break;

        case "executorDeserializeTime":
            return "Task Deserialization Time";
            break;

        default:
            return "NA";
    }
}

function displayRowsForSummaryMetricsTable(row, type, columnIndex) {
    switch(row.metric) {
        case 'Input Size / Records':
            var str = formatBytes(row.data.bytesRead[columnIndex], type) + " / " +
              row.data.recordsRead[columnIndex];
            return str;
            break;

        case 'Output Size / Records':
            var str = formatBytes(row.data.bytesWritten[columnIndex], type) + " / " +
              row.data.recordsWritten[columnIndex];
            return str;
            break;

        case 'Shuffle Read Size / Records':
            var str = formatBytes(row.data.readBytes[columnIndex], type) + " / " +
              row.data.readRecords[columnIndex];
            return str;
            break;

        case 'Shuffle Read Blocked Time':
            var str = formatDuration(row.data.fetchWaitTime[columnIndex]);
            return str;
            break;

        case 'Shuffle Remote Reads':
            var str = formatBytes(row.data.remoteBytesRead[columnIndex], type);
            return str;
            break;

        case 'Shuffle Write Size / Records':
            var str = formatBytes(row.data.writeBytes[columnIndex], type) + " / " +
              row.data.writeRecords[columnIndex];
            return str;
            break;

        default:
            return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                    || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.data[columnIndex], type) : (formatDuration(row.data[columnIndex]));

    }
}

function createDataTableForTaskSummaryMetricsTable(task_summary_metrics_table) {
    var taskMetricsTable = "#summary-metrics-table";
    if ($.fn.dataTable.isDataTable(taskMetricsTable)) {
        taskSummaryMetricsDataTable.clear().draw();
        taskSummaryMetricsDataTable.rows.add(task_summary_metrics_table).draw();
    } else {
        var task_conf = {
            "data": task_summary_metrics_table,
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
            "order": [[0, "asc"]]
        };
        taskSummaryMetricsDataTable = $(taskMetricsTable).DataTable(task_conf);
    }
    task_summary_metrics_table_current_state_array = task_summary_metrics_table.slice();
}

var task_summary_metrics_table_array = [];
var task_summary_metrics_table_current_state_array = [];
var taskSummaryMetricsDataTable;

$(document).ready(function () {
    setDataTableDefaults();

    $("#showAdditionalMetrics").append(
        "<div><a id='additionalMetrics'>" +
        "<span class='expand-input-rate-arrow arrow-closed' id='arrowtoggle1'></span>" +
        " Show Additional Metrics" +
        "</a></div>" +
        "<div class='container-fluid container-fluid-div' id='toggle-metrics' hidden>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-0' data-column='0'> Select All</div>" +
        "<div id='scheduler_delay'><input type='checkbox' class='toggle-vis' id='box-11' data-column='11'> Scheduler Delay</div>" +
        "<div id='task_deserialization_time'><input type='checkbox' class='toggle-vis' id='box-12' data-column='12'> Task Deserialization Time</div>" +
        "<div id='shuffle_read_blocked_time'><input type='checkbox' class='toggle-vis' id='box-13' data-column='13'> Shuffle Read Blocked Time</div>" +
        "<div id='shuffle_remote_reads'><input type='checkbox' class='toggle-vis' id='box-14' data-column='14'> Shuffle Remote Reads</div>" +
        "<div id='result_serialization_time'><input type='checkbox' class='toggle-vis' id='box-15' data-column='15'> Result Serialization Time</div>" +
        "<div id='getting_result_time'><input type='checkbox' class='toggle-vis' id='box-16' data-column='16'> Getting Result Time</div>" +
        "<div id='peak_execution_memory'><input type='checkbox' class='toggle-vis' id='box-17' data-column='17'> Peak Execution Memory</div>" +
        "</div>");

    $('#scheduler_delay').attr("data-toggle", "tooltip")
        .attr("data-placement", "bottom")
        .attr("title", "Scheduler delay includes time to ship the task from the scheduler to the executor, and time to send " +
            "the task result from the executor to the scheduler. If scheduler delay is large, consider decreasing the size of tasks or decreasing the size of task results.");
    $('#task_deserialization_time').attr("data-toggle", "tooltip")
        .attr("data-placement", "bottom")
        .attr("title", "Time spent deserializing the task closure on the executor, including the time to read the broadcasted task.");
    $('#shuffle_read_blocked_time').attr("data-toggle", "tooltip")
        .attr("data-placement", "bottom")
        .attr("title", "Time that the task spent blocked waiting for shuffle data to be read from remote machines.");
    $('#shuffle_remote_reads').attr("data-toggle", "tooltip")
        .attr("data-placement", "bottom")
        .attr("title", "Total shuffle bytes read from remote executors. This is a subset of the shuffle read bytes; the remaining shuffle data is read locally. ");
    $('#result_serialization_time').attr("data-toggle", "tooltip")
            .attr("data-placement", "bottom")
            .attr("title", "Time spent serializing the task result on the executor before sending it back to the driver.");
    $('#getting_result_time').attr("data-toggle", "tooltip")
            .attr("data-placement", "bottom")
            .attr("title", "Time that the driver spends fetching task results from workers. If this is large, consider decreasing the amount of data returned from each task.");
    $('#peak_execution_memory').attr("data-toggle", "tooltip")
            .attr("data-placement", "bottom")
            .attr("title", "Execution memory refers to the memory used by internal data structures created during " +
                "shuffles, aggregations and joins when Tungsten is enabled. The value of this accumulator " +
                "should be approximately the sum of the peak sizes across all such data structures created " +
                "in this task. For SQL jobs, this only tracks all unsafe operators, broadcast joins, and " +
                "external sort.");
    $('#scheduler_delay').tooltip(true);
    $('#task_deserialization_time').tooltip(true);
    $('#shuffle_read_blocked_time').tooltip(true);
    $('#shuffle_remote_reads').tooltip(true);
    $('#result_serialization_time').tooltip(true);
    $('#getting_result_time').tooltip(true);
    $('#peak_execution_memory').tooltip(true);
    tasksSummary = $("#parent-container");
    getStandAloneAppId(function (appId) {

        var endPoint = stageEndPoint(appId);
        $.getJSON(endPoint, function(response, status, jqXHR) {

            var stageAttemptId = getStageAttemptId();
            var responseBody = response[stageAttemptId];
            var dataToShow = {};
            dataToShow.showInputData = responseBody.inputBytes > 0?true:false;
            dataToShow.showOutputData = responseBody.outputBytes > 0?true:false;
            dataToShow.showShuffleReadData = responseBody.shuffleReadBytes > 0?true:false;
            dataToShow.showShuffleWriteData = responseBody.shuffleWriteBytes > 0?true:false;
            dataToShow.showBytesSpilledData =
                (responseBody.diskBytesSpilled > 0 || responseBody.memoryBytesSpilled > 0)?true:false;

            // prepare data for task aggregated metrics table
            indices = Object.keys(responseBody.executorSummary);
            var executor_summary_table = [];
            indices.forEach(function (ix) {
               responseBody.executorSummary[ix].id = ix;
               executor_summary_table.push(responseBody.executorSummary[ix]);
            });

            // prepare data for accumulatorUpdates
            var accumulator_table = responseBody.accumulatorUpdates.filter(accumUpdate =>
                !(accumUpdate.name).toString().includes("internal."));

            // rendering the UI page
            var data = {"executors": response};
            $.get(createTemplateURI(appId, "stagespage"), function(template) {
                tasksSummary.append(Mustache.render($(template).filter("#stages-summary-template").html(), data));

                $("#additionalMetrics").click(function(){
                    $("#arrowtoggle1").toggleClass("arrow-open arrow-closed");
                    $("#toggle-metrics").toggle();
                });

                $("#aggregatedMetrics").click(function(){
                    $("#arrowtoggle2").toggleClass("arrow-open arrow-closed");
                    $("#toggle-aggregatedMetrics").toggle();
                });

                var quantiles = "0,0.25,0.5,0.75,1.0";
                $.getJSON(stageEndPoint(appId) + "/"+stageAttemptId+"/taskSummary?quantiles="+quantiles,
                  function(taskMetricsResponse, status, jqXHR) {
                    var taskMetricKeys = Object.keys(taskMetricsResponse);
                    taskMetricKeys.forEach(function (ix) {
                        var columnName = getColumnNameForTaskMetricSummary(ix);
                        if (columnName == "Shuffle Read Size / Records") {
                            var row1 = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 3
                            };
                            var row2 = {
                                "metric": "Shuffle Read Blocked Time",
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 13
                            };
                            var row3 = {
                                "metric": "Shuffle Remote Reads",
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 14
                            };
                            if (dataToShow.showShuffleReadData) {
                                task_summary_metrics_table_array.push(row1);
                            }
                            task_summary_metrics_table_array.push(row2);
                            task_summary_metrics_table_array.push(row3);
                        }
                        else if (columnName == "Scheduler Delay") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 11
                            };
                            task_summary_metrics_table_array.push(row);
                        }
                        else if (columnName == "Task Deserialization Time") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 12
                            };
                            task_summary_metrics_table_array.push(row);
                        }
                        else if (columnName == "Result Serialization Time") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 15
                            };
                            task_summary_metrics_table_array.push(row);
                        }
                        else if (columnName == "Getting Result Time") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 16
                            };
                            task_summary_metrics_table_array.push(row);
                        }
                        else if (columnName == "Peak Execution Memory") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 17
                            };
                            task_summary_metrics_table_array.push(row);
                        }
                        else if (columnName == "Input Size / Records") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 1
                            };
                            if (dataToShow.showInputData) {
                                task_summary_metrics_table_array.push(row);
                            }
                        }
                        else if (columnName == "Output Size / Records") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 2
                            };
                            if (dataToShow.showOutputData) {
                                task_summary_metrics_table_array.push(row);
                            }
                        }
                        else if (columnName == "Shuffle Write Size / Records") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 4
                            };
                            if (dataToShow.showShuffleWriteData) {
                                task_summary_metrics_table_array.push(row);
                            }
                        }
                        else if (columnName == "Shuffle spill (disk)") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 5
                            };
                            if (dataToShow.showBytesSpilledData) {
                                task_summary_metrics_table_array.push(row);
                            }
                        }
                        else if (columnName == "Shuffle spill (memory)") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 6
                            };
                            if (dataToShow.showBytesSpilledData) {
                                task_summary_metrics_table_array.push(row);
                            }
                        }
                        else if (columnName != "NA") {
                            var row = {
                                "metric": columnName,
                                "data": taskMetricsResponse[ix],
                                "checkboxId": 0
                            };
                            task_summary_metrics_table_array.push(row);
                        }
                    });
                    var task_summary_metrics_table_filtered_array =
                        task_summary_metrics_table_array.filter(row => row.checkboxId < 11);
                    createDataTableForTaskSummaryMetricsTable(task_summary_metrics_table_filtered_array);
                });

                // building task aggregated metrics by executor table
                var executorSummaryTable = "#summary-executor-table";
                var executor_summary_conf = {
                    "data": executor_summary_table,
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
                                return row.inputRecords != 0 ? formatBytes(row.inputBytes, type) + " / " + row.inputRecords : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.outputRecords != 0 ? formatBytes(row.outputBytes, type) + " / " + row.outputRecords : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleReadRecords != 0 ? formatBytes(row.shuffleRead, type) + " / " + row.shuffleReadRecords : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleWriteRecords != 0 ? formatBytes(row.shuffleWrite, type) + " / " + row.shuffleWriteRecords : 0;
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
                    "order": [[0, "asc"]]
                }
                var executorSummaryTableSelector =
                  $(executorSummaryTable).DataTable(executor_summary_conf);
                $('#parent-container [data-toggle="tooltip"]').tooltip();

                // building accumulator update table
                var accumulatorTable = "#accumulator-table";
                var accumulator_conf = {
                    "data": accumulator_table,
                    "columns": [
                        {data : "id"},
                        {data : "name"},
                        {data : "value"}
                    ],
                    "paging": false,
                    "searching": false,
                    "order": [[0, "asc"]]
                }
                $(accumulatorTable).DataTable(accumulator_conf);

                // building tasks table that uses server side functionality
                var taskTable = "#active-tasks-table";
                var task_conf = {
                    "serverSide": true,
                    "paging": true,
                    "info": true,
                    "processing": true,
                    "lengthMenu": [[20, 40, 60, 100, responseBody.numTasks], [20, 40, 60, 100, "All"]],
                    "ajax": {
                        "url": stageEndPoint(appId) + "/" + stageAttemptId + "/taskTable",
                        "data": {
                            "numTasks": responseBody.numTasks
                        },
                        "dataSrc": function ( jsons ) {
                            var jsonStr = JSON.stringify(jsons);
                            var tasksToShow = JSON.parse(jsonStr);
                            return tasksToShow.aaData;
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
                                if ("taskMetrics" in row) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.executorRunTime) : row.taskMetrics.executorRunTime;
                                } else {
                                    return "N/A";
                                }
                            },
                            name: "Duration"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.jvmGcTime) : row.taskMetrics.jvmGcTime;
                                } else {
                                    return "N/A";
                                }
                            },
                            name: "GC Time"
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.schedulerDelay) : row.schedulerDelay;
                            },
                            name: "Scheduler Delay"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.executorDeserializeTime) : row.taskMetrics.executorDeserializeTime;
                                } else {
                                    return "N/A";
                                }
                            },
                            name: "Task Deserialization Time"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.shuffleReadMetrics !== 'undefined') {
                                        return type === 'display' ? formatDuration(row.taskMetrics.shuffleReadMetrics.fetchWaitTime) : row.taskMetrics.shuffleReadMetrics.fetchWaitTime;
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Read Blocked Time"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    return type === 'display' ? formatBytes(row.taskMetrics.shuffleReadMetrics.remoteBytesRead, type) : row.taskMetrics.shuffleReadMetrics.remoteBytesRead;
                                } else {
                                    return "N/A";
                                }
                            },
                            name: "Shuffle Remote Reads"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    return type === 'display' ? formatDuration(row.taskMetrics.resultSerializationTime) : row.taskMetrics.resultSerializationTime;
                                } else {
                                    return "N/A";
                                }
                            },
                            name: "Result Serialization Time"
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.gettingResultTime) : row.gettingResultTime;
                            },
                            name: "Getting Result Time"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    return type === 'display' ? formatBytes(row.taskMetrics.peakExecutionMemory, type) : row.taskMetrics.peakExecutionMemory;
                                } else {
                                    return "N/A";
                                }
                            },
                            name: "Peak Execution Memory"
                        },
                        {
                            data : function (row, type) {
                                if (accumulator_table.length > 0 && row.accumulatorUpdates.length > 0) {
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
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.inputMetrics.bytesRead > 0) {
                                        if (type === 'display') {
                                            return formatBytes(row.taskMetrics.inputMetrics.bytesRead, type) + " / " + row.taskMetrics.inputMetrics.recordsRead;
                                        } else {
                                            return row.taskMetrics.inputMetrics.bytesRead + " / " + row.taskMetrics.inputMetrics.recordsRead;
                                        }
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Input Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.outputMetrics.bytesWritten > 0) {
                                        if (type === 'display') {
                                            return formatBytes(row.taskMetrics.outputMetrics.bytesWritten, type) + " / " + row.taskMetrics.outputMetrics.recordsWritten;
                                        } else {
                                            return row.taskMetrics.outputMetrics.bytesWritten + " / " + row.taskMetrics.outputMetrics.recordsWritten;
                                        }
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Output Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.shuffleWriteMetrics.writeTime > 0) {
                                        return type === 'display' ? formatDuration(parseInt(row.taskMetrics.shuffleWriteMetrics.writeTime) / 1000000) : row.taskMetrics.shuffleWriteMetrics.writeTime;
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Write Time"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.shuffleWriteMetrics.bytesWritten > 0) {
                                        if (type === 'display') {
                                            return formatBytes(row.taskMetrics.shuffleWriteMetrics.bytesWritten, type) + " / " + row.taskMetrics.shuffleWriteMetrics.recordsWritten;
                                        } else {
                                            return row.taskMetrics.shuffleWriteMetrics.bytesWritten + " / " + row.taskMetrics.shuffleWriteMetrics.recordsWritten;
                                        }
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Write Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.shuffleReadMetrics.localBytesRead > 0) {
                                        var totalBytesRead = parseInt(row.taskMetrics.shuffleReadMetrics.localBytesRead) + parseInt(row.taskMetrics.shuffleReadMetrics.remoteBytesRead);
                                        if (type === 'display') {
                                            return formatBytes(totalBytesRead, type) + " / " + row.taskMetrics.shuffleReadMetrics.recordsRead;
                                        } else {
                                            return totalBytesRead + " / " + row.taskMetrics.shuffleReadMetrics.recordsRead;
                                        }
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Read Size / Records"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.memoryBytesSpilled > 0) {
                                        return type === 'display' ? formatBytes(row.taskMetrics.memoryBytesSpilled, type) : row.taskMetrics.memoryBytesSpilled;
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Spill (Memory)"
                        },
                        {
                            data : function (row, type) {
                                if ("taskMetrics" in row) {
                                    if (row.taskMetrics.diskBytesSpilled > 0) {
                                        return type === 'display' ? formatBytes(row.taskMetrics.diskBytesSpilled, type) : row.taskMetrics.diskBytesSpilled;
                                    } else {
                                        return "";
                                    }
                                } else {
                                    return "";
                                }
                            },
                            name: "Shuffle Spill (Disk)"
                        },
                        {
                            data : function (row, type) {
                                var msg = row.errorMessage;
                                if (typeof msg === 'undefined'){
                                    return "";
                                } else {
                                        var form_head = msg.substring(0, msg.indexOf("at"));
                                        var form = "<span onclick=\"this.parentNode.querySelector('.stacktrace-details').classList.toggle('collapsed')\" class=\"expand-details\">+details</span>";
                                        var form_msg = "<div class=\"stacktrace-details collapsed\"><pre>" + row.errorMessage + "</pre></div>";
                                        return form_head + form + form_msg;
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
                var taskTableSelector = $(taskTable).DataTable(task_conf);
                $('#active-tasks-table_filter input').unbind();
                var searchEvent;
                $('#active-tasks-table_filter input').bind('keyup', function(e) {
                  if (typeof searchEvent !== 'undefined') {
                    window.clearTimeout(searchEvent);
                  }
                  var value = this.value;
                  searchEvent = window.setTimeout(function(){
                    taskTableSelector.search( value ).draw();}, 2500);
                });

                var optionalColumns = [11, 12, 13, 14, 15, 16, 17];
                var allChecked = true;
                for(k = 0; k < optionalColumns.length; k++) {
                    if (taskTableSelector.column(optionalColumns[k]).visible()) {
                        $("#box-"+optionalColumns[k]).checked = true;
                    } else {
                        allChecked = false;
                    }
                }
                if (allChecked) {
                    $("#box-0").checked = true;
                }

                // hide or show columns dynamically event
                $('input.toggle-vis').on('click', function(e){
                    // Get the column
                    var para = $(this).attr('data-column');
                    if (para == "0") {
                        var column = taskTableSelector.column(optionalColumns);
                        if ($(this).is(":checked")) {
                            $(".toggle-vis").prop('checked', true);
                            column.visible(true);
                            createDataTableForTaskSummaryMetricsTable(task_summary_metrics_table_array);
                        } else {
                            $(".toggle-vis").prop('checked', false);
                            column.visible(false);
                            var task_summary_metrics_table_filtered_array =
                                task_summary_metrics_table_array.filter(row => row.checkboxId < 11);
                            createDataTableForTaskSummaryMetricsTable(task_summary_metrics_table_filtered_array);
                        }
                    } else {
                        var column = taskTableSelector.column(para);
                        // Toggle the visibility
                        column.visible(!column.visible());
                        var task_summary_metrics_table_filtered_array = [];
                        if ($(this).is(":checked")) {
                            task_summary_metrics_table_current_state_array.push(task_summary_metrics_table_array.filter(row => (row.checkboxId).toString() == para)[0]);
                            task_summary_metrics_table_filtered_array = task_summary_metrics_table_current_state_array.slice();
                        } else {
                            task_summary_metrics_table_filtered_array =
                                task_summary_metrics_table_current_state_array.filter(row => (row.checkboxId).toString() != para);
                        }
                        createDataTableForTaskSummaryMetricsTable(task_summary_metrics_table_filtered_array);
                    }
                });

                // title number and toggle list
                $("#summaryMetricsTitle").html("Summary Metrics for " + "<a href='#tasksTitle'>" + responseBody.numCompleteTasks + " Completed Tasks" + "</a>");
                $("#tasksTitle").html("Task (" + responseBody.numCompleteTasks + ")");

                // hide or show the accumulate update table
                if (accumulator_table.length == 0) {
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

                executorSummaryTableSelector.column(9).visible(dataToShow.showInputData);
                executorSummaryTableSelector.column(10).visible(dataToShow.showOutputData);
                executorSummaryTableSelector.column(11).visible(dataToShow.showShuffleReadData);
                executorSummaryTableSelector.column(12).visible(dataToShow.showShuffleWriteData);
                executorSummaryTableSelector.column(13).visible(dataToShow.showBytesSpilledData);
                executorSummaryTableSelector.column(14).visible(dataToShow.showBytesSpilledData);
            });
        });
    });
});
