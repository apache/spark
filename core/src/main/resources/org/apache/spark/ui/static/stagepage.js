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

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function () {
    $.blockUI({message: '<h3>Loading Stage Page...</h3>'});
});

$.extend( $.fn.dataTable.ext.type.order, {
    "file-size-pre": ConvertDurationString,

    "file-size-asc": function ( a, b ) {
        a = ConvertDurationString( a );
        b = ConvertDurationString( b );
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "file-size-desc": function ( a, b ) {
        a = ConvertDurationString( a );
        b = ConvertDurationString( b );
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

// This function will only parse the URL under certain format
// e.g. https://axonitered-jt1.red.ygrid.yahoo.com:50509/history/application_1502220952225_59143/stages/stage/?id=0&attempt=0
function stageEndPoint(appId) {
    var words = document.baseURI.split('/');
    var words2 = document.baseURI.split('?');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        var stageIdLen = words2[1].indexOf('&');
        var stageId = words2[1].substr(3, stageIdLen - 3);
        var newBaseURI = words.slice(0, ind + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        var attemptId = words[ind + 2];
        var stageIdLen = words2[1].indexOf('&');
        var stageId = words2[1].substr(3, stageIdLen - 3);
        var newBaseURI = words.slice(0, ind).join('/');
        if (isNaN(attemptId) || attemptId == "0") {
            return newBaseURI + "/api/v1/applications/" + appId + "/stages/" + stageId;
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/stages/" + stageId;
        }
    }
    var stageIdLen = words2[1].indexOf('&');
    var stageId = words2[1].substr(3, stageIdLen - 3);
    return location.origin + "/api/v1/applications/" + appId + "/stages/" + stageId;
}

function getColumnNameForTaskMetricSummary(columnKey) {
    if(columnKey == "executorRunTime") {
        return "Duration";
    }
    else if(columnKey == "jvmGcTime") {
        return "GC Time";
    }
    else if(columnKey == "gettingResultTime") {
        return "Getting Result Time";
    }
    else if(columnKey == "inputMetrics") {
        return "Input Size / Records";
    }
    else if(columnKey == "outputMetrics") {
        return "Output Size / Records";
    }
    else if(columnKey == "peakExecutionMemory") {
        return "Peak Execution Memory";
    }
    else if(columnKey == "resultSerializationTime") {
        return "Result Serialization Time";
    }
    else if(columnKey == "schedulerDelay") {
        return "Scheduler Delay";
    }
    else if(columnKey == "diskBytesSpilled") {
        return "Shuffle spill (disk)";
    }
    else if(columnKey == "memoryBytesSpilled") {
        return "Shuffle spill (memory)";
    }
    else if(columnKey == "shuffleReadMetrics") {
        return "Shuffle Read Size / Records";
    }
    else if(columnKey == "shuffleWriteMetrics") {
        return "Shuffle Write Size / Records";
    }
    else if(columnKey == "executorDeserializeTime") {
        return "Task Deserialization Time";
    }
    return "NA";
}

$(document).ready(function () {
    setDataTableDefaults();

    $("#showAdditionalMetrics").append(
        "<div><a id='taskMetric'>" +
        "<span class='expand-input-rate-arrow arrow-closed' id='arrowtoggle1'></span>" +
        " Show Additional Metrics" +
        "</a></div>" +
        "<div class='container-fluid' id='toggle-metrics' hidden>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-0' data-column='0'> Select All</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-11' data-column='11'> Scheduler Delay</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-12' data-column='12'> Task Deserialization Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-13' data-column='13'> Shuffle Read Blocked Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-14' data-column='14'> Shuffle Remote Reads</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-15' data-column='15'> Result Serialization Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-16' data-column='16'> Getting Result Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-17' data-column='17'> Peak Execution Memory</div>" +
        "</div>");

    tasksSummary = $("#active-tasks");
    getStandAloneAppId(function (appId) {

        var endPoint = stageEndPoint(appId);
        $.getJSON(endPoint, function(response, status, jqXHR) {

            // prepare data for task aggregated metrics table
            indices = Object.keys(response[0].executorSummary);
            var task_summary_table = [];
            indices.forEach(function (ix) {
               response[0].executorSummary[ix].id = ix;
               task_summary_table.push(response[0].executorSummary[ix]);
            });

            // prepare data for accumulatorUpdates
            var indices = Object.keys(response[0].accumulatorUpdates);
            var accumulator_table_all = [];
            var accumulator_table = [];
            indices.forEach(function (ix) {
               accumulator_table_all.push(response[0].accumulatorUpdates[ix]);
            });

            accumulator_table_all.forEach(function (x){
                var name = (x.name).toString();
                if(name.includes("internal.") == false){
                    accumulator_table.push(x);
                }
            });

            // rendering the UI page
            var data = {"executors": response};
            $.get(createTemplateURI(appId, "stagespage"), function(template) {
                tasksSummary.append(Mustache.render($(template).filter("#stages-summary-template").html(), data));

                $("#taskMetric").click(function(){
                    $("#arrowtoggle1").toggleClass("arrow-open arrow-closed");
                    $("#toggle-metrics").toggle();
                });

                $("#aggregatedMetrics").click(function(){
                    $("#arrowtoggle2").toggleClass("arrow-open arrow-closed");
                    $("#toggle-aggregatedMetrics").toggle();
                });

                var task_metrics_table = [];
                var stageAttemptId = getStageAttemptId();
                $.getJSON(stageEndPoint(appId) + "/"+stageAttemptId+"/taskSummary?quantiles=0,0.25,0.5,0.75,1.0", function(response1, status, jqXHR) {
                    var taskMetricIndices = Object.keys(response1);
                    taskMetricIndices.forEach(function (ix) {
                        var columnName = getColumnNameForTaskMetricSummary(ix);
                        if (columnName == "Shuffle Read Size / Records") {
                            var row1 = {
                                "metric": columnName,
                                "data": response1[ix]
                            };
                            var row2 = {
                                "metric": "Shuffle Read Blocked Time",
                                "data": response1[ix]
                            };
                            var row3 = {
                                "metric": "Shuffle Remote Reads",
                                "data": response1[ix]
                            };
                            task_metrics_table.push(row1);
                            task_metrics_table.push(row2);
                            task_metrics_table.push(row3);
                        }
                        else if (columnName != "NA") {
                            var row = {
                                "metric": columnName,
                                "data": response1[ix]
                            };
                            task_metrics_table.push(row);
                        }
                    });

                    var taskMetricsTable = "#summary-metrics-table";
                    var task_conf = {
                        "data": task_metrics_table,
                        "columns": [
                            {data : 'metric'},
                            {
                                data: function (row, type) {
                                    if (row.metric == 'Input Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesRead).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[0], type) + " / " + str2arr[0];
                                        return str;
                                    } else if (row.metric == 'Output Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesWritten).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsWritten).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[0], type) + " / " + str2arr[0];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Size / Records') {
                                        var str1arr = JSON.stringify(row.data.readBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.readRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[0], type) + " / " + str2arr[0];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Blocked Time') {
                                        var str1arr = JSON.stringify(row.data.fetchWaitTime).split("[")[1].split("]")[0].split(",");
                                        var str = formatDuration(str1arr[0]);
                                        return str;
                                    } else if (row.metric == 'Shuffle Remote Reads') {
                                        var str1arr = JSON.stringify(row.data.remoteBytesRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[0], type);
                                        return str;
                                    } else if (row.metric == 'Shuffle Write Size / Records') {
                                        var str1arr = JSON.stringify(row.data.writeBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.writeRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[0], type) + " / " + str2arr[0];
                                        return str;
                                    } else {
                                        return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                                || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.data[0], type) : (formatDuration(row.data[0]));
                                    }
                                }
                            },
                            {
                                data: function (row, type) {
                                    if (row.metric == 'Input Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesRead).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[1], type) + " / " + str2arr[1];
                                        return str;
                                    } else if (row.metric == 'Output Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesWritten).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsWritten).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[1], type) + " / " + str2arr[1];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Size / Records') {
                                        var str1arr = JSON.stringify(row.data.readBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.readRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[1], type) + " / " + str2arr[1];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Blocked Time') {
                                        var str1arr = JSON.stringify(row.data.fetchWaitTime).split("[")[1].split("]")[0].split(",");
                                        var str = formatDuration(str1arr[1]);
                                        return str;
                                    } else if (row.metric == 'Shuffle Remote Reads') {
                                        var str1arr = JSON.stringify(row.data.remoteBytesRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[1], type);
                                        return str;
                                    } else if (row.metric == 'Shuffle Write Size / Records') {
                                        var str1arr = JSON.stringify(row.data.writeBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.writeRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[1], type) + " / " + str2arr[1];
                                        return str;
                                    } else {
                                        return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                                || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.data[1], type) : (formatDuration(row.data[1]));
                                    }
                                }
                            },
                            {
                                data: function (row, type) {
                                    if (row.metric == 'Input Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesRead).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[2], type) + " / " + str2arr[2];
                                        return str;
                                    } else if (row.metric == 'Output Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesWritten).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsWritten).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[2], type) + " / " + str2arr[2];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Size / Records') {
                                        var str1arr = JSON.stringify(row.data.readBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.readRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[2], type) + " / " + str2arr[2];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Blocked Time') {
                                        var str1arr = JSON.stringify(row.data.fetchWaitTime).split("[")[1].split("]")[0].split(",");
                                        var str = formatDuration(str1arr[2]);
                                        return str;
                                    } else if (row.metric == 'Shuffle Remote Reads') {
                                        var str1arr = JSON.stringify(row.data.remoteBytesRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[2], type);
                                        return str;
                                    } else if (row.metric == 'Shuffle Write Size / Records') {
                                        var str1arr = JSON.stringify(row.data.writeBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.writeRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[2], type) + " / " + str2arr[2];
                                        return str;
                                    } else {
                                        return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                                || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.data[2], type) : (formatDuration(row.data[2]));
                                    }
                                }
                            },
                            {
                                data: function (row, type) {
                                    if (row.metric == 'Input Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesRead).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[3], type) + " / " + str2arr[3];
                                        return str;
                                    } else if (row.metric == 'Output Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesWritten).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsWritten).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[3], type) + " / " + str2arr[3];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Size / Records') {
                                        var str1arr = JSON.stringify(row.data.readBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.readRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[3], type) + " / " + str2arr[3];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Blocked Time') {
                                        var str1arr = JSON.stringify(row.data.fetchWaitTime).split("[")[1].split("]")[0].split(",");
                                        var str = formatDuration(str1arr[3]);
                                        return str;
                                    } else if (row.metric == 'Shuffle Remote Reads') {
                                        var str1arr = JSON.stringify(row.data.remoteBytesRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[3], type);
                                        return str;
                                    } else if (row.metric == 'Shuffle Write Size / Records') {
                                        var str1arr = JSON.stringify(row.data.writeBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.writeRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[3], type) + " / " + str2arr[3];
                                        return str;
                                    } else {
                                        return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                                || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.data[3], type) : (formatDuration(row.data[3]));
                                    }
                                }
                            },
                            {
                                data: function (row, type) {
                                    if (row.metric == 'Input Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesRead).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsRead).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[4], type) + " / " + str2arr[4];
                                        return str;
                                    } else if (row.metric == 'Output Size / Records') {
                                        var str1arr = JSON.stringify(row.data.bytesWritten).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.recordsWritten).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[4], type) + " / " + str2arr[4];
                                        return str;
                                    } else if (row.metric == 'Shuffle Read Size / Records') {
                                        var str1arr = JSON.stringify(row.data.readBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.readRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[4], type) + " / " + str2arr[4];
                                        return str;
                                      } else if (row.metric == 'Shuffle Read Blocked Time') {
                                          var str1arr = JSON.stringify(row.data.fetchWaitTime).split("[")[1].split("]")[0].split(",");
                                          var str = formatDuration(str1arr[4]);
                                          return str;
                                      } else if (row.metric == 'Shuffle Remote Reads') {
                                          var str1arr = JSON.stringify(row.data.remoteBytesRead).split("[")[1].split("]")[0].split(",");
                                          var str = formatBytes(str1arr[4], type);
                                          return str;
                                      } else if (row.metric == 'Shuffle Write Size / Records') {
                                        var str1arr = JSON.stringify(row.data.writeBytes).split("[")[1].split("]")[0].split(",");
                                        var str2arr = JSON.stringify(row.data.writeRecords).split("[")[1].split("]")[0].split(",");
                                        var str = formatBytes(str1arr[4], type) + " / " + str2arr[4];
                                        return str;
                                    } else {
                                        return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                                || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.data[4], type) : (formatDuration(row.data[4]));
                                    }
                                }
                            }
                        ],
                        "columnDefs": [
                            { "type": "file-size", "targets": 1 },
                            { "type": "file-size", "targets": 2 },
                            { "type": "file-size", "targets": 3 },
                            { "type": "file-size", "targets": 4 },
                            { "type": "file-size", "targets": 5 }
                        ],
                        "paging": false,
                        "searching": false,
                        "order": [[0, "asc"]]
                    };
                    $(taskMetricsTable).DataTable(task_conf);
                });

                // building task aggregated metric table
                var tasksSummarytable = "#summary-stages-table";
                var task_summary_conf = {
                    "data": task_summary_table,
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
                $(tasksSummarytable).DataTable(task_summary_conf);
                $('#active-tasks [data-toggle="tooltip"]').tooltip();

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
                    "ajax": {
                        "url": stageEndPoint(appId) + "/" + stageAttemptId + "/taskTable",
                        "data": {
                            "numTasks": response[0].numTasks
                            },
                        "dataSrc": function ( jsons ) {
                            var jsonStr = JSON.stringify(jsons);
                            var marrr = JSON.parse(jsonStr);
                            return marrr.aaData;
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
                $('#active-tasks-table_filter input').bind('keyup', function(e) {
                  if(e.keyCode == 13) {
                    taskTableSelector.search( this.value ).draw();
                  }
                });

                var optionalColumns = [11, 12, 13, 14, 15, 16, 17];
                var allChecked = true;
                for(k = 0; k < optionalColumns.length; k++) {
                    if (taskTableSelector.column(optionalColumns[k]).visible()) {
                        document.getElementById("box-"+optionalColumns[k]).checked = true;
                    } else {
                        allChecked = false;
                    }
                }
                if (allChecked) {
                    document.getElementById("box-0").checked = true;
                }

                // hide or show columns dynamically event
                $('input.toggle-vis').on('click', function(e){
                    // Get the column
                    var para = $(this).attr('data-column');
                    if(para == "0"){
                        var column = taskTableSelector.column([11, 12, 13, 14, 15, 16, 17]);
                        if($(this).is(":checked")){
                            $(".toggle-vis").prop('checked', true);
                            column.visible(true);
                        } else {
                            $(".toggle-vis").prop('checked', false);
                            column.visible(false);
                        }
                    } else {
                    var column = taskTableSelector.column($(this).attr('data-column'));
                    // Toggle the visibility
                    column.visible(!column.visible());
                    }
                });

                // title number and toggle list
                $("#summaryMetricsTitle").html("Summary Metrics for " + "<a href='#tasksTitle'>" + response[0].numCompleteTasks + " Completed Tasks" + "</a>");
                $("#tasksTitle").html("Task (" + response[0].numCompleteTasks + ")");

                // hide or show the accumulate update table
                if (accumulator_table.length == 0) {
                    $("#accumulator-update-table").hide();
                } else {
                    taskTableSelector.column(18).visible(true);
                    $("#accumulator-update-table").show();
                }
            });
        });
    });
});
