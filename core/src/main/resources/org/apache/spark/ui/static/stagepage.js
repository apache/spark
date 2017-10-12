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

function sortNumber(a,b) {
    return a - b;
}

function quantile(array, percentile) {
    index = percentile/100. * (array.length-1);
    if (Math.floor(index) == index) {
    	result = array[index];
    } else {
        var i = Math.floor(index);
        fraction = index - i;
        result = array[i];
    }
    return result;
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
        "<div><input type='checkbox' class='toggle-vis' id='box-10' data-column='10'> Scheduler Delay</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-11' data-column='11'> Task Deserialization Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-12' data-column='12'> Shuffle Read Blocked Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-13' data-column='13'> Shuffle Remote Reads</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-14' data-column='14'> Result Serialization Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-15' data-column='15'> Getting Result Time</div>" +
        "<div><input type='checkbox' class='toggle-vis' id='box-16' data-column='16'> Peak Execution Memory</div>" +
        "</div>");

    tasksSummary = $("#active-tasks");
    getStandAloneAppId(function (appId) {

        var endPoint = stageEndPoint(appId);
        $.getJSON(endPoint, function(response, status, jqXHR) {

            // prepare data for tasks table
            var indices = Object.keys(response[0].tasks);
            var task_table = [];
            indices.forEach(function (ix) {
               task_table.push(response[0].tasks[ix]);
            });

            // prepare data for task aggregated metrics table
            indices = Object.keys(response[0].executorSummary);
            var task_summary_table = [];
            indices.forEach(function (ix) {
               response[0].executorSummary[ix].id = ix;
               task_summary_table.push(response[0].executorSummary[ix]);
            });

            // prepare data for task summary table
            var durationSummary = [];
            var schedulerDelaySummary = [];
            var taskDeserializationSummary = [];
            var gcTimeSummary = [];
            var resultSerializationTimeSummary = [];
            var gettingResultTimeSummary = [];
            var peakExecutionMemorySummary = [];

            task_table.forEach(function (x){
                durationSummary.push(x.taskMetrics.executorRunTime);
                schedulerDelaySummary.push(x.schedulerDelay);
                taskDeserializationSummary.push(x.taskMetrics.executorDeserializeTime);
                gcTimeSummary.push(x.taskMetrics.jvmGcTime);
                resultSerializationTimeSummary.push(x.taskMetrics.resultSerializationTime);
                gettingResultTimeSummary.push(x.gettingResultTime);
                peakExecutionMemorySummary.push(x.taskMetrics.peakExecutionMemory);
            });

            var task_metrics_table = [];
            var task_metrics_table_all = [];
            var task_metrics_table_col = ["Duration", "Scheduler Delay", "Task Deserialization Time", "GC Time", "Result Serialization Time", "Getting Result Time", "Peak Execution Memory"];

            task_metrics_table_all.push(durationSummary);
            task_metrics_table_all.push(schedulerDelaySummary);
            task_metrics_table_all.push(taskDeserializationSummary);
            task_metrics_table_all.push(gcTimeSummary);
            task_metrics_table_all.push(resultSerializationTimeSummary);
            task_metrics_table_all.push(gettingResultTimeSummary);
            task_metrics_table_all.push(peakExecutionMemorySummary);

            for(i = 0; i < task_metrics_table_col.length; i++){
                var task_sort_table = (task_metrics_table_all[i]).sort(sortNumber);
                var row = {
                    "metric": task_metrics_table_col[i],
                    "p0": quantile(task_sort_table, 0),
                    "p25": quantile(task_sort_table, 25),
                    "p50": quantile(task_sort_table, 50),
                    "p75": quantile(task_sort_table, 75),
                    "p100": quantile(task_sort_table, 100)
                };
                task_metrics_table.push(row);
            }

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
            var data = {executors: response, "taskstable": task_table, "task_metrics_table": task_metrics_table};
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

                // building task summary table
                var taskMetricsTable = "#summary-metrics-table";
                var task_conf = {
                    "data": task_metrics_table,
                    "columns": [
                        {data : 'metric'},
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p0)) : formatBytes(row.p0, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p25)) : formatBytes(row.p25, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p50)) : formatBytes(row.p50, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p75)) : formatBytes(row.p75, type);
                            }
                        },
                        {
                            data: function (row, type) {
                                return row.metric != 'Peak Execution Memory' ? (formatDuration(row.p100)) : formatBytes(row.p100, type);
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

               // building task aggregated metric table
                var tasksSummarytable = "#summary-stages-table";
                var task_summary_conf = {
                    "data": task_summary_table,
                    "columns": [
                        {data : "id"},
                        {data : "executorLogs", render: formatLogsCells},
                        {data : "host"},
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
                        {data : "blacklisted"},
                        {
                            data : function (row, type) {
                                return row.inputRecords != 0 ? formatBytes(row.inputBytes/row.inputRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.outputRecords != 0 ? formatBytes(row.outputBytes/row.outputRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleReadRecords != 0 ? formatBytes(row.shuffleRead/row.shuffleReadRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return row.shuffleWriteRecords != 0 ? formatBytes(row.shuffleWrite/row.shuffleWriteRecords) : 0;
                            }
                        },
                        {
                            data : function (row, type) {
                                return typeof row.memoryBytesSpilled != 'undefined' ? formatBytes(row.memoryBytesSpilled) : "";
                            }
                        },
                        {
                            data : function (row, type) {
                                return typeof row.diskBytesSpilled != 'undefined' ? formatBytes(row.diskBytesSpilled) : "";
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

                // building tasks table
                var taskTable = "#active-tasks-table";
                var task_conf = {
                    "data": task_table,
                    "columns": [
                        {data: function (row, type) {
                            return type !== 'display' ? (isNaN(row.index) ? 0 : row.index ) : row.index;
                            }
                        },
                        {data : "taskId"},
                        {data : "attempt"},
                        {data : "taskState"},
                        {data : "taskLocality"},
                        {
                            data : function (row, type) {
                                return row.executorId + ' / ' + row.host;
                            }
                        },
                        {data : "executorLogs", render: formatLogsCells},
                        {data : "launchTime", render: formatDate},
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.executorRunTime) : row.taskMetrics.executorRunTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.jvmGcTime) : row.taskMetrics.jvmGcTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.schedulerDelay) : row.schedulerDelay;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.executorDeserializeTime) : row.taskMetrics.executorDeserializeTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.shuffleReadMetrics.fetchWaitTime) : row.taskMetrics.shuffleReadMetrics.fetchWaitTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatBytes(row.taskMetrics.shuffleReadMetrics.remoteBytesRead, type) : row.taskMetrics.shuffleReadMetrics.remoteBytesRead;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.taskMetrics.resultSerializationTime) : row.taskMetrics.resultSerializationTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatDuration(row.gettingResultTime) : row.gettingResultTime;
                            }
                        },
                        {
                            data : function (row, type) {
                                return type === 'display' ? formatBytes(row.taskMetrics.peakExecutionMemory, type) : row.taskMetrics.peakExecutionMemory;
                            }
                        },
                        {
                            data : function (row, type) {
                                if (accumulator_table.length > 0 && row.accumulatorUpdates.length > 0) {
                                    return row.accumulatorUpdates[0].name + ' : ' + row.accumulatorUpdates[0].update;
                                } else {
                                    return "";
                                }
                            }
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
                            }
                        }
                    ],
                    "columnDefs": [
                        { "visible": false, "targets": 10 },
                        { "visible": false, "targets": 11 },
                        { "visible": false, "targets": 12 },
                        { "visible": false, "targets": 13 },
                        { "visible": false, "targets": 14 },
                        { "visible": false, "targets": 15 },
                        { "visible": false, "targets": 16 },
                        { "visible": false, "targets": 17 }
                    ],
                    "order": [[0, "asc"]]
                };
                var taskTableSelector = $(taskTable).DataTable(task_conf);

                var optionalColumns = [10, 11, 12, 13, 14, 15, 16];
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
                        var column = taskTableSelector.column([10, 11, 12, 13, 14, 15, 16]);
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
                $("#summaryMetricsTitle").html("Summary Metrics for " + "<a href='#tasksTitle'>" + task_table.length + " Completed Tasks" + "</a>");
                $("#tasksTitle").html("Task (" + task_table.length + ")");

                // hide or show the accumulate update table
                if (accumulator_table.length == 0) {
                    $("#accumulator-update-table").hide();
                } else {
                    taskTableSelector.column(17).visible(true);
                    $("#accumulator-update-table").show();
                }
            });
        });
    });
});
