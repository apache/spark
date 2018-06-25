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

function sortRecords(a,b) {
  var aSplit = a.split("/");
  var bSplit = b.split("/");
  if (aSplit[0] == bSplit[0]) {
    return aSplit[1] - bSplit[1];
  }
  return aSplit[0] - bSplit[0];
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
               task_table.push(response[0].tasks[parseInt(ix)]);
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
            var inputSizeRecordsSummary = [];
            var outputSizeRecordsSummary = [];
            var shuffleWriteSizeRecordsSummary = [];
            var shuffleSpillMemorySummary = [];
            var shuffleSpillDiskSummary = [];

            console.log("hereeeeeeeeee 1 "+task_table.length);
            task_table.forEach(function (x){
                if ("taskMetrics" in x) {
                  durationSummary.push(x.taskMetrics.executorRunTime);
                  taskDeserializationSummary.push(x.taskMetrics.executorDeserializeTime);
                  resultSerializationTimeSummary.push(x.taskMetrics.resultSerializationTime);
                  gcTimeSummary.push(x.taskMetrics.jvmGcTime);
                  peakExecutionMemorySummary.push(x.taskMetrics.peakExecutionMemory);
                  if (x.taskMetrics.inputMetrics.bytesRead > 0) {
                    inputSizeRecordsSummary.push(x.taskMetrics.inputMetrics.bytesRead + "/" + x.taskMetrics.inputMetrics.recordsRead);
                  }
                  if (x.taskMetrics.outputMetrics.bytesWritten > 0) {
                    outputSizeRecordsSummary.push(x.taskMetrics.outputMetrics.bytesWritten + "/" + x.taskMetrics.outputMetrics.recordsWritten);
                  }
                  if (x.taskMetrics.shuffleWriteMetrics.bytesWritten > 0) {
                    shuffleWriteSizeRecordsSummary.push(x.taskMetrics.shuffleWriteMetrics.bytesWritten + "/" + x.taskMetrics.shuffleWriteMetrics.recordsWritten);
                  }
                  if (x.taskMetrics.memoryBytesSpilled > 0) {
                    shuffleSpillMemorySummary.push(x.taskMetrics.memoryBytesSpilled);
                  }
                  if (x.taskMetrics.diskBytesSpilled > 0) {
                    shuffleSpillDiskSummary.push(x.taskMetrics.diskBytesSpilled);
                  }
                }
                schedulerDelaySummary.push(x.schedulerDelay);
                gettingResultTimeSummary.push(x.gettingResultTime);
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
            if (inputSizeRecordsSummary.length > 0) {
              task_metrics_table_all.push(inputSizeRecordsSummary);
              task_metrics_table_col.push("Input Size / Records");
            }
            if (outputSizeRecordsSummary.length > 0) {
              task_metrics_table_all.push(outputSizeRecordsSummary);
              task_metrics_table_col.push("Output Size / Records");
            }
            if (shuffleWriteSizeRecordsSummary.length > 0) {
              task_metrics_table_all.push(shuffleWriteSizeRecordsSummary);
              task_metrics_table_col.push("Shuffle Write Size / Records");
            }
            if (shuffleSpillMemorySummary.length > 0) {
              task_metrics_table_all.push(shuffleSpillMemorySummary);
              task_metrics_table_col.push("Shuffle spill (memory)");
            }
            if (shuffleSpillDiskSummary.length > 0) {
              task_metrics_table_all.push(shuffleSpillDiskSummary);
              task_metrics_table_col.push("Shuffle spill (disk)");
            }

            for(i = 0; i < task_metrics_table_col.length; i++){
              var task_sort_table;
              if (task_metrics_table_col[i] == 'Input Size / Records' || task_metrics_table_col[i] == 'Output Size / Records'
                  || task_metrics_table_col[i] == 'Shuffle Write Size / Records') {
                task_sort_table = (task_metrics_table_all[i]).sort(sortRecords);
              } else {
                task_sort_table = (task_metrics_table_all[i]).sort(sortNumber);
              }
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
                                if (row.metric == 'Input Size / Records' || row.metric == 'Output Size / Records'
                                    || row.metric == 'Shuffle Write Size / Records') {
                                  var strarray = row.p0.split("/");
                                  var str = formatBytes(strarray[0], type) + " / " + strarray[1];
                                  return str;
                                } else {
                                  return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                      || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.p0, type) : (formatDuration(row.p0));
                                }
                            }
                        },
                        {
                            data: function (row, type) {
                                if (row.metric == 'Input Size / Records' || row.metric == 'Output Size / Records'
                                    || row.metric == 'Shuffle Write Size / Records') {
                                  var strarray = row.p25.split("/");
                                  var str = formatBytes(strarray[0], type) + " / " + strarray[1];
                                  return str;
                                } else {
                                  return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                      || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.p25, type) : (formatDuration(row.p25));
                                }
                            }
                        },
                        {
                            data: function (row, type) {
                                if (row.metric == 'Input Size / Records' || row.metric == 'Output Size / Records'
                                    || row.metric == 'Shuffle Write Size / Records') {
                                  var strarray = row.p50.split("/");
                                  var str = formatBytes(strarray[0], type) + " / " + strarray[1];
                                  return str;
                                } else {
                                  return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                      || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.p50, type) : (formatDuration(row.p50));
                                }
                            }
                        },
                        {
                            data: function (row, type) {
                                if (row.metric == 'Input Size / Records' || row.metric == 'Output Size / Records'
                                    || row.metric == 'Shuffle Write Size / Records') {
                                  var strarray = row.p75.split("/");
                                  var str = formatBytes(strarray[0], type) + " / " + strarray[1];
                                  return str;
                                } else {
                                  return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                      || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.p75, type) : (formatDuration(row.p75));
                                }
                            }
                        },
                        {
                            data: function (row, type) {
                                if (row.metric == 'Input Size / Records' || row.metric == 'Output Size / Records'
                                    || row.metric == 'Shuffle Write Size / Records') {
                                  var strarray = row.p100.split("/");
                                  var str = formatBytes(strarray[0], type) + " / " + strarray[1];
                                  return str;
                                } else {
                                  return (row.metric == 'Peak Execution Memory' || row.metric == 'Shuffle spill (memory)'
                                      || row.metric == 'Shuffle spill (disk)') ? formatBytes(row.p100, type) : (formatDuration(row.p100));
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

                // building tasks table
                var taskTable = "#active-tasks-table";
                var task_conf = {
                    "serverSide": true,
                    "paging": false,
                    "info": true,
                    "processing": true,
                    "ajax": {
                        "url": stageEndPoint(appId) + "/taskTable",
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
                                    return row.accumulatorUpdates[0].name + ' : ' + row.accumulatorUpdates[0].update;
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
                        { "visible": false, "targets": 18 },
                        { "visible": false, "targets": 19 },
                        { "visible": false, "targets": 20 },
                        { "visible": false, "targets": 21 },
                        { "visible": false, "targets": 22 },
                        { "visible": false, "targets": 23 },
                        { "visible": false, "targets": 24 }
                    ],
                };
                var taskTableSelector = $(taskTable).DataTable(task_conf);

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

                if (inputSizeRecordsSummary.length > 0) {
                    taskTableSelector.column(19).visible(true);
                }
                if (outputSizeRecordsSummary.length > 0) {
                    taskTableSelector.column(20).visible(true);
                }
                if (shuffleWriteSizeRecordsSummary.length > 0) {
                    taskTableSelector.column(21).visible(true);
                }
                if (shuffleWriteSizeRecordsSummary.length > 0) {
                    taskTableSelector.column(22).visible(true);
                }
                if (shuffleSpillMemorySummary.length > 0) {
                    taskTableSelector.column(23).visible(true);
                }
                if (shuffleSpillDiskSummary.length > 0) {
                    taskTableSelector.column(24).visible(true);
                }
            });
        });
    });
});
