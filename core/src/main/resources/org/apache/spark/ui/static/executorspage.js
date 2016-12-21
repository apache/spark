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

var threadDumpEnabled = false;

function setThreadDumpEnabled(val) {
    threadDumpEnabled = val;
}

function getThreadDumpEnabled() {
    return threadDumpEnabled;
}

function formatStatus(status, type) {
    if (type !== 'display') return status;
    if (status) {
        return "Active"
    } else {
        return "Dead"
    }
}

jQuery.extend(jQuery.fn.dataTableExt.oSort, {
    "title-numeric-pre": function (a) {
        var x = a.match(/title="*(-?[0-9\.]+)/)[1];
        return parseFloat(x);
    },

    "title-numeric-asc": function (a, b) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "title-numeric-desc": function (a, b) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
});

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function () {
    $.blockUI({message: '<h3>Loading Executors Page...</h3>'});
});

function createTemplateURI(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var baseURI = words.slice(0, ind + 1).join('/') + '/' + appId + '/static/executorspage-template.html';
        return baseURI;
    }
    ind = words.indexOf("history");
    if(ind > 0) {
        var baseURI = words.slice(0, ind).join('/') + '/static/executorspage-template.html';
        return baseURI;
    }
    return location.origin + "/static/executorspage-template.html";
}

function getStandAloneppId(cb) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        cb(appId);
        return;
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        cb(appId);
        return;
    }
    //Looks like Web UI is running in standalone mode
    //Let's get application-id using REST End Point
    $.getJSON(location.origin + "/api/v1/applications", function(response, status, jqXHR) {
        if (response && response.length > 0) {
            var appId = response[0].id
            cb(appId);
            return;
        }
    });
}

function createRESTEndPoint(appId) {
    var words = document.baseURI.split('/');
    var ind = words.indexOf("proxy");
    if (ind > 0) {
        var appId = words[ind + 1];
        var newBaseURI = words.slice(0, ind + 2).join('/');
        return newBaseURI + "/api/v1/applications/" + appId + "/allexecutors"
    }
    ind = words.indexOf("history");
    if (ind > 0) {
        var appId = words[ind + 1];
        var attemptId = words[ind + 2];
        var newBaseURI = words.slice(0, ind).join('/');
        if (isNaN(attemptId)) {
            return newBaseURI + "/api/v1/applications/" + appId + "/allexecutors";
        } else {
            return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId + "/allexecutors";
        }
    }
    return location.origin + "/api/v1/applications/" + appId + "/allexecutors";
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

// When GCTimePercent is edited change ToolTips.TASK_TIME to match
var GCTimePercent = 0.1;

function totalDurationStyle(totalGCTime, totalDuration) {
    // Red if GC time over GCTimePercent of total time
    return (totalGCTime > GCTimePercent * totalDuration) ?
        ("hsla(0, 100%, 50%, " + totalDurationAlpha(totalGCTime, totalDuration) + ")") : "";
}

function totalDurationColor(totalGCTime, totalDuration) {
    return (totalGCTime > GCTimePercent * totalDuration) ? "white" : "black";
}

$(document).ready(function () {
    $.extend($.fn.dataTable.defaults, {
        stateSave: true,
        lengthMenu: [[20, 40, 60, 100, -1], [20, 40, 60, 100, "All"]],
        pageLength: 20
    });

    executorsSummary = $("#active-executors");

    getStandAloneppId(function (appId) {
    
        var endPoint = createRESTEndPoint(appId);
        $.getJSON(endPoint, function (response, status, jqXHR) {
            var summary = [];
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
                allExecCnt += 1;
                allRDDBlocks += exec.rddBlocks;
                allMemoryUsed += exec.memoryUsed;
                allMaxMemory += exec.maxMemory;
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
                if (exec.isActive) {
                    activeExecCnt += 1;
                    activeRDDBlocks += exec.rddBlocks;
                    activeMemoryUsed += exec.memoryUsed;
                    activeMaxMemory += exec.maxMemory;
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
                } else {
                    deadExecCnt += 1;
                    deadRDDBlocks += exec.rddBlocks;
                    deadMemoryUsed += exec.memoryUsed;
                    deadMaxMemory += exec.maxMemory;
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
                }
            });
    
            var totalSummary = {
                "execCnt": ( "Total(" + allExecCnt + ")"),
                "allRDDBlocks": allRDDBlocks,
                "allMemoryUsed": allMemoryUsed,
                "allMaxMemory": allMaxMemory,
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
                "allTotalShuffleWrite": allTotalShuffleWrite
            };
            var activeSummary = {
                "execCnt": ( "Active(" + activeExecCnt + ")"),
                "allRDDBlocks": activeRDDBlocks,
                "allMemoryUsed": activeMemoryUsed,
                "allMaxMemory": activeMaxMemory,
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
                "allTotalShuffleWrite": activeTotalShuffleWrite
            };
            var deadSummary = {
                "execCnt": ( "Dead(" + deadExecCnt + ")" ),
                "allRDDBlocks": deadRDDBlocks,
                "allMemoryUsed": deadMemoryUsed,
                "allMaxMemory": deadMaxMemory,
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
                "allTotalShuffleWrite": deadTotalShuffleWrite
            };
    
            var data = {executors: response, "execSummary": [activeSummary, deadSummary, totalSummary]};
            $.get(createTemplateURI(appId), function (template) {
    
                executorsSummary.append(Mustache.render($(template).filter("#executors-summary-template").html(), data));
                var selector = "#active-executors-table";
                var conf = {
                    "data": response,
                    "columns": [
                        {
                            data: function (row, type) {
                                return type !== 'display' ? (isNaN(row.id) ? 0 : row.id ) : row.id;
                            }
                        },
                        {data: 'hostPort'},
                        {data: 'isActive', render: formatStatus},
                        {data: 'rddBlocks'},
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.memoryUsed, type) + ' / ' + formatBytes(row.maxMemory, type)) : row.memoryUsed;
                            }
                        },
                        {data: 'diskUsed', render: formatBytes},
                        {data: 'totalCores'},
                        {
                            data: 'activeTasks',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if (sData > 0) {
                                    $(nTd).css('color', 'white');
                                    $(nTd).css('background', activeTasksStyle(oData.activeTasks, oData.maxTasks));
                                }
                            }
                        },
                        {
                            data: 'failedTasks',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
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
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if (oData.totalDuration > 0) {
                                    $(nTd).css('color', totalDurationColor(oData.totalGCTime, oData.totalDuration));
                                    $(nTd).css('background', totalDurationStyle(oData.totalGCTime, oData.totalDuration));
                                }
                            }
                        },
                        {data: 'totalInputBytes', render: formatBytes},
                        {data: 'totalShuffleRead', render: formatBytes},
                        {data: 'totalShuffleWrite', render: formatBytes},
                        {data: 'executorLogs', render: formatLogsCells},
                        {
                            data: 'id', render: function (data, type) {
                                return type === 'display' ? ("<a href='threadDump/?executorId=" + data + "'>Thread Dump</a>" ) : data;
                            }
                        }
                    ],
                    "columnDefs": [
                        {
                            "targets": [ 16 ],
                            "visible": getThreadDumpEnabled()
                        }
                    ],
                    "order": [[0, "asc"]]
                };
    
                var dt = $(selector).DataTable(conf);
                dt.column(15).visible(logsExist(response));
                $('#active-executors [data-toggle="tooltip"]').tooltip();
    
                var sumSelector = "#summary-execs-table";
                var sumConf = {
                    "data": [activeSummary, deadSummary, totalSummary],
                    "columns": [
                        {
                            data: 'execCnt',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).css('font-weight', 'bold');
                            }
                        },
                        {data: 'allRDDBlocks'},
                        {
                            data: function (row, type) {
                                return type === 'display' ? (formatBytes(row.allMemoryUsed, type) + ' / ' + formatBytes(row.allMaxMemory, type)) : row.allMemoryUsed;
                            }
                        },
                        {data: 'allDiskUsed', render: formatBytes},
                        {data: 'allTotalCores'},
                        {
                            data: 'allActiveTasks',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if (sData > 0) {
                                    $(nTd).css('color', 'white');
                                    $(nTd).css('background', activeTasksStyle(oData.allActiveTasks, oData.allMaxTasks));
                                }
                            }
                        },
                        {
                            data: 'allFailedTasks',
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
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
                                return type === 'display' ? (formatDuration(row.allTotalDuration, type) + ' (' + formatDuration(row.allTotalGCTime, type) + ')') : row.allTotalDuration
                            },
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                if (oData.allTotalDuration > 0) {
                                    $(nTd).css('color', totalDurationColor(oData.allTotalGCTime, oData.allTotalDuration));
                                    $(nTd).css('background', totalDurationStyle(oData.allTotalGCTime, oData.allTotalDuration));
                                }
                            }
                        },
                        {data: 'allTotalInputBytes', render: formatBytes},
                        {data: 'allTotalShuffleRead', render: formatBytes},
                        {data: 'allTotalShuffleWrite', render: formatBytes}
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
});
