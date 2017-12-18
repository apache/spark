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
            var allTotalBlacklisted = 0;

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
            var activeTotalBlacklisted = 0;

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
            var deadTotalBlacklisted = 0;

            response.forEach(function (exec) {
                var memoryMetrics = {
                    usedOnHeapStorageMemory: 0,
                    usedOffHeapStorageMemory: 0,
                    totalOnHeapStorageMemory: 0,
                    totalOffHeapStorageMemory: 0
                };

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
                allTotalBlacklisted += exec.isBlacklisted ? 1 : 0;
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
                    activeTotalBlacklisted += exec.isBlacklisted ? 1 : 0;
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
                    deadTotalBlacklisted += exec.isBlacklisted ? 1 : 0;
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
                "allTotalBlacklisted": allTotalBlacklisted
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
                "allTotalBlacklisted": activeTotalBlacklisted
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
                "allTotalBlacklisted": deadTotalBlacklisted
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
                        {data: 'isActive', render: function (data, type, row) {
                            if (row.isBlacklisted) return "Blacklisted";
                            else return formatStatus (data, type);
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
                            },
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).addClass('on_heap_memory')
                            }
                        },
                        {
                            data: function (row, type) {
                                if (type !== 'display')
                                    return row.memoryMetrics.usedOffHeapStorageMemory;
                                else
                                    return (formatBytes(row.memoryMetrics.usedOffHeapStorageMemory, type) + ' / ' +
                                        formatBytes(row.memoryMetrics.totalOffHeapStorageMemory, type));
                            },
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).addClass('off_heap_memory')
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
                        {name: 'executorLogsCol', data: 'executorLogs', render: formatLogsCells},
                        {
                            name: 'threadDumpCol',
                            data: 'id', render: function (data, type) {
                                return type === 'display' ? ("<a href='threadDump/?executorId=" + data + "'>Thread Dump</a>" ) : data;
                            }
                        }
                    ],
                    "order": [[0, "asc"]]
                };
    
                var dt = $(selector).DataTable(conf);
                dt.column('executorLogsCol:name').visible(logsExist(response));
                dt.column('threadDumpCol:name').visible(getThreadDumpEnabled());
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
                                if (type !== 'display')
                                    return row.allMemoryUsed
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
                            },
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).addClass('on_heap_memory')
                            }
                        },
                        {
                            data: function (row, type) {
                                if (type !== 'display')
                                    return row.allOffHeapMemoryUsed;
                                else
                                    return (formatBytes(row.allOffHeapMemoryUsed, type) + ' / ' +
                                        formatBytes(row.allOffHeapMaxMemory, type));
                            },
                            "fnCreatedCell": function (nTd, sData, oData, iRow, iCol) {
                                $(nTd).addClass('off_heap_memory')
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
                        {data: 'allTotalShuffleWrite', render: formatBytes},
                        {data: 'allTotalBlacklisted'}
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
