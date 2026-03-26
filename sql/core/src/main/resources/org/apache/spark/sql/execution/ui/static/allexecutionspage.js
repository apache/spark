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

/* global $, uiRoot, appBasePath */
/* eslint-disable no-unused-vars */

function formatDurationSql(milliseconds) {
  if (milliseconds < 100) return parseInt(milliseconds).toFixed(1) + " ms";
  var seconds = milliseconds / 1000;
  if (seconds < 1) return seconds.toFixed(1) + " s";
  if (seconds < 60) return seconds.toFixed(0) + " s";
  var minutes = seconds / 60;
  if (minutes < 10) return minutes.toFixed(1) + " min";
  if (minutes < 60) return minutes.toFixed(0) + " min";
  var hours = minutes / 60;
  return hours.toFixed(1) + " h";
}

function formatDateSql(dateStr) {
  if (!dateStr) return "";
  try {
    var dt = new Date(dateStr.replace("GMT", "Z"));
    if (isNaN(dt.getTime())) return dateStr;
    var pad = function(n) { return n < 10 ? "0" + n : n; };
    return dt.getFullYear() + "-" + pad(dt.getMonth() + 1) + "-" + pad(dt.getDate()) + " " +
      pad(dt.getHours()) + ":" + pad(dt.getMinutes()) + ":" + pad(dt.getSeconds());
  } catch (e) { return dateStr; }
}
function escapeHtml(str) {
  if (!str) return str;
  var div = document.createElement("div");
  div.appendChild(document.createTextNode(str));
  return div.innerHTML;
}
/* eslint-enable no-unused-vars */

function createRESTEndPointForSQLTab(appId) {
  var words = document.baseURI.split("/");
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join("/");
    return newBaseURI + "/api/v1/applications/" + appId +
      "/sql/?details=false&planDescription=false";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join("/");
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId +
        "/sql/?details=false&planDescription=false";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId +
        "/sql/?details=false&planDescription=false";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId +
    "/sql/?details=false&planDescription=false";
}

function statusBadge(status) {
  var cls = "bg-secondary";
  if (status === "COMPLETED") cls = "bg-success";
  else if (status === "RUNNING") cls = "bg-primary";
  else if (status === "FAILED") cls = "bg-danger";
  return '<span class="badge ' + cls + '">' + status + '</span>';
}

function jobIdLinks(ids) {
  if (!ids || ids.length === 0) return "";
  var basePath = uiRoot + appBasePath;
  return ids.map(function (id) {
    return '<a href="' + basePath + '/jobs/job/?id=' + id + '">' + id + '</a>';
  }).join(", ");
}

function descriptionHtml(exec) {
  var desc = exec.description || "";
  var basePath = uiRoot + appBasePath;
  var url = basePath + "/SQL/execution/?id=" + exec.id;
  if (desc.length > 100) {
    var short = escapeHtml(desc.substring(0, 100)) + "...";
    return '<a href="' + url + '" title="' + escapeHtml(desc) + '">' +
      short + '</a>';
  }
  return '<a href="' + url + '">' + (escapeHtml(desc) || exec.id) + '</a>';
}

$.fn.dataTable.ext.search.push(function (settings, data) {
  if (settings.nTable.id !== "sql-table") return true;
  var sel = $("#status-filter").val();
  return !sel || data[2] === sel;
});

$(document).ready(function () {
  // Resolve appId: check proxy/history in URL, fallback to REST API
  var words = document.baseURI.split("/");
  var appId = "";
  var ind = words.indexOf("proxy");
  if (ind > 0) {
    appId = words[ind + 1];
  } else {
    ind = words.indexOf("history");
    if (ind > 0) {
      appId = words[ind + 1];
    }
  }

  function init(resolvedAppId) {
    var endPoint = createRESTEndPointForSQLTab(resolvedAppId);

    var groupSubExecEnabled = true;
    var configEl = document.getElementById("group-sub-exec-config");
    if (configEl) {
      groupSubExecEnabled = configEl.getAttribute("data-value") === "true";
    }

    $.getJSON(endPoint, function (response) {
      var tableData = response.map(function (exec) {
        return [
          exec.id,
          exec.queryId || "",
          exec.status,
          exec.description || "",
          exec.submissionTime || "",
          exec.duration,
          exec.runningJobIds || [],
          exec.successJobIds || [],
          exec.failedJobIds || [],
          exec.errorMessage || "",
          exec.rootExecutionId
        ];
      });

      // Group sub-executions under their root execution (when enabled)
      var subExecMap = {};
      var rootRows = [];
      if (groupSubExecEnabled) {
        tableData.forEach(function (row) {
          var id = row[0];
          var rootId = row[10];
          if (rootId !== id && subExecMap[rootId] !== undefined) {
            subExecMap[rootId].push(row);
          } else {
            subExecMap[id] = [];
            rootRows.push(row);
          }
        });
        // Second pass: attach orphaned sub-executions that appeared before their root
        tableData.forEach(function (row) {
          var id = row[0];
          var rootId = row[10];
          if (rootId !== id && subExecMap[rootId] !== undefined
            && subExecMap[rootId].indexOf(row) === -1) {
            subExecMap[rootId].push(row);
          }
        });
      } else {
        rootRows = tableData;
      }

      var container = document.getElementById("sql-executions-table");
      container.innerHTML =
      '<select id="status-filter" class="form-select form-select-sm ' +
      'd-inline-block w-auto mb-2">' +
      '<option value="">All Statuses</option>' +
      '<option value="RUNNING">Running</option>' +
      '<option value="COMPLETED">Completed</option>' +
      '<option value="FAILED">Failed</option>' +
      '</select>' +
      '<table id="sql-table" class="table table-striped compact cell-border" ' +
      'style="width:100%"></table>';

      var columns = [
        {
          title: "ID",
          render: function (data, type) {
            if (type !== "display") return data;
            var basePath = uiRoot + appBasePath;
            return '<a href="' + basePath + '/SQL/execution/?id=' + data + '">' +
              data + '</a>';
          }
        },
        {
          title: "Query ID",
          render: function (data, type) {
            if (type !== "display" || !data) return data;
            return '<span title="' + data + '">' + data.substring(0, 8) + '...</span>';
          }
        },
        {
          title: "Status",
          render: function (data, type) {
            if (type !== "display") return data;
            return statusBadge(data);
          }
        },
        {
          title: "Description",
          render: function (data, type, row) {
            if (type !== "display") return data;
            return descriptionHtml({ id: row[0], description: data });
          }
        },
        {
          title: "Submitted",
          render: function (data, type) {
            if (type !== "display") return data;
            return formatDateSql(data);
          }
        },
        {
          title: "Duration",
          render: function (data, type) {
            if (type !== "display") return data;
            return formatDurationSql(data);
          }
        },
        {
          title: "Running Jobs",
          orderable: false,
          render: function (data, type) {
            if (type !== "display") return data.join(",");
            return jobIdLinks(data);
          }
        },
        {
          title: "Succeeded Jobs",
          orderable: false,
          render: function (data, type) {
            if (type !== "display") return data.join(",");
            return jobIdLinks(data);
          }
        },
        {
          title: "Failed Jobs",
          orderable: false,
          render: function (data, type) {
            if (type !== "display") return data.join(",");
            return jobIdLinks(data);
          }
        },
        {
          title: "Error Message",
          render: function (data, type) {
            if (type !== "display" || !data) return data;
            if (data.length > 100) {
              return '<span title="' + escapeHtml(data) + '">' +
                escapeHtml(data.substring(0, 100)) + '...</span>';
            }
            return escapeHtml(data);
          }
        }
      ];

      if (groupSubExecEnabled) {
        columns.push({
          title: "Sub Executions",
          orderable: false,
          render: function (_data, type, row) {
            var children = subExecMap[row[0]] || [];
            if (type !== "display" || children.length === 0) return children.length || "";
            return '<a href="#" class="toggle-sub-exec">' +
              '+' + children.length + ' sub</a>';
          }
        });
      }

      var table = $("#sql-table").DataTable({
        data: rootRows,
        columns: columns,
        order: [[0, "desc"]],
        pageLength: 20,
        deferRender: true,
        language: { search: "Search:&#160;" }
      });

      // Child row expansion for sub-executions
      $("#sql-table tbody").on("click", "a.toggle-sub-exec", function (e) {
        e.preventDefault();
        var tr = $(this).closest("tr");
        var dtRow = table.row(tr);
        if (dtRow.child.isShown()) {
          dtRow.child.hide();
          tr.removeClass("shown");
        } else {
          var parentId = dtRow.data()[0];
          var children = subExecMap[parentId] || [];
          var html = '<table class="table table-sm table-bordered mb-0 ms-4">';
          html += '<thead><tr><th>ID</th><th>Status</th><th>Description</th>' +
          '<th>Duration</th><th>Succeeded Jobs</th></tr></thead><tbody>';
          children.forEach(function (child) {
            var basePath = uiRoot + appBasePath;
            html += '<tr><td><a href="' + basePath + '/SQL/execution/?id=' + child[0] +
            '">' + child[0] + '</a></td>';
            html += '<td>' + statusBadge(child[2]) + '</td>';
            html += '<td>' + escapeHtml(child[3] || '') + '</td>';
            html += '<td>' + formatDurationSql(child[5]) + '</td>';
            html += '<td>' + jobIdLinks(child[7]) + '</td></tr>';
          });
          html += '</tbody></table>';
          dtRow.child(html).show();
          tr.addClass("shown");
        }
      });

      $("#status-filter").on("change", function () {
        table.draw();
      });
    });

  } // end init

  if (appId) {
    init(appId);
  } else {
    // Standalone mode: fetch appId from REST API
    $.getJSON(uiRoot + "/api/v1/applications", function (response) {
      if (response && response.length > 0) {
        init(response[0].id);
      }
    });
  }
});
