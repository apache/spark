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

function createSQLTableEndPoint(appId) {
  var words = document.baseURI.split("/");
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join("/");
    return newBaseURI + "/api/v1/applications/" + appId + "/sql/sqlTable";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join("/");
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId + "/sql/sqlTable";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" +
        attemptId + "/sql/sqlTable";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId + "/sql/sqlTable";
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

// Remove client-side filter — status filtering is now server-side

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
    var sqlTableEndPoint = createSQLTableEndPoint(resolvedAppId);

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

    var table = $("#sql-table").DataTable({
      serverSide: true,
      processing: true,
      paging: true,
      info: true,
      lengthMenu: [[20, 50, 100, -1], [20, 50, 100, "All"]],
      orderMulti: false,
      ajax: {
        url: sqlTableEndPoint,
        data: function (d) {
          // Pass status filter as custom server-side parameter
          var sel = $("#status-filter").val();
          if (sel) {
            d.status = sel;
          }
        },
        dataSrc: function (json) { return json.aaData; },
        error: function () {
          $("#sql-table_processing").css("display", "none");
        }
      },
      columns: [
        {
          data: "id", name: "id", title: "ID",
          render: function (data, type) {
            if (type !== "display") return data;
            var basePath = uiRoot + appBasePath;
            return '<a href="' + basePath + '/SQL/execution/?id=' + data + '">' +
              data + '</a>';
          }
        },
        {
          data: "queryId", name: "queryId", title: "Query ID",
          orderable: false,
          render: function (data, type) {
            if (type !== "display" || !data) return data || "";
            return '<span title="' + data + '">' + data.substring(0, 8) + '...</span>';
          }
        },
        {
          data: "status", name: "status", title: "Status",
          render: function (data, type) {
            if (type !== "display") return data;
            return statusBadge(data);
          }
        },
        {
          data: "description", name: "description", title: "Description",
          render: function (data, type, row) {
            if (type !== "display") return data || "";
            return descriptionHtml({ id: row.id, description: data });
          }
        },
        {
          data: "submissionTime", name: "submissionTime", title: "Submitted",
          render: function (data, type) {
            if (type !== "display") return data;
            return formatDateSql(data);
          }
        },
        {
          data: "duration", name: "duration", title: "Duration",
          render: function (data, type) {
            if (type !== "display") return data;
            return formatDurationSql(data);
          }
        },
        {
          data: "jobIds", name: "jobIds", title: "Succeeded Jobs",
          orderable: false,
          render: function (data, type) {
            if (type !== "display") return (data || []).join(",");
            return jobIdLinks(data || []);
          }
        },
        {
          data: "errorMessage", name: "errorMessage", title: "Error Message",
          orderable: false,
          render: function (data, type) {
            if (type !== "display" || !data) return data || "";
            if (data.length > 100) {
              return '<span title="' + escapeHtml(data) + '">' +
                escapeHtml(data.substring(0, 100)) + '...</span>';
            }
            return escapeHtml(data);
          }
        }
      ],
      order: [[0, "desc"]],
      language: { search: "Search:&#160;" }
    });

    $("#status-filter").on("change", function () {
      table.draw();
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
