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

import {
  formatDuration, formatDate, getStandAloneAppId, setDataTableDefaults
} from "../utils.js";

function createRESTEndPointForSQLTab(appId) {
  var words = document.baseURI.split("/");
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join("/");
    return newBaseURI + "/api/v1/applications/" + appId +
      "/sql/?details=false&planDescription=false&offset=0&length=10000";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join("/");
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId +
        "/sql/?details=false&planDescription=false&offset=0&length=10000";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" + attemptId +
        "/sql/?details=false&planDescription=false&offset=0&length=10000";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId +
    "/sql/?details=false&planDescription=false&offset=0&length=10000";
}

function statusBadge(status) {
  var cls = "bg-secondary";
  if (status === "COMPLETED") cls = "bg-success";
  else if (status === "RUNNING") cls = "bg-primary";
  else if (status === "FAILED") cls = "bg-danger";
  return '<span class="badge ' + cls + '">' + status + '</span>';
}

function jobIdsHtml(runningIds, successIds, failedIds) {
  var links = [];
  var basePath = uiRoot + appBasePath;
  runningIds.forEach(function (id) {
    links.push('<a href="' + basePath + '/jobs/job/?id=' + id + '">' + id + '</a>');
  });
  successIds.forEach(function (id) {
    links.push('<a href="' + basePath + '/jobs/job/?id=' + id + '">' + id + '</a>');
  });
  failedIds.forEach(function (id) {
    links.push('<a href="' + basePath + '/jobs/job/?id=' + id + '">' + id + '</a>');
  });
  return links.join(", ");
}

function descriptionHtml(exec) {
  var desc = exec.description || "";
  var basePath = uiRoot + appBasePath;
  var url = basePath + "/SQL/execution/?id=" + exec.id;
  if (desc.length > 100) {
    var short = desc.substring(0, 100) + "...";
    return '<a href="' + url + '" title="' + desc.replace(/"/g, "&quot;") + '">' +
      short + '</a>';
  }
  return '<a href="' + url + '">' + (desc || exec.id) + '</a>';
}

$.fn.dataTable.ext.search.push(function (settings, data) {
  if (settings.nTable.id !== "sql-table") return true;
  var sel = $("#status-filter").val();
  return !sel || data[1] === sel;
});

$(document).ready(function () {
  setDataTableDefaults();

  getStandAloneAppId(function (appId) {
    var endPoint = createRESTEndPointForSQLTab(appId);
    $.getJSON(endPoint, function (response) {
      var tableData = response.map(function (exec) {
        var allJobIds = (exec.runningJobIds || [])
          .concat(exec.successJobIds || [])
          .concat(exec.failedJobIds || []);
        return [
          exec.id,
          exec.status,
          exec.description || "",
          exec.submissionTime || "",
          exec.duration,
          allJobIds,
          exec.runningJobIds || [],
          exec.successJobIds || [],
          exec.failedJobIds || []
        ];
      });

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
        data: tableData,
        columns: [
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
              return formatDate(data);
            }
          },
          {
            title: "Duration",
            render: function (data, type) {
              if (type !== "display") return data;
              return formatDuration(data);
            }
          },
          {
            title: "Job IDs",
            orderable: false,
            render: function (_ignored_data, type, row) {
              if (type !== "display") return row[5].join(",");
              return jobIdsHtml(row[6], row[7], row[8]);
            }
          }
        ],
        order: [[0, "desc"]],
        pageLength: 20,
        deferRender: true,
        language: { search: "Search:&#160;" }
      });

      $("#status-filter").on("change", function () {
        table.draw();
      });
    });
  });
});
