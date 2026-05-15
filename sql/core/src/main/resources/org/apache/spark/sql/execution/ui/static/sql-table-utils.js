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
    var pad = function (n) { return n < 10 ? "0" + n : n; };
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

// Render a description cell.
//   exec   - {id, description}
//   opts.detail - if true: render full text in <pre class="sql-cell-pre">,
//                 no truncation, no self-link. Used by the SQL detail page.
//                 Multi-line / long descriptions are wrapped in <details>
//                 so the cell starts collapsed with a one-line summary.
//                 if false (default): truncate to 100 chars, render as a
//                 link to the execution detail page. Used by the list page
//                 and by sub-execution child rows.
function descriptionHtml(exec, opts) {
  opts = opts || {};
  var desc = exec.description || "";
  if (opts.detail) {
    if (!desc) {
      return '<span class="text-muted">(no description)</span>';
    }
    return collapsiblePre(desc);
  }
  var basePath = uiRoot + appBasePath;
  var url = basePath + "/SQL/execution/?id=" + exec.id;
  if (desc.length > 100) {
    var short = escapeHtml(desc.substring(0, 100)) + "...";
    return '<a href="' + url + '" title="' + escapeHtml(desc) + '">' +
      short + '</a>';
  }
  return '<a href="' + url + '">' + (escapeHtml(desc) || exec.id) + '</a>';
}

// Render a long, possibly multi-line value as either a plain <pre> when it
// fits one short line, or a <details>/<summary>/<pre> disclosure block
// otherwise. The summary shows the first 100 characters of the first line so
// the cell stays compact when collapsed.
function collapsiblePre(text) {
  var firstLineBreak = text.indexOf("\n");
  var firstLine = firstLineBreak >= 0 ? text.substring(0, firstLineBreak) : text;
  var multiLine = firstLineBreak >= 0;
  if (!multiLine && text.length <= 100) {
    return '<pre class="sql-cell-pre">' + escapeHtml(text) + '</pre>';
  }
  var summary = firstLine.length > 100 ?
    firstLine.substring(0, 100) + "..." :
    firstLine + (multiLine ? "  ..." : "");
  return '<details class="sql-cell-details">' +
    '<summary>' + escapeHtml(summary) + '</summary>' +
    '<pre class="sql-cell-pre">' + escapeHtml(text) + '</pre>' +
    '</details>';
}

// Resolve the Spark applicationId for the current page, then invoke
// callback(appId). Checks /proxy/<id> and /history/<id> path prefixes first;
// falls back to the REST list endpoint for the local-mode UI.
function withResolvedAppId(callback) {
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
  if (appId) {
    callback(appId);
    return;
  }
  $.getJSON(uiRoot + "/api/v1/applications", function (response) {
    if (response && response.length > 0) {
      callback(response[0].id);
    }
  });
}

// Build the base URL for SQL REST endpoints, accounting for proxy/history paths.
// Returns "<baseURI>/api/v1/applications/<resolvedAppId>/sql"; the caller can
// append "/sqlTable", "/<id>", etc.
function createSqlApiBase(appId) {
  var words = document.baseURI.split("/");
  var ind = words.indexOf("proxy");
  var newBaseURI;
  if (ind > 0) {
    appId = words[ind + 1];
    newBaseURI = words.slice(0, ind + 2).join("/");
    return newBaseURI + "/api/v1/applications/" + appId + "/sql";
  }
  ind = words.indexOf("history");
  if (ind > 0) {
    appId = words[ind + 1];
    var attemptId = words[ind + 2];
    newBaseURI = words.slice(0, ind).join("/");
    if (isNaN(attemptId)) {
      return newBaseURI + "/api/v1/applications/" + appId + "/sql";
    } else {
      return newBaseURI + "/api/v1/applications/" + appId + "/" +
        attemptId + "/sql";
    }
  }
  return uiRoot + "/api/v1/applications/" + appId + "/sql";
}

// Factory for the shared SQL DataTable column definitions used on both the
// SQL listing page (`allexecutionspage.js`) and the SQL execution detail page
// (`executionpage.js`).
//   opts.detail - true on the detail page (single-row table): no truncation,
//                 ID and Description rendered as plain text (no self-link),
//                 description rendered as <pre> so SQL formatting is kept.
//                 false on the list page (default): truncate long values,
//                 ID and Description link to the detail page.
function getSqlTableColumns(opts) {
  opts = opts || {};
  var detail = opts.detail === true;

  var idColumn = {
    data: "id", name: "id", title: "ID",
    render: function (data, type) {
      if (type !== "display") return data;
      if (detail) return data;
      var basePath = uiRoot + appBasePath;
      return '<a href="' + basePath + '/SQL/execution/?id=' + data + '">' +
        data + '</a>';
    }
  };

  var queryIdColumn = {
    data: "queryId", name: "queryId", title: "Query ID",
    orderable: false,
    render: function (data, type) {
      if (type !== "display" || !data) return data || "";
      var safe = escapeHtml(data);
      if (detail) return safe;
      return '<span title="' + safe + '">' +
        escapeHtml(data.substring(0, 8)) + '...</span>';
    }
  };

  var statusColumn = {
    data: "status", name: "status", title: "Status",
    render: function (data, type) {
      if (type !== "display") return data;
      return statusBadge(data);
    }
  };

  var descriptionColumn = {
    data: "description", name: "description", title: "Description",
    render: function (data, type, row) {
      if (type !== "display") return data || "";
      return descriptionHtml({ id: row.id, description: data }, { detail: detail });
    }
  };

  var submissionColumn = {
    data: "submissionTime", name: "submissionTime", title: "Submitted",
    render: function (data, type) {
      if (type !== "display") return data;
      return formatDateSql(data);
    }
  };

  var durationColumn = {
    data: "duration", name: "duration", title: "Duration",
    render: function (data, type) {
      if (type !== "display") return data;
      return formatDurationSql(data);
    }
  };

  var jobsColumn = {
    data: "jobIds", name: "jobIds", title: "Succeeded Jobs",
    orderable: false,
    render: function (data, type) {
      if (type !== "display") return (data || []).join(",");
      return jobIdLinks(data || []);
    }
  };

  var errorColumn = {
    data: "errorMessage", name: "errorMessage", title: "Error Message",
    orderable: false,
    render: function (data, type) {
      if (type !== "display" || !data) return data || "";
      if (detail) {
        return collapsiblePre(data);
      }
      if (data.length > 100) {
        return '<span title="' + escapeHtml(data) + '">' +
          escapeHtml(data.substring(0, 100)) + '...</span>';
      }
      return escapeHtml(data);
    }
  };

  return [idColumn, queryIdColumn, statusColumn, descriptionColumn,
    submissionColumn, durationColumn, jobsColumn, errorColumn];
}
