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

/* global $, createSqlApiBase, getSqlTableColumns, withResolvedAppId */

// Renders the single-row summary table at the top of the SQL execution detail
// page using the same column layout as the SQL listing page. Fetches the
// execution data from the v1 API and feeds it into a one-row DataTable.
$(document).ready(function () {
  var tableEl = document.getElementById("sql-execution-table");
  if (!tableEl) return;

  var executionId = tableEl.getAttribute("data-execution-id");
  if (!executionId) return;

  function init(resolvedAppId) {
    var endpoint = createSqlApiBase(resolvedAppId) + "/" + executionId +
      "?details=false&planDescription=false";
    $.getJSON(endpoint, function (data) {
      // ExecutionData fields: id, status, description, submissionTime, duration,
      //   runningJobIds, successJobIds, failedJobIds, queryId, errorMessage,
      //   rootExecutionId. Map to the row shape consumed by getSqlTableColumns:
      //   id, queryId, status, description, submissionTime, duration, jobIds,
      //   errorMessage.
      var row = {
        id: data.id,
        queryId: data.queryId || "",
        status: data.status,
        description: data.description || "",
        submissionTime: data.submissionTime,
        duration: data.duration,
        jobIds: data.successJobIds || [],
        errorMessage: data.errorMessage || ""
      };

      $("#sql-execution-table").DataTable({
        data: [row],
        columns: getSqlTableColumns({ detail: true }),
        paging: false,
        searching: false,
        info: false,
        ordering: false,
        dom: "t"
      });
    }).fail(function () {
      $("#sql-execution-table").replaceWith(
        '<div class="alert alert-warning">' +
        'Failed to load execution metadata.</div>');
    });
  }

  withResolvedAppId(init);
});
