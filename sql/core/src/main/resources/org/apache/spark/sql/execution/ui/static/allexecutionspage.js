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

/* global $, uiRoot, appBasePath, createSqlApiBase, getSqlTableColumns,
   withResolvedAppId, statusBadge, jobIdLinks, formatDurationSql,
   descriptionHtml */

$(document).ready(function () {
  // Read the cluster-level grouping toggle rendered into the page by Scala
  var groupSubExecEnabled = true;
  var configEl = document.getElementById("group-sub-exec-config");
  if (configEl) {
    groupSubExecEnabled = configEl.getAttribute("data-value") === "true";
  }

  function init(resolvedAppId) {
    var sqlTableEndPoint = createSqlApiBase(resolvedAppId) + "/sqlTable";

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

    var columns = getSqlTableColumns({ detail: false });
    if (groupSubExecEnabled) {
      // Trailing "Sub Executions" column matching the SPARK-41752 / 4.1 layout:
      // shows "+N sub" when the root has children, blank otherwise. Click to
      // expand a child row containing the sub-execution rows.
      columns.push({
        data: null, name: "subExecutions", title: "Sub Executions",
        orderable: false, searchable: false,
        className: "sub-exec-toggle",
        render: function (data, type, row) {
          if (type !== "display") return "";
          var subs = row.subExecutions || [];
          if (subs.length === 0) return "";
          var childId = "sub-exec-" + row.id;
          return '<a href="#" class="toggle-sub-exec" role="button" ' +
            'aria-expanded="false" aria-controls="' + childId + '">' +
            '+' + subs.length + ' sub</a>';
        }
      });
    }

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
          d.groupSubExecution = groupSubExecEnabled ? "true" : "false";
        },
        dataSrc: function (json) { return json.aaData; },
        error: function () {
          $("#sql-table_processing").css("display", "none");
        }
      },
      columns: columns,
      order: [[0, "desc"]],
      language: { search: "Search:&#160;" }
    });

    // Child-row expansion for sub-executions. Sub data is embedded per root row
    // in the server payload (`row.subExecutions`), so no second fetch is needed.
    // Under serverSide: true DataTables destroys/recreates rows on every sort,
    // filter or page change, so we track expanded row IDs out-of-band and
    // re-attach the child on each draw.
    if (groupSubExecEnabled) {
      var expandedRowIds = {};

      var renderSubExecutionsHtml = function (rowData) {
        var subs = (rowData && rowData.subExecutions) || [];
        var basePath = uiRoot + appBasePath;
        var childId = "sub-exec-" + (rowData && rowData.id);
        var html = '<table id="' + childId +
          '" class="table table-sm table-bordered mb-0 sub-exec-table">';
        html += '<thead><tr><th>ID</th><th>Status</th><th>Description</th>' +
          '<th>Duration</th><th>Succeeded Jobs</th></tr></thead><tbody>';
        subs.forEach(function (child) {
          html += '<tr><td><a href="' + basePath + '/SQL/execution/?id=' +
            child.id + '">' + child.id + '</a></td>';
          html += '<td>' + statusBadge(child.status) + '</td>';
          html += '<td>' + descriptionHtml({
            id: child.id, description: child.description || ""
          }) + '</td>';
          html += '<td>' + formatDurationSql(child.duration) + '</td>';
          html += '<td>' + jobIdLinks(child.jobIds || []) + '</td></tr>';
        });
        html += '</tbody></table>';
        return html;
      };

      $("#sql-table tbody").on("click", "a.toggle-sub-exec", function (e) {
        e.preventDefault();
        var tr = $(this).closest("tr");
        var dtRow = table.row(tr);
        var rowData = dtRow.data();
        var subs = (rowData && rowData.subExecutions) || [];
        if (dtRow.child.isShown()) {
          dtRow.child.hide();
          tr.removeClass("shown");
          $(this).text("+" + subs.length + " sub").attr("aria-expanded", "false");
          delete expandedRowIds[rowData.id];
        } else {
          dtRow.child(renderSubExecutionsHtml(rowData)).show();
          tr.addClass("shown");
          $(this).text("\u2212" + subs.length + " sub").attr("aria-expanded", "true");
          expandedRowIds[rowData.id] = true;
        }
      });

      table.on("draw", function () {
        $("#sql-table tbody > tr").each(function () {
          var dtRow = table.row(this);
          var data = dtRow.data();
          if (data && expandedRowIds[data.id]) {
            var subs = data.subExecutions || [];
            dtRow.child(renderSubExecutionsHtml(data)).show();
            $(this).addClass("shown");
            $(this).find("a.toggle-sub-exec")
              .text("\u2212" + subs.length + " sub")
              .attr("aria-expanded", "true");
          }
        });
      });
    }

    $("#status-filter").on("change", function () {
      table.draw();
    });

  } // end init

  withResolvedAppId(init);
});
