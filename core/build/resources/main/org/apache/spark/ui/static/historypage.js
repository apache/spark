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

/* global $, Mustache, jQuery, uiRoot */

import {formatDuration, formatTimeMillis, stringAbbreviate} from "./utils.js";

export {setAppLimit};

var appLimit = -1;

/* eslint-disable no-unused-vars */
function setAppLimit(val) {
  appLimit = val;
}
/* eslint-enable no-unused-vars*/

function makeIdNumeric(id) {
  var strs = id.split("_");
  if (strs.length < 3) {
    return id;
  }
  var appSeqNum = strs[2];
  var resl = strs[0] + "_" + strs[1] + "_";
  var diff = 10 - appSeqNum.length;
  while (diff > 0) {
    resl += "0"; // padding 0 before the app sequence number to make sure it has 10 characters
    diff--;
  }
  resl += appSeqNum;
  return resl;
}

function getParameterByName(name, searchString) {
  var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
    results = regex.exec(searchString);
  return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

function removeColumnByName(columns, columnName) {
  return columns.filter(function(col) {return col.name != columnName})
}

function getColumnIndex(columns, columnName) {
  for(var i = 0; i < columns.length; i++) {
    if (columns[i].name == columnName)
      return i;
  }
  return -1;
}

jQuery.extend( jQuery.fn.dataTableExt.oSort, {
  "title-numeric-pre": function ( a ) {
    var x = a.match(/title="*(-?[0-9.]+)/)[1];
    return parseFloat( x );
  },

  "title-numeric-asc": function ( a, b ) {
    return ((a < b) ? -1 : ((a > b) ? 1 : 0));
  },

  "title-numeric-desc": function ( a, b ) {
    return ((a < b) ? 1 : ((a > b) ? -1 : 0));
  }
});

jQuery.extend( jQuery.fn.dataTableExt.oSort, {
  "appid-numeric-pre": function ( a ) {
    var x = a.match(/title="*(-?[0-9a-zA-Z\-_]+)/)[1];
    return makeIdNumeric(x);
  },

  "appid-numeric-asc": function ( a, b ) {
    return ((a < b) ? -1 : ((a > b) ? 1 : 0));
  },

  "appid-numeric-desc": function ( a, b ) {
    return ((a < b) ? 1 : ((a > b) ? -1 : 0));
  }
});

jQuery.extend( jQuery.fn.dataTableExt.ofnSearch, {
  "appid-numeric": function ( a ) {
    return a.replace(/[\r\n]/g, " ").replace(/<.*?>/g, "");
  }
});

$(document).ajaxStop($.unblockUI);
$(document).ajaxStart(function(){
  $.blockUI({ message: '<h3>Loading history summary...</h3>'});
});

$(document).ready(function() {
  $.extend( $.fn.dataTable.defaults, {
    stateSave: true,
    lengthMenu: [[20,40,60,100,-1], [20, 40, 60, 100, "All"]],
    pageLength: 20
  });

  var historySummary = $("#history-summary");
  var searchString = window.location.search;
  var requestedIncomplete = getParameterByName("showIncomplete", searchString);
  requestedIncomplete = (requestedIncomplete == "true" ? true : false);

  var appParams = {
    limit: appLimit,
    status: (requestedIncomplete ? "running" : "completed")
  };

  $.getJSON(uiRoot + "/api/v1/applications", appParams, function(response, _ignored_status, _ignored_jqXHR) {
    var array = [];
    var hasMultipleAttempts = false;
    for (var i in response) {
      var app = response[i];
      if (app["attempts"][0]["completed"] == requestedIncomplete) {
        continue; // if we want to show for Incomplete, we skip the completed apps; otherwise skip incomplete ones.
      }
      var version = "Unknown"
      if (app["attempts"].length > 0) {
        version = app["attempts"][0]["appSparkVersion"]
      }
      var id = app["id"];
      var name = app["name"];
      if (app["attempts"].length > 1) {
        hasMultipleAttempts = true;
      }

      // TODO: Replace hasOwnProperty with prototype.hasOwnProperty after we find it's safe to do.
      /* eslint-disable no-prototype-builtins */
      for (var j in app["attempts"]) {
        var attempt = app["attempts"][j];
        attempt["startTime"] = formatTimeMillis(attempt["startTimeEpoch"]);
        attempt["endTime"] = formatTimeMillis(attempt["endTimeEpoch"]);
        attempt["lastUpdated"] = formatTimeMillis(attempt["lastUpdatedEpoch"]);
        attempt["log"] = uiRoot + "/api/v1/applications/" + id + "/" +
          (attempt.hasOwnProperty("attemptId") ? attempt["attemptId"] + "/" : "") + "logs";
        attempt["durationMillisec"] = attempt["duration"];
        attempt["duration"] = formatDuration(attempt["duration"]);
        attempt["id"] = id;
        attempt["name"] = name;
        attempt["version"] = version;
        attempt["attemptUrl"] = uiRoot + "/history/" + id + "/" +
          (attempt.hasOwnProperty("attemptId") ? attempt["attemptId"] + "/" : "") + "jobs/";
        array.push(attempt);
      }
      /* eslint-enable no-prototype-builtins */
    }
    if(array.length < 20) {
      $.fn.dataTable.defaults.paging = false;
    }

    var data = {
      "uiroot": uiRoot,
      "applications": array,
      "hasMultipleAttempts": hasMultipleAttempts,
      "showCompletedColumns": !requestedIncomplete,
    };

    $.get(uiRoot + "/static/historypage-template.html", function(template) {
      var sibling = historySummary.prev();
      historySummary.detach();
      var apps = $(Mustache.render($(template).filter("#history-summary-template").html(),data));
      var attemptIdColumnName = 'attemptId';
      var startedColumnName = 'started';
      var completedColumnName = 'completed';
      var durationColumnName = 'duration';
      var conf = {
        "data": array,
        "columns": [
          {name: 'version', data: 'version' },
          {
            name: 'appId',
            type: "appid-numeric",
            data: 'id',
            render: (id, type, row) => `<span title="${id}"><a href="${row.attemptUrl}">${id}</a></span>`
          },
          {
            name: 'appName',
            data: 'name',
            render: (name) => stringAbbreviate(name, 60)
          },
          {
            name: attemptIdColumnName,
            data: 'attemptId',
            render: (attemptId, type, row) => (attemptId ? `<a href="${row.attemptUrl}">${attemptId}</a>` : '')
          },
          {name: startedColumnName, data: 'startTime' },
          {name: completedColumnName, data: 'endTime' },
          {
            name: durationColumnName,
            type: "title-numeric",
            data: 'duration',
            render: (id, type, row) => `<span title="${row.durationMillisec}">${row.duration}</span>`
          },
          {name: 'user', data: 'sparkUser' },
          {name: 'lastUpdated', data: 'lastUpdated' },
          {
            name: 'eventLog',
            data: 'log',
            render: (log, _ignored_type, _ignored_row) => `<a href="${log}" class="btn btn-info btn-mini">Download</a>`
          },
        ],
        "aoColumnDefs": [
          {
            aTargets: [0, 1, 2],
            fnCreatedCell: (nTd, _ignored_sData, _ignored_oData, _ignored_iRow, _ignored_iCol) => {
              if (hasMultipleAttempts) {
                $(nTd).css('background-color', '#fff');
              }
            }
          },
        ],
        "autoWidth": false,
        "deferRender": true
      };

      if (hasMultipleAttempts) {
        conf.rowsGroup = [
          'appId:name',
          'version:name',
          'appName:name'
        ];
      } else {
        conf.columns = removeColumnByName(conf.columns, attemptIdColumnName);
      }

      var defaultSortColumn = completedColumnName;
      if (requestedIncomplete) {
        defaultSortColumn = startedColumnName;
        conf.columns = removeColumnByName(conf.columns, completedColumnName);
        conf.columns = removeColumnByName(conf.columns, durationColumnName);
      }
      conf.order = [[ getColumnIndex(conf.columns, defaultSortColumn), "desc" ]];
      conf.columnDefs = [
        {"searchable": false, "targets": [getColumnIndex(conf.columns, durationColumnName)]}
      ];
      historySummary.append(apps);
      apps.DataTable(conf);
      sibling.after(historySummary);
      $('#history-summary [data-toggle="tooltip"]').tooltip();
    });
  });
});
