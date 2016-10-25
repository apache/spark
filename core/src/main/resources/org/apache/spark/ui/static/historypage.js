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

var appLimit = -1;

function setAppLimit(val) {
    appLimit = val;
}

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

function formatDate(date) {
  if (date <= 0) return "-";
  else return date.split(".")[0].replace("T", " ");
}

function getParameterByName(name, searchString) {
  var regex = new RegExp("[\\?&]" + name + "=([^&#]*)"),
  results = regex.exec(searchString);
  return results === null ? "" : decodeURIComponent(results[1].replace(/\+/g, " "));
}

jQuery.extend( jQuery.fn.dataTableExt.oSort, {
    "title-numeric-pre": function ( a ) {
        var x = a.match(/title="*(-?[0-9\.]+)/)[1];
        return parseFloat( x );
    },

    "title-numeric-asc": function ( a, b ) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "title-numeric-desc": function ( a, b ) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

jQuery.extend( jQuery.fn.dataTableExt.oSort, {
    "appid-numeric-pre": function ( a ) {
        var x = a.match(/title="*(-?[0-9a-zA-Z\-\_]+)/)[1];
        return makeIdNumeric(x);
    },

    "appid-numeric-asc": function ( a, b ) {
        return ((a < b) ? -1 : ((a > b) ? 1 : 0));
    },

    "appid-numeric-desc": function ( a, b ) {
        return ((a < b) ? 1 : ((a > b) ? -1 : 0));
    }
} );

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

    historySummary = $("#history-summary");
    searchString = historySummary["context"]["location"]["search"];
    requestedIncomplete = getParameterByName("showIncomplete", searchString);
    requestedIncomplete = (requestedIncomplete == "true" ? true : false);

    $.getJSON("api/v1/applications?limit=" + appLimit, function(response,status,jqXHR) {
      var array = [];
      var hasMultipleAttempts = false;
      for (i in response) {
        var app = response[i];
        if (app["attempts"][0]["completed"] == requestedIncomplete) {
          continue; // if we want to show for Incomplete, we skip the completed apps; otherwise skip incomplete ones.
        }
        var id = app["id"];
        var name = app["name"];
        if (app["attempts"].length > 1) {
            hasMultipleAttempts = true;
        }
        var num = app["attempts"].length;
        for (j in app["attempts"]) {
          var attempt = app["attempts"][j];
          attempt["startTime"] = formatDate(attempt["startTime"]);
          attempt["endTime"] = formatDate(attempt["endTime"]);
          attempt["lastUpdated"] = formatDate(attempt["lastUpdated"]);
          var app_clone = {"id" : id, "name" : name, "num" : num, "attempts" : [attempt]};
          array.push(app_clone);
        }
      }

      var data = {"applications": array}
      $.get("static/historypage-template.html", function(template) {
        historySummary.append(Mustache.render($(template).filter("#history-summary-template").html(),data));
        var selector = "#history-summary-table";
        var conf = {
                    "columns": [
                        {name: 'first', type: "appid-numeric"},
                        {name: 'second'},
                        {name: 'third'},
                        {name: 'fourth'},
                        {name: 'fifth'},
                        {name: 'sixth', type: "title-numeric"},
                        {name: 'seventh'},
                        {name: 'eighth'},
                    ],
                    "autoWidth": false,
                    "order": [[ 4, "desc" ]]
        };

        var rowGroupConf = {
                           "rowsGroup": [
                               'first:name',
                               'second:name'
                           ],
        };

        if (hasMultipleAttempts) {
          jQuery.extend(conf, rowGroupConf);
          var rowGroupCells = document.getElementsByClassName("rowGroupColumn");
          for (i = 0; i < rowGroupCells.length; i++) {
            rowGroupCells[i].style='background-color: #ffffff';
          }
        }

        if (!hasMultipleAttempts) {
          var attemptIDCells = document.getElementsByClassName("attemptIDSpan");
          for (i = 0; i < attemptIDCells.length; i++) {
            attemptIDCells[i].style.display='none';
          }
        }

        var durationCells = document.getElementsByClassName("durationClass");
        for (i = 0; i < durationCells.length; i++) {
          var timeInMilliseconds = parseInt(durationCells[i].title);
          durationCells[i].innerHTML = formatDuration(timeInMilliseconds);
        }

        if ($(selector.concat(" tr")).length < 20) {
          $.extend(conf, {paging: false});
        }

        $(selector).DataTable(conf);
        $('#hisotry-summary [data-toggle="tooltip"]').tooltip();
      });
    });
});
