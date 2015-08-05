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

/* Adds background colors to stripe table rows in the summary table (on the stage page). This is
 * necessary (instead of using css or the table striping provided by bootstrap) because the summary
 * table has hidden rows.
 *
 * An ID selector (rather than a class selector) is used to ensure this runs quickly even on pages
 * with thousands of task rows (ID selectors are much faster than class selectors). */
function stripeSummaryTable() {
    $("#task-summary-table").find("tr:not(:hidden)").each(function (index) {
       if (index % 2 == 1) {
         $(this).css("background-color", "#f9f9f9");
       } else {
         $(this).css("background-color", "#ffffff");
       }
    });
}

function toggleThreadStackTrace(threadId) {
    var threadCell = $("#" + threadId + "_stacktrace");
    var columnHeader = $('#stacktrace_column');
    var bindNum = parseInt(columnHeader.attr('bind'));
    if (threadCell.hasClass('hidden')) {
        // expand thread cell
        columnHeader.attr("bind", bindNum + 1);
        columnHeader.removeClass("hidden");
        threadCell.removeClass('hidden');
    } else {
        // collapse thread cell
        columnHeader.attr("bind", bindNum - 1);
        if (bindNum - 1 == 0) {
            columnHeader.addClass("hidden");
        }
        threadCell.addClass('hidden');
    }
}

// expandOrCollapse - true: expand, false: collapse
function expandOrCollapseAllThreadStackTrace(expandOrCollapse) {
    var columnHeader = $('#stacktrace_column');
    if (expandOrCollapse) {
        columnHeader.removeClass('hidden');
        $('.accordion-body').removeClass('hidden');
        columnHeader.attr("bind", $('.accordion-body').length);
        $('.expandbutton').toggleClass('hidden')
    } else {
        columnHeader.addClass('hidden');
        $('.accordion-body').addClass('hidden');
        columnHeader.attr("bind", 0);
        $('.expandbutton').toggleClass('hidden');
    }
}

// inOrOut - true: over, false: out
function onMouseOverAndOut(threadId) {
    $("#" + threadId + "_td_id").toggleClass("threaddump-td-mouseover");
    $("#" + threadId + "_td_name").toggleClass("threaddump-td-mouseover");
    $("#" + threadId + "_td_state").toggleClass("threaddump-td-mouseover");
}

function grep() {
    var grepExp = $("#grepexp").val();
    if (grepExp != "") {
        var url = location.href
        if (url.indexOf("&grepexp=") == -1) {
            location.href = url + "&grepexp=" + grepExp;
        } else {
            location.href = url.replace(/grep=(.*)&+/g, "grep=" + grepExp + "&");
        }
    } else {
        alert("input cannot be empty");
    }
}

function viewAll() {
    location.href = location.href.replace(/grep=(.*)&+/g, "");
}
