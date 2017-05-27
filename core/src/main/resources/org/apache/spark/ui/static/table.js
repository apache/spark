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

function toggleThreadStackTrace(threadId, forceAdd) {
    var stackTrace = $("#" + threadId + "_stacktrace")
    if (stackTrace.length == 0) {
        var stackTraceText = $('#' + threadId + "_td_stacktrace").html()
        var threadCell = $("#thread_" + threadId + "_tr")
        threadCell.after("<tr id=\"" + threadId +"_stacktrace\" class=\"accordion-body\"><td colspan=\"4\"><pre>" +
            stackTraceText +  "</pre></td></tr>")
    } else {
        if (!forceAdd) {
            stackTrace.remove()
        }
    }
}

function expandAllThreadStackTrace(toggleButton) {
    $('.accordion-heading').each(function() {
        //get thread ID
        if (!$(this).hasClass("hidden")) {
            var trId = $(this).attr('id').match(/thread_([0-9]+)_tr/m)[1]
            toggleThreadStackTrace(trId, true)
        }
    })
    if (toggleButton) {
        $('.expandbutton').toggleClass('hidden')
    }
}

function collapseAllThreadStackTrace(toggleButton) {
    $('.accordion-body').each(function() {
        $(this).remove()
    })
    if (toggleButton) {
        $('.expandbutton').toggleClass('hidden');
    }
}


// inOrOut - true: over, false: out
function onMouseOverAndOut(threadId) {
    $("#" + threadId + "_td_id").toggleClass("threaddump-td-mouseover");
    $("#" + threadId + "_td_name").toggleClass("threaddump-td-mouseover");
    $("#" + threadId + "_td_state").toggleClass("threaddump-td-mouseover");
    $("#" + threadId + "_td_locking").toggleClass("threaddump-td-mouseover");
}

function onSearchStringChange() {
    var searchString = $('#search').val().toLowerCase();
    //remove the stacktrace
    collapseAllThreadStackTrace(false)
    if (searchString.length == 0) {
        $('tr').each(function() {
            $(this).removeClass('hidden')
        })
    } else {
        $('tr').each(function(){
            if($(this).attr('id') && $(this).attr('id').match(/thread_[0-9]+_tr/) ) {
                var children = $(this).children()
                var found = false
                for (i = 0; i < children.length; i++) {
                    if (children.eq(i).text().toLowerCase().indexOf(searchString) >= 0) {
                        found = true
                    }
                }
                if (found) {
                    $(this).removeClass('hidden')
                } else {
                    $(this).addClass('hidden')
                }
            }
        });
    }
}
