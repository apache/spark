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

// create the Spark namespace
var Spark = Spark || {};

// define the UI namespace within Spark
Spark.UI = Spark.UI || {};

Spark.UI = (function ($) {
    // Initialize the application
    var init = function () {
    };

    var log = function(msg) {
        console.log(msg);
    };

    // define a function that fills a table with the rows given
    // from a JSON data input: [{row1},{row2},...]
    var fillTable = function (data, tableId) {
        var tbl_body = "";
        $.each(data, function() {
            var tbl_row = "";
            $.each(this, function(k , v) {
                tbl_row += "<td>"+v+"</td>";
            })
            tbl_body += "<tr>"+tbl_row+"</tr>";
        })
        $("#" + tableId + " tbody").html(tbl_body);
    };

    // Return the public facing methods for Spark.UI
    return {
        init: init,
        log: log,
        fillTable: fillTable
    };
}(jQuery));

// execute when the page finishes loading
$(function () {
    Spark.UI.init();
});