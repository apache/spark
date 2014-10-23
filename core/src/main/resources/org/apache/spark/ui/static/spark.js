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
    //  - If a value of an entry in a row is an array it will further expand it with <span>
    //  - If a value is a sub-object with the syntax {value: nnn, attr: something}
    // then the cell will contain that value and the list of given attributes
    //  - If an entry contains </td> then we use it directly in the table as cell
    // without wrapping in with <td>
    //
    // example: [{x: 1}, {y: {value: 2, att: high}}]
    //          -> "<tr><td>1</td></tr><tr><td att='high'>2</td></tr>"
    //
    var fillTable = function (data, tableId) {
      var initTime = Date.now()
      // creating this array to hold the elements is an optimization since it's faster in
      // Javascript to join of strings rather than having a string and keep adding substring
      var r = new Array();
      var j = -1;
      for (var i = 0; i < data.length; i++) {
        var d = data[i];
        r[++j] = "<tr>";
        for (var ttt in d) {
          if (d.hasOwnProperty(ttt)) {
            var v = d[ttt]
            if (v instanceof Array) {
              tbl_longentry = "";
              for (var k = 0; k < v.length; k++){
                tbl_longentry += "<span>" + v[k] + "<br/></span>";
              }
              r[++j] = "<td>" + tbl_longentry + "</td>";
            } else if ((typeof v == "object") && (v != null)) {
              options = "";
              for (var key in v) {
                if ( v.hasOwnProperty(key) && (key != 'value')) {
                  options += " " + key + "='" + v[key] +"'";
                }
              }
              r[++j] = "<td"; r[++j] = options; r[++j] = ">"; r[++j] = v['value']; r[++j] = "</td>";
            } else if (v.toString().indexOf("</td>") != -1){
              r[++j] = v;
            } else {
              r[++j] = "<td>"; r[++j] = v; r[++j] = "</td>";
            }
          }
        }
        r[++j] = "</tr>";
      }
      $("#" + tableId + " tbody").html(r.join(''));
      $("#" + tableId + "Text").text("");
      var finalTime = Date.now();
      log("total rendering time for table " + tableId +": " + (finalTime - initTime)/1000 + " sec");
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
