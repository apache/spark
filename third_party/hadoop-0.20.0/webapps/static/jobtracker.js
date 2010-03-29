/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

function checkButtonVerbage()
{
  var inputs = document.getElementsByName("jobCheckBox");
  var check = getCheckStatus(inputs);

  setCheckButtonVerbage(! check);
}

function selectAll()
{
  var inputs = document.getElementsByName("jobCheckBox");
  var check = getCheckStatus(inputs);

  for (var i in inputs) {
    if ('jobCheckBox' == inputs[i].name) {
      if ( inputs[i].parentNode.parentNode.style.display != 'none') {
        inputs[i].checked = ! check;
      }
    }
  }

  setCheckButtonVerbage(check);
}

function getCheckStatus(inputs)
{
  var check = true;

  for (var i in inputs) {
    if ('jobCheckBox' == inputs[i].name) {
      if ( inputs[i].parentNode.parentNode.style.display != 'none') {
        check = (inputs[i].checked && check);
      }
    }
  }

  return check;
}


function setCheckButtonVerbage(check)
{
  var op = document.getElementById("checkEm");
  op.value = check ? "Select All" : "Deselect All";
}

function applyfilter()
{
  var cols = ["job","priority","user","name"];
  var nodes = [];
  var filters = [];

  for (var i = 0; i < cols.length; ++i) {
    nodes[i] = document.getElementById(cols[i] + "_0" );
  }

  var filter = document.getElementById("filter");
  filters = filter.value.split(' ');

  var row = 0;
  while ( nodes[0] != null ) {
    //default display status
    var display = true;

    // for each filter
    for (var filter_idx = 0; filter_idx < filters.length; ++filter_idx) {

      // go check each column
      if ((getDisplayStatus(nodes, filters[filter_idx], cols)) == 0) {
        display = false;
        break;
      }
    }

    // set the display status
    nodes[0].parentNode.style.display = display ? '' : 'none';

    // next row
    ++row;

    // next set of controls
    for (var i = 0; i < cols.length; ++i) {
      nodes[i] = document.getElementById(cols[i] + "_" + row);
    }
  }  // while
}

function getDisplayStatus(nodes, filter, cols)
{
  var offset = filter.indexOf(':');

  var search = offset != -1 ? filter.substring(offset + 1).toLowerCase() : filter.toLowerCase();

  for (var col = 0; col < cols.length; ++col) {
    // a column specific filter
    if (offset != -1 ) {
      var searchCol = filter.substring(0, offset).toLowerCase();

         if (searchCol == cols[col]) {
         // special case jobs to remove unnecessary stuff
         return containsIgnoreCase(stripHtml(nodes[col].innerHTML), search);
          }
     } else if (containsIgnoreCase(stripHtml(nodes[col].innerHTML), filter)) {
       return true;
     }
   }

  return false;
}

function stripHtml(text)
{
  return text.replace(/<[^>]*>/g,'').replace(/&[^;]*;/g,'');
}

function containsIgnoreCase(haystack, needle)
{
  return haystack.toLowerCase().indexOf(needle.toLowerCase()) != -1;
}

function confirmAction()
{
  return confirm("Are you sure?");
}

function toggle(id)
{
  if ( document.getElementById(id).style.display != 'block') {
    document.getElementById(id).style.display = 'block';
  }
  else {
    document.getElementById(id).style.display = 'none';
  }
}
