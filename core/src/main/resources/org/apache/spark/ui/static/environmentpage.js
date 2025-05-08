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

/* global $ */

$(document).ready(function(){
  $('th').on('click', function(e) {
    let inputBox = $(this).find('.env-table-filter-input');
    if (inputBox.length === 0) {
      $('<input class="env-table-filter-input form-control" type="text">')
        .appendTo(this)
        .focus();
    } else {
      inputBox.toggleClass('d-none');
      inputBox.focus();
    }
    e.stopPropagation();
  });

  $(document).on('click', function() {
    $('.env-table-filter-input').toggleClass('d-none', true);
  });

  $(document).on('input', '.env-table-filter-input', function() {
    const table = $(this).closest('table');
    const filters = table.find('.env-table-filter-input').map(function() {
      const columnIdx = $(this).closest('th').index();
      const searchString = $(this).val().toLowerCase();
      return { columnIdx, searchString };
    }).get();

    table.find('tbody tr').each(function() {
      let showRow = true;
      for (const filter of filters) {
        const cellText = $(this).find('td').eq(filter.columnIdx).text().toLowerCase();
        if (filter.searchString && cellText.indexOf(filter.searchString) === -1) {
          showRow = false;
          break;
        }
      }
      if (showRow) {
        $(this).show();
      } else {
        $(this).hide();
      }
    });
  });
});
