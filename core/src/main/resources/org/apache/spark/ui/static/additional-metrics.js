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

/* Register functions to show/hide columns based on checkboxes. These need
 * to be registered after the page loads. */
$(function() {
    $("span.expand-additional-metrics").click(function(){
        // Expand the list of additional metrics.
        var additionalMetricsDiv = $(this).parent().find('.additional-metrics');
        $(additionalMetricsDiv).toggleClass('collapsed');

        // Switch the class of the arrow from open to closed.
        $(this).find('.expand-additional-metrics-arrow').toggleClass('arrow-open');
        $(this).find('.expand-additional-metrics-arrow').toggleClass('arrow-closed');
    });

    stripeSummaryTable();

    $("input:checkbox").click(function() {
        var column = "table ." + $(this).attr("name");
        $(column).toggle();
        stripeSummaryTable();
    });

    $("#select-all-metrics").click(function() {
       if (this.checked) {
          // Toggle all un-checked options.
          $('input:checkbox:not(:checked)').trigger('click');
       } else {
          // Toggle all checked options.
          $('input:checkbox:checked').trigger('click');
       }
    });

    // Trigger a click on the checkbox if a user clicks the label next to it.
    $("span.additional-metric-title").click(function() {
        $(this).parent().find('input:checkbox').trigger('click');
    });
});
