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

/* Adds background colors to stripe table rows. This is necessary (instead of using css or the
 * table striping provided by bootstrap) to appropriately stripe tables with hidden rows. */
function stripeTables() {
    $("table.table-striped-custom").each(function() {
        $(this).find("tr:not(:hidden)").each(function (index) {
           if (index % 2 == 1) {
             $(this).css("background-color", "#f9f9f9");
           } else {
             $(this).css("background-color", "#ffffff");
           }
        });
    });
}

/* Stripe all tables after pages finish loading. */
$(function() {
    stripeTables();
});
