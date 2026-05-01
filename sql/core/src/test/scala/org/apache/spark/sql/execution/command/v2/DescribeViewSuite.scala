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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.execution.command

class DescribeViewSuite
  extends command.DescribeViewSuiteBase with ViewCommandSuiteBase {

  test("V2: extended emits the v2-native `# Detailed View Information` header") {
    // v1 emits `# Detailed Table Information` for views (CatalogTableType.VIEW shares the
    // same describe path as CatalogTableType.{MANAGED,EXTERNAL}); v2's `DescribeV2ViewExec`
    // routes views to a dedicated header. Pin the v2-side text here so the divergence stays
    // intentional.
    val view = s"$catalog.$namespace.v2_desc_ext_header"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x")
    val rows = sql(s"DESCRIBE TABLE EXTENDED $view").collect().map(_.getString(0))
    assert(rows.contains("# Detailed View Information"),
      s"v2 extended describe should emit the View header; got:\n${rows.mkString("\n")}")
  }
}
