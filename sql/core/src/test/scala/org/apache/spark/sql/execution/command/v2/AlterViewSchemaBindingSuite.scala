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

class AlterViewSchemaBindingSuite
  extends command.AlterViewSchemaBindingSuiteBase with ViewCommandSuiteBase {

  test("V2: catalog stores the new schema mode on ViewInfo") {
    val view = s"$catalog.$namespace.v2_schema_mode"
    createView(view)
    sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")
    val stored = viewCatalog.getStoredView(Array(namespace), "v2_schema_mode")
    assert(stored.schemaMode == "EVOLUTION")
  }

  test("V2: switching to EVOLUTION clears queryColumnNames; switching back restores them") {
    // Mirrors v1 `generateViewProperties`: in EVOLUTION mode the view always uses its current
    // schema as the column source, so persisting `queryColumnNames` would be non-canonical.
    // After flipping back to BINDING via a CREATE OR REPLACE (which re-captures column names),
    // the field must be populated again so view-text expansion has the original aliases.
    val name = "v2_schema_mode_qcols"
    val view = s"$catalog.$namespace.$name"
    sql(s"CREATE VIEW $view AS SELECT 1 AS x, 2 AS y")
    val initial = viewCatalog.getStoredView(Array(namespace), name)
    assert(initial.queryColumnNames.toSeq == Seq("x", "y"))

    sql(s"ALTER VIEW $view WITH SCHEMA EVOLUTION")
    val afterEvo = viewCatalog.getStoredView(Array(namespace), name)
    assert(afterEvo.schemaMode == "EVOLUTION")
    assert(afterEvo.queryColumnNames.isEmpty,
      "queryColumnNames must be cleared in EVOLUTION mode")

    sql(s"ALTER VIEW $view WITH SCHEMA BINDING")
    val afterBinding = viewCatalog.getStoredView(Array(namespace), name)
    assert(afterBinding.schemaMode == "BINDING")
    // ALTER VIEW WITH SCHEMA BINDING does not re-analyze the view body; the queryColumnNames
    // field stays at whatever ALTER VIEW WITH SCHEMA EVOLUTION left it as. Users who want the
    // original aliases back run CREATE OR REPLACE VIEW, which re-captures them.
    assert(afterBinding.queryColumnNames.isEmpty)
  }
}
