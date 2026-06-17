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

/**
 * The class contains tests for the `ALTER VIEW ... SET TBLPROPERTIES` command on V2 view
 * catalogs (`AlterV2ViewSetPropertiesExec`).
 */
class AlterViewSetTblPropertiesSuite
  extends command.AlterViewSetTblPropertiesSuiteBase with ViewCommandSuiteBase {

  test("V2: catalog stores the property on ViewInfo") {
    val view = s"$catalog.$namespace.v2_set_view_info"
    createView(view)
    sql(s"ALTER VIEW $view SET TBLPROPERTIES ('k' = 'v')")
    val stored = viewCatalog.getStoredView(Array(namespace), "v2_set_view_info")
    assert(stored.properties.get("k") == "v")
  }
}
