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

import org.apache.spark.SparkException
import org.apache.spark.sql.connector.catalog.ViewChange
import org.apache.spark.sql.execution.command

class AlterViewUnsetTblPropertiesSuite
  extends command.AlterViewUnsetTblPropertiesSuiteBase with ViewCommandSuiteBase {

  test("V2: unset removes the entry from the stored View") {
    val view = s"$catalog.$namespace.v2_unset_view_info"
    createViewWithProps(view, "k" -> "v")
    sql(s"ALTER VIEW $view UNSET TBLPROPERTIES ('k')")
    val stored = viewCatalog.getStoredView(Array(namespace), "v2_unset_view_info")
    assert(!stored.properties.containsKey("k"))
    assert(viewCatalog.getLastViewChanges === Seq(ViewChange.removeProperty("k")))
  }

  test("V2: catalog IllegalArgumentException is converted to a structured error") {
    val view = s"$catalog.$namespace.v2_unset_view_rejected"
    createViewWithProps(view, "k" -> "v")
    viewCatalog.failAlterViewWith(new IllegalArgumentException("unset rejected"))
    try {
      checkError(
        exception = intercept[SparkException] {
          sql(s"ALTER VIEW $view UNSET TBLPROPERTIES ('k')")
        },
        condition = "UNSUPPORTED_VIEW_CHANGE",
        parameters = Map("message" -> "unset rejected"))
    } finally {
      viewCatalog.clearAlterViewFailure()
    }
  }
}
