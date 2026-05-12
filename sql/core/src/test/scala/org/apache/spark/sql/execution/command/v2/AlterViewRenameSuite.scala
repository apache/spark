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

import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.command

class AlterViewRenameSuite
  extends command.AlterViewRenameSuiteBase with ViewCommandSuiteBase {

  test("V2: catalog state moves the entry between identifiers") {
    val src = s"$catalog.$namespace.v2_rename_src"
    createView(src)
    sql(s"ALTER VIEW $src RENAME TO v2_rename_dst")
    assert(!viewCatalog.viewExists(Identifier.of(Array(namespace), "v2_rename_src")))
    assert(viewCatalog.viewExists(Identifier.of(Array(namespace), "v2_rename_dst")))
  }

  test("V2: rename to a 2-part target moves the view across namespaces") {
    // Exercises `RenameV2ViewExec`'s non-empty-namespace branch -- when the new identifier
    // already carries a namespace, the exec passes it through verbatim rather than defaulting
    // to the source namespace.
    val src = s"$catalog.$namespace.v2_rename_xns_src"
    val otherNs = "other_ns"
    sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$otherNs")
    createView(src)
    try {
      sql(s"ALTER VIEW $src RENAME TO $otherNs.v2_rename_xns_dst")
      assert(!viewCatalog.viewExists(Identifier.of(Array(namespace), "v2_rename_xns_src")))
      assert(viewCatalog.viewExists(Identifier.of(Array(otherNs), "v2_rename_xns_dst")))
    } finally {
      sql(s"DROP VIEW IF EXISTS $catalog.$otherNs.v2_rename_xns_dst")
    }
  }
}
