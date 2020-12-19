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

import org.apache.spark.sql.connector.catalog.{CatalogPlugin, Identifier}
import org.apache.spark.sql.execution.command

class DropTableSuite extends command.DropTableSuiteBase with CommandSuiteBase {
  test("purge option") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createTable(t)
      val errMsg = intercept[UnsupportedOperationException] {
        sql(s"DROP TABLE $catalog.ns.tbl PURGE")
      }.getMessage
      // The default TableCatalog.dropTable implementation doesn't support the purge option.
      assert(errMsg.contains("Purge option is not supported"))
    }
  }

  test("table qualified with the session catalog name") {
    import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
    def catalog(name: String): CatalogPlugin = {
      spark.sessionState.catalogManager.catalog(name)
    }

    val ident = Identifier.of(Array("default"), "tbl")
    sql("CREATE TABLE tbl USING json AS SELECT 1 AS i")
    assert(catalog("spark_catalog").asTableCatalog.tableExists(ident) === true)
    sql("DROP TABLE spark_catalog.default.tbl")
    assert(catalog("spark_catalog").asTableCatalog.tableExists(ident) === false)
  }
}
