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

import org.apache.spark.SparkUnsupportedOperationException
import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.InMemoryTableSessionCatalog
import org.apache.spark.sql.execution.command
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION

/**
 * The class contains tests for the `DROP TABLE` command to check V2 table catalogs.
 */
class DropTableSuite extends command.DropTableSuiteBase with CommandSuiteBase {
  test("purge option") {
    withNamespaceAndTable("ns", "tbl") { t =>
      createTable(t)
      // The default TableCatalog.purgeTable implementation throws an exception.
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          sql(s"DROP TABLE $catalog.ns.tbl PURGE")
        },
        condition = "UNSUPPORTED_FEATURE.PURGE_TABLE",
        parameters = Map.empty
      )
    }
  }

  test("table qualified with the session catalog name") {
    withSQLConf(
      V2_SESSION_CATALOG_IMPLEMENTATION.key -> classOf[InMemoryTableSessionCatalog].getName) {

      sql("CREATE TABLE tbl USING json AS SELECT 1 AS i")
      checkAnswer(
        sql("SHOW TABLES IN spark_catalog.default").select("tableName"),
        Row("tbl"))

      sql("DROP TABLE spark_catalog.default.tbl")
      checkAnswer(
        sql("SHOW TABLES IN spark_catalog.default").select("tableName"),
        Seq.empty)
    }
  }
}
