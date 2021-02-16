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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.{AnalysisException, Row}
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `RENAME TABLE` command that check V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.RenameTableSuite`
 *   - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.RenameTableSuite`
 */
trait RenameTableSuiteBase extends command.RenameTableSuiteBase {
  test("omit namespace in the destination table") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")

      sql(s"ALTER TABLE $src RENAME TO dst_tbl")
      checkTables("ns", "dst_tbl")
      checkAnswer(sql(s"SELECT c0 FROM $dst"), Seq(Row(0)))
    }
  }

  test("destination database is different") {
    withNamespaceAndTable("dst_ns", "dst_tbl") { dst =>
      withNamespace("src_ns") {
        sql(s"CREATE NAMESPACE $catalog.src_ns")
        val src = dst.replace("dst", "src")
        sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
        val errMsg = intercept[AnalysisException] {
          sql(s"ALTER TABLE $src RENAME TO dst_ns.dst_tbl")
        }.getMessage
        assert(errMsg.contains("source and destination databases do not match"))
      }
    }
  }

  test("rename without explicitly specifying database") {
    try {
      withNamespaceAndTable("ns", "dst_tbl") { dst =>
        val src = dst.replace("dst", "src")
        sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
        sql(s"INSERT INTO $src SELECT 0")

        sql(s"USE $catalog.ns")
        sql(s"ALTER TABLE src_tbl RENAME TO dst_tbl")
        checkTables("ns", "dst_tbl")
        checkAnswer(sql(s"SELECT c0 FROM $dst"), Seq(Row(0)))
      }
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }
}

/**
 * The class contains tests for the `RENAME TABLE` command to check V1 In-Memory table catalog.
 */
class RenameTableSuite extends RenameTableSuiteBase with CommandSuiteBase {
}
