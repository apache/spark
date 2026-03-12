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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.command

/**
 * This base suite contains unified tests for the `ALTER TABLE .. RENAME` command that check V1
 * table catalogs. The tests that cannot run for all V1 catalogs are located in more
 * specific test suites:
 *
 *   - V1 In-Memory catalog: `org.apache.spark.sql.execution.command.v1.AlterTableRenameSuite`
 *   - V1 Hive External catalog: `org.apache.spark.sql.hive.execution.command.AlterTableRenameSuite`
 */
trait AlterTableRenameSuiteBase extends command.AlterTableRenameSuiteBase with QueryErrorsBase {
  test("destination database is different") {
    withNamespaceAndTable("dst_ns", "dst_tbl") { dst =>
      withNamespace("src_ns") {
        sql(s"CREATE NAMESPACE $catalog.src_ns")
        val src = dst.replace("dst", "src")
        sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"ALTER TABLE $src RENAME TO dst_ns.dst_tbl")
          },
          condition = "_LEGACY_ERROR_TEMP_1073",
          parameters = Map("db" -> "src_ns", "newDb" -> "dst_ns")
        )
      }
    }
  }

  test("preserve table stats") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")
      sql(s"ANALYZE TABLE $src COMPUTE STATISTICS")
      val size = getTableSize(src)
      assert(size > 0)
      sql(s"ALTER TABLE $src RENAME TO ns.dst_tbl")
      assert(size === getTableSize(dst))
    }
  }

  test("the destination folder exists already") {
    withNamespaceAndTable("ns", "dst_tbl") { dst =>
      val src = dst.replace("dst", "src")
      sql(s"CREATE TABLE $src (c0 INT) $defaultUsing")
      sql(s"INSERT INTO $src SELECT 0")

      sql(s"CREATE TABLE $dst (c0 INT) $defaultUsing")
      withTableDir(dst) { (fs, dst_dir) =>
        sql(s"DROP TABLE $dst")
        fs.mkdirs(dst_dir)
        checkError(
          exception = intercept[SparkRuntimeException] {
            sql(s"ALTER TABLE $src RENAME TO ns.dst_tbl")
          },
          condition = "LOCATION_ALREADY_EXISTS",
          parameters = Map(
            "location" -> s"'$dst_dir'",
            "identifier" -> toSQLId(dst)))
      }
    }
  }
}

/**
 * The class contains tests for the `ALTER TABLE .. RENAME` command to check
 * V1 In-Memory table catalog.
 */
class AlterTableRenameSuite extends AlterTableRenameSuiteBase with CommandSuiteBase
