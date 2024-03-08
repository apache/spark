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

package org.apache.spark.sql.execution.command

import java.time.{Duration, Period}

import org.apache.spark.sql.{QueryTest, Row}

/**
 * This base suite contains unified tests for the `ALTER TABLE .. REPLACE COLUMNS` command that
 * check the V2 table catalog. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableReplaceColumnsSuite`
 */
trait AlterTableReplaceColumnsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. REPLACE COLUMNS"

  test("SPARK-37304: Replace columns by ANSI intervals") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (ym INTERVAL MONTH, dt INTERVAL HOUR, data STRING) $defaultUsing")
      // TODO(SPARK-37303): Uncomment the command below after REPLACE COLUMNS is fixed
      // sql(s"INSERT INTO $t SELECT INTERVAL '1' MONTH, INTERVAL '2' HOUR, 'abc'")
      sql(
        s"""
           |ALTER TABLE $t REPLACE COLUMNS (
           | new_ym INTERVAL YEAR,
           | new_dt INTERVAL MINUTE,
           | new_data INT)""".stripMargin)
      sql(s"INSERT INTO $t SELECT INTERVAL '3' YEAR, INTERVAL '4' MINUTE, 5")

      checkAnswer(
        sql(s"SELECT new_ym, new_dt, new_data FROM $t"),
        Seq(
          Row(Period.ofYears(3), Duration.ofMinutes(4), 5)))
    }
  }
}
