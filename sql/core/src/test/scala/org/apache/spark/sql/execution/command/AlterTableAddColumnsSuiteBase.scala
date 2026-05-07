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
 * This base suite contains unified tests for the `ALTER TABLE .. ADD COLUMNS` command that
 * check V1 and V2 table catalogs. The tests that cannot run for all supported catalogs are
 * located in more specific test suites:
 *
 *   - V2 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v2.AlterTableAddColumnsSuite`
 *   - V1 table catalog tests:
 *     `org.apache.spark.sql.execution.command.v1.AlterTableAddColumnsSuiteBase`
 *     - V1 In-Memory catalog:
 *       `org.apache.spark.sql.execution.command.v1.AlterTableAddColumnsSuite`
 *     - V1 Hive External catalog:
 *       `org.apache.spark.sql.hive.execution.command.AlterTableAddColumnsSuite`
 */
trait AlterTableAddColumnsSuiteBase extends QueryTest with DDLCommandTestUtils {
  override val command = "ALTER TABLE .. ADD COLUMNS"

  test("add an ANSI interval columns") {
    assume(!catalogVersion.contains("Hive")) // Hive catalog doesn't support the interval types

    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (id bigint) $defaultUsing")
      sql(s"ALTER TABLE $t ADD COLUMNS (ym INTERVAL YEAR, dt INTERVAL HOUR)")
      sql(s"INSERT INTO $t SELECT 0, INTERVAL '100' YEAR, INTERVAL '10' HOUR")
      checkAnswer(
        sql(s"SELECT id, ym, dt data FROM $t"),
        Seq(Row(0, Period.ofYears(100), Duration.ofHours(10))))
    }
  }
}
