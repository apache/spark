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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.execution.command.DDLCommandTestUtils

/**
 * This base suite contains unified tests for table constraints (CHECK, PRIMARY KEY, UNIQUE,
 * FOREIGN KEY) that check V1 table catalogs. V1 tables do not support table constraints.
 * The tests that cannot run for all V1 catalogs are located in more specific test suites:
 *
 *   - V1 In-Memory catalog:
 *     `org.apache.spark.sql.execution.command.v1.TableConstraintSuite`
 *   - V1 Hive External catalog:
 *     `org.apache.spark.sql.hive.execution.command.TableConstraintSuite`
 */
trait TableConstraintSuiteBase extends DDLCommandTestUtils {
  override val command = "TABLE CONSTRAINT"

  private val constraintTypes = Seq(
    "CHECK (id > 0)",
    "PRIMARY KEY (id)",
    "UNIQUE (id)",
    "FOREIGN KEY (id) REFERENCES t2(id)"
  )

  gridTest("SPARK-54761: create table with constraint - should fail")(constraintTypes)
  { constraint =>
    withNamespaceAndTable("ns", "table_1") { t =>
      val createTableSql = s"CREATE TABLE $t (id INT, CONSTRAINT c1 $constraint) $defaultUsing"
      val error = intercept[AnalysisException] {
        sql(createTableSql)
      }
      checkError(
        exception = error,
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> s"`$catalog`.`ns`.`table_1`",
          "operation" -> "CONSTRAINT"
        )
      )
    }
  }

  gridTest("SPARK-54761: alter table add constraint - should fail")(constraintTypes) { constraint =>
    withNamespaceAndTable("ns", "table_1") { t =>
      sql(s"CREATE TABLE $t (id INT) $defaultUsing")
      val alterTableSql = s"ALTER TABLE $t ADD CONSTRAINT c1 $constraint"
      val error = intercept[AnalysisException] {
        sql(alterTableSql)
      }
      checkError(
        exception = error,
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> s"`$catalog`.`ns`.`table_1`",
          "operation" -> "ADD CONSTRAINT"
        )
      )
    }
  }

  test("SPARK-54761: alter table drop constraint - should fail") {
    withNamespaceAndTable("ns", "table_1") { t =>
      sql(s"CREATE TABLE $t (id INT) $defaultUsing")
      val error = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP CONSTRAINT c1")
      }
      checkError(
        exception = error,
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> s"`$catalog`.`ns`.`table_1`",
          "operation" -> "DROP CONSTRAINT"
        )
      )
    }
  }

  // REPLACE TABLE is not supported for V1 tables, so the error should be about
  // REPLACE TABLE, not about CONSTRAINT
  gridTest("SPARK-54761: replace table with constraint - should fail")(constraintTypes)
  { constraint =>
    withNamespaceAndTable("ns", "table_1") { t =>
      val replaceTableSql = s"REPLACE TABLE $t (id INT, CONSTRAINT c1 $constraint) $defaultUsing"
      val error = intercept[AnalysisException] {
        sql(replaceTableSql)
      }
      checkError(
        exception = error,
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        parameters = Map(
          "tableName" -> s"`$catalog`.`ns`.`table_1`",
          "operation" -> "REPLACE TABLE"
        )
      )
    }
  }
}

/**
 * The class contains tests for table constraints to check V1 In-Memory table catalog.
 */
class TableConstraintSuite extends TableConstraintSuiteBase with CommandSuiteBase

