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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.execution.command.DDLCommandTestUtils

class DropConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "ALTER TABLE .. DROP CONSTRAINT"

  test("Drop a non-exist constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP CONSTRAINT c1")
      }
      checkError(
        exception = e,
        condition = "CONSTRAINT_DOES_NOT_EXIST",
        sqlState = "42704",
        parameters = Map("constraintName" -> "c1", "tableName" -> "test_catalog.ns.tbl")
      )
    }
  }

  test("Drop a non-exist constraint if exists") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      sql(s"ALTER TABLE $t DROP CONSTRAINT IF EXISTS c1")
    }
  }

  test("Drop a constraint on a non-exist table") {
    Seq("", "IF EXISTS").foreach { ifExists =>
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE test_catalog.ns.tbl DROP CONSTRAINT $ifExists c1")
      }
      checkError(
        exception = e,
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        sqlState = "42P01",
        parameters = Map("relationName" -> "`test_catalog`.`ns`.`tbl`"),
        context = ExpectedContext(
          fragment = "test_catalog.ns.tbl",
          start = 12,
          stop = 30
        )
      )
    }
  }

  test("Drop existing constraints") {
    Seq("", "IF EXISTS").foreach { ifExists =>
      withNamespaceAndTable("ns", "tbl", catalog) { t =>
        sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
        sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (id > 0)")
        sql(s"ALTER TABLE $t ADD CONSTRAINT c2 CHECK (len(data) > 0)")
        sql(s"ALTER TABLE $t DROP CONSTRAINT $ifExists c1")
        val table = loadTable(catalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head.asInstanceOf[Check]
        assert(constraint.name() == "c2")

        sql(s"ALTER TABLE $t DROP CONSTRAINT $ifExists c2")
        val table2 = loadTable(catalog, "ns", "tbl")
        assert(table2.constraints.length == 0)
      }
    }
  }

  test("Drop constraint is case insensitive") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      sql(s"ALTER TABLE $t ADD CONSTRAINT abc CHECK (id > 0)")
      sql(s"ALTER TABLE $t DROP CONSTRAINT aBC")
    }
  }
}
