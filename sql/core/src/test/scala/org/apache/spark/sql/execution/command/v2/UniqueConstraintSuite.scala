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
import org.apache.spark.sql.execution.command.DDLCommandTestUtils

class UniqueConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "UNIQUE CONSTRAINT"

  private val validConstraintCharacteristics = Seq(
    ("", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NOT ENFORCED", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NOT ENFORCED NORELY", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NORELY NOT ENFORCED", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NORELY", "NOT ENFORCED UNVALIDATED NORELY"),
    ("RELY", "NOT ENFORCED UNVALIDATED RELY")
  )

  test("Add unique constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", catalog) { t =>
        sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
        assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

        sql(s"ALTER TABLE $t ADD CONSTRAINT uk1 UNIQUE (id) $characteristic")
        val table = loadTable(catalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "uk1")
        assert(constraint.toDDL == s"CONSTRAINT uk1 UNIQUE (id) $expectedDDL")
      }
    }
  }

  test("Create table with unique constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        val constraintStr = s"CONSTRAINT uk1 UNIQUE (id) $characteristic"
        sql(s"CREATE TABLE $t (id bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "uk1")
        assert(constraint.toDDL == s"CONSTRAINT uk1 UNIQUE (id) $expectedDDL")
      }
    }
  }

  test("Replace table with unique constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        val constraintStr = s"CONSTRAINT uk1 UNIQUE (id) $characteristic"
        sql(s"CREATE TABLE $t (id bigint) $defaultUsing")
        sql(s"REPLACE TABLE $t (id bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "uk1")
        assert(constraint.toDDL == s"CONSTRAINT uk1 UNIQUE (id) $expectedDDL")
      }
    }
  }

  test("Add duplicated unique constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT uk1 UNIQUE (id)")
      // Constraint names are case-insensitive
      Seq("uk1", "UK1").foreach { name =>
        val error = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD CONSTRAINT $name UNIQUE (id)")
        }
        checkError(
          exception = error,
          condition = "CONSTRAINT_ALREADY_EXISTS",
          sqlState = "42710",
          parameters = Map("constraintName" -> "uk1",
            "oldConstraint" -> "CONSTRAINT uk1 UNIQUE (id) NOT ENFORCED UNVALIDATED NORELY")
        )
      }
    }
  }

  test("Add unique constraint with multiple columns") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id1 bigint, id2 bigint, data string) $defaultUsing")
      assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT uk1 UNIQUE (id1, id2)")
      val table = loadTable(catalog, "ns", "tbl")
      assert(table.constraints.length == 1)
      val constraint = table.constraints.head
      assert(constraint.name() == "uk1")
      assert(constraint.toDDL ==
        "CONSTRAINT uk1 UNIQUE (id1, id2) NOT ENFORCED UNVALIDATED NORELY")
    }
  }
}
