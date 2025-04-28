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

class PrimaryKeyConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "PRIMARY KEY CONSTRAINT"

  private val validConstraintCharacteristics = Seq(
    ("", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NOT ENFORCED", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NOT ENFORCED NORELY", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NORELY NOT ENFORCED", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NORELY", "NOT ENFORCED UNVALIDATED NORELY"),
    ("RELY", "NOT ENFORCED UNVALIDATED RELY")
  )

  test("Add primary key constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
        assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

        sql(s"ALTER TABLE $t ADD CONSTRAINT pk1 PRIMARY KEY (id) $characteristic")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "pk1")
        assert(constraint.toDDL == s"CONSTRAINT pk1 PRIMARY KEY (id) $expectedDDL")
      }
    }
  }

  test("Create table with primary key constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        val constraintStr = s"CONSTRAINT pk1 PRIMARY KEY (id) $characteristic"
        sql(s"CREATE TABLE $t (id bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "pk1")
        assert(constraint.toDDL == s"CONSTRAINT pk1 PRIMARY KEY (id) $expectedDDL")
      }
    }
  }

  test("Replace table with primary key constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        val constraintStr = s"CONSTRAINT pk1 PRIMARY KEY (id) $characteristic"
        sql(s"CREATE TABLE $t (id bigint) $defaultUsing")
        sql(s"REPLACE TABLE $t (id bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "pk1")
        assert(constraint.toDDL == s"CONSTRAINT pk1 PRIMARY KEY (id) $expectedDDL")
      }
    }
  }

  test("Add duplicated primary key constraint") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT pk1 PRIMARY KEY (id)")
      // Constraint names are case-insensitive
      Seq("pk1", "PK1").foreach { name =>
        val error = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD CONSTRAINT $name PRIMARY KEY (id)")
        }
        checkError(
          exception = error,
          condition = "CONSTRAINT_ALREADY_EXISTS",
          sqlState = "42710",
          parameters = Map("constraintName" -> "pk1",
            "oldConstraint" -> "CONSTRAINT pk1 PRIMARY KEY (id) NOT ENFORCED UNVALIDATED NORELY")
        )
      }
    }
  }

  test("Add primary key constraint with multiple columns") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id1 bigint, id2 bigint, data string) $defaultUsing")
      assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT pk1 PRIMARY KEY (id1, id2)")
      val table = loadTable(nonPartitionCatalog, "ns", "tbl")
      assert(table.constraints.length == 1)
      val constraint = table.constraints.head
      assert(constraint.name() == "pk1")
      assert(constraint.toDDL ==
        "CONSTRAINT pk1 PRIMARY KEY (id1, id2) NOT ENFORCED UNVALIDATED NORELY")
    }
  }
}
