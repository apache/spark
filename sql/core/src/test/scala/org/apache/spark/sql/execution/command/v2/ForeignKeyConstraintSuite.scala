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

class ForeignKeyConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "FOREIGN KEY CONSTRAINT"

  private val validConstraintCharacteristics = Seq(
    ("", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NOT ENFORCED", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NOT ENFORCED NORELY", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NORELY NOT ENFORCED", "NOT ENFORCED UNVALIDATED NORELY"),
    ("NORELY", "NOT ENFORCED UNVALIDATED NORELY"),
    ("RELY", "NOT ENFORCED UNVALIDATED RELY")
  )

  test("Add foreign key constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", catalog) { t =>
        sql(s"CREATE TABLE $t (id bigint, fk bigint, data string) $defaultUsing")
        sql(s"CREATE TABLE ${t}_ref (id bigint, data string) $defaultUsing")
        assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

        sql(s"ALTER TABLE $t ADD CONSTRAINT fk1 FOREIGN KEY (fk) " +
          s"REFERENCES ${t}_ref(id) $characteristic")
        val table = loadTable(catalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "fk1")
        assert(constraint.toDDL == s"CONSTRAINT fk1 FOREIGN KEY (fk) " +
          s"REFERENCES test_catalog.ns.tbl_ref (id) $expectedDDL")
      }
    }
  }

  test("Create table with foreign key constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        sql(s"CREATE TABLE ${t}_ref (id bigint, data string) $defaultUsing")
        val constraintStr = s"CONSTRAINT fk1 FOREIGN KEY (fk) " +
          s"REFERENCES ${t}_ref(id) $characteristic"
        sql(s"CREATE TABLE $t (id bigint, fk bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "fk1")
        assert(constraint.toDDL == s"CONSTRAINT fk1 FOREIGN KEY (fk) " +
          s"REFERENCES non_part_test_catalog.ns.tbl_ref (id) $expectedDDL")
      }
    }
  }

  test("REPLACE table with foreign key constraint") {
    validConstraintCharacteristics.foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        sql(s"CREATE TABLE $t (id bigint) $defaultUsing")
        sql(s"CREATE TABLE ${t}_ref (id bigint, data string) $defaultUsing")
        val constraintStr = s"CONSTRAINT fk1 FOREIGN KEY (fk) " +
          s"REFERENCES ${t}_ref(id) $characteristic"
        sql(s"REPLACE TABLE $t (id bigint, fk bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.constraints.length == 1)
        val constraint = table.constraints.head
        assert(constraint.name() == "fk1")
        assert(constraint.toDDL == s"CONSTRAINT fk1 FOREIGN KEY (fk) " +
          s"REFERENCES non_part_test_catalog.ns.tbl_ref (id) $expectedDDL")
      }
    }
  }

  test("Add duplicated foreign key constraint") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, fk bigint, data string) $defaultUsing")
      sql(s"CREATE TABLE ${t}_ref (id bigint, data string) $defaultUsing")
      assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT fk1 FOREIGN KEY (fk) REFERENCES ${t}_ref(id)")
      // Constraint names are case-insensitive
      Seq("fk1", "FK1").foreach { name =>
        val error = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD CONSTRAINT $name FOREIGN KEY (fk) REFERENCES ${t}_ref(id)")
        }
        checkError(
          exception = error,
          condition = "CONSTRAINT_ALREADY_EXISTS",
          sqlState = "42710",
          parameters = Map("constraintName" -> "fk1",
            "oldConstraint" ->
              ("CONSTRAINT fk1 FOREIGN KEY (fk) " +
                "REFERENCES test_catalog.ns.tbl_ref (id) NOT ENFORCED UNVALIDATED NORELY"))
        )
      }
    }
  }

  test("Add foreign key constraint with multiple columns") {
    withNamespaceAndTable("ns", "tbl", catalog) { t =>
      sql(s"CREATE TABLE $t (id1 bigint, id2 bigint, fk1 bigint, fk2 bigint, data string) " +
        s"$defaultUsing")
      sql(s"CREATE TABLE ${t}_ref (id1 bigint, id2 bigint, data string) $defaultUsing")
      assert(loadTable(catalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT fk1 FOREIGN KEY (fk1, fk2) REFERENCES ${t}_ref(id1, id2)")
      val table = loadTable(catalog, "ns", "tbl")
      assert(table.constraints.length == 1)
      val constraint = table.constraints.head
      assert(constraint.name() == "fk1")
      assert(constraint.toDDL ==
        s"CONSTRAINT fk1 FOREIGN KEY (fk1, fk2) " +
          s"REFERENCES test_catalog.ns.tbl_ref (id1, id2) NOT ENFORCED UNVALIDATED NORELY")
    }
  }
}
