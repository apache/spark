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
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.constraints.Check
import org.apache.spark.sql.execution.command.DDLCommandTestUtils
import org.apache.spark.sql.internal.SQLConf

class CheckConstraintSuite extends QueryTest with CommandSuiteBase with DDLCommandTestUtils {
  override protected def command: String = "Check CONSTRAINT"

  test("Nondeterministic expression -- alter table") {
    withTable("t") {
      sql("create table t(i double)")
      val query =
        """
          |ALTER TABLE t ADD CONSTRAINT c1 CHECK (i > rand(0))
          |""".stripMargin
      val error = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = error,
        condition = "NON_DETERMINISTIC_CHECK_CONSTRAINT",
        sqlState = "42621",
        parameters = Map("checkCondition" -> "i > rand(0)"),
        context = ExpectedContext(
          fragment = "i > rand(0)",
          start = 40,
          stop = 50
        )
      )
    }
  }

  test("Nondeterministic expression -- create table") {
    Seq(
      "CREATE TABLE t(i DOUBLE CHECK (i > rand(0)))",
      "CREATE TABLE t(i DOUBLE, CONSTRAINT c1 CHECK (i > rand(0)))",
      "REPLACE TABLE t(i DOUBLE CHECK (i > rand(0)))",
      "REPLACE TABLE t(i DOUBLE, CONSTRAINT c1 CHECK (i > rand(0)))"
    ).foreach { query =>
      withTable("t") {
        val error = intercept[AnalysisException] {
          sql(query)
        }
        checkError(
          exception = error,
          condition = "NON_DETERMINISTIC_CHECK_CONSTRAINT",
          sqlState = "42621",
          parameters = Map("checkCondition" -> "i > rand(0)"),
          context = ExpectedContext(
            fragment = "i > rand(0)"
          )
        )
      }
    }
  }

  test("Expression referring a column of another table -- alter table") {
    withTable("t", "t2") {
      sql("CREATE TABLE t(i DOUBLE) USING parquet")
      sql("CREATE TABLE t2(j STRING) USING parquet")
      val query =
        """
          |ALTER TABLE t ADD CONSTRAINT c1 CHECK (len(t2.j) > 0)
          |""".stripMargin
      val error = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = error,
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> "`t2`.`j`", "proposal" -> "`t`.`i`"),
        context = ExpectedContext(
          fragment = "t2.j",
          start = 44,
          stop = 47
        )
      )
    }
  }

  test("Expression referring a column of another table -- create and replace table") {
    withTable("t", "t2") {
      sql("CREATE TABLE t(i double) USING parquet")
      val query = "CREATE TABLE t2(j string check(t.i > 0)) USING parquet"
      val error = intercept[AnalysisException] {
        sql(query)
      }
      checkError(
        exception = error,
        condition = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        sqlState = "42703",
        parameters = Map("objectName" -> "`t`.`i`", "proposal" -> "`j`"),
        context = ExpectedContext(
          fragment = "t.i"
        )
      )
    }
  }

  private def getCheckConstraint(table: Table): Check = {
    assert(table.constraints.length == 1)
    assert(table.constraints.head.isInstanceOf[Check])
    table.constraints.head.asInstanceOf[Check]
    table.constraints.head.asInstanceOf[Check]
  }

  test("Predicate should be null if it can't be converted to V2 predicate") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, j string) $defaultUsing")
      sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (from_json(j, 'a INT').a > 1)")
      val table = loadTable(nonPartitionCatalog, "ns", "tbl")
      val constraint = getCheckConstraint(table)
      assert(constraint.name() == "c1")
      assert(constraint.toDDL ==
        "CONSTRAINT c1 CHECK (from_json(j, 'a INT').a > 1) ENFORCED UNVALIDATED NORELY")
      assert(constraint.predicateSql() == "from_json(j, 'a INT').a > 1")
      assert(constraint.predicate() == null)
    }
  }

  def getConstraintCharacteristics(): Seq[(String, String)] = {
    val validStatus = "UNVALIDATED"
    Seq(
      ("", s"ENFORCED $validStatus NORELY"),
      ("NOT ENFORCED", s"NOT ENFORCED $validStatus NORELY"),
      ("NOT ENFORCED NORELY", s"NOT ENFORCED $validStatus NORELY"),
      ("NORELY NOT ENFORCED", s"NOT ENFORCED $validStatus NORELY"),
      ("NORELY", s"ENFORCED $validStatus NORELY"),
      ("NOT ENFORCED RELY", s"NOT ENFORCED $validStatus RELY"),
      ("RELY NOT ENFORCED", s"NOT ENFORCED $validStatus RELY"),
      ("NOT ENFORCED RELY", s"NOT ENFORCED $validStatus RELY"),
      ("RELY NOT ENFORCED", s"NOT ENFORCED $validStatus RELY"),
      ("RELY", s"ENFORCED $validStatus RELY")
    )
  }

  test("Create table with check constraint") {
    getConstraintCharacteristics().foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        val constraintStr = s"CONSTRAINT c1 CHECK (id > 0) $characteristic"
        sql(s"CREATE TABLE $t (id bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        val constraint = getCheckConstraint(table)
        assert(constraint.name() == "c1")
        assert(constraint.toDDL == s"CONSTRAINT c1 CHECK (id > 0) $expectedDDL")
      }
    }
  }

  test("Create table with check constraint: char/varchar type") {
    Seq(true, false).foreach { preserveCharVarcharTypeInfo =>
      withSQLConf(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key ->
        preserveCharVarcharTypeInfo.toString) {
        Seq("CHAR(10)", "VARCHAR(10)").foreach { dt =>
          withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
            val constraintStr = "CONSTRAINT c1 CHECK (LENGTH(name) > 0)"
            sql(s"CREATE TABLE $t (id bigint, name $dt, $constraintStr) $defaultUsing")
            val table = loadTable(nonPartitionCatalog, "ns", "tbl")
            val constraint = getCheckConstraint(table)
            assert(constraint.name() == "c1")
            assert(constraint.toDDL ==
              s"CONSTRAINT c1 CHECK (LENGTH(name) > 0) ENFORCED UNVALIDATED NORELY")
            assert(constraint.predicateSql() == "LENGTH(name) > 0")
          }
        }
      }
    }
  }

  test("Replace table with check constraint") {
    getConstraintCharacteristics().foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        val constraintStr = s"CONSTRAINT c1 CHECK (id > 0) $characteristic"
        sql(s"CREATE TABLE $t (id bigint) $defaultUsing")
        sql(s"REPLACE TABLE $t (id bigint, data string, $constraintStr) $defaultUsing")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        val constraint = getCheckConstraint(table)
        assert(constraint.name() == "c1")
        assert(constraint.toDDL == s"CONSTRAINT c1 CHECK (id > 0) $expectedDDL")
      }
    }
  }

  test("Alter table add check constraint") {
    getConstraintCharacteristics().foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
        assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

        sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (id > 0) $characteristic")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        val constraint = getCheckConstraint(table)
        assert(constraint.name() == "c1")
        assert(constraint.toDDL == s"CONSTRAINT c1 CHECK (id > 0) $expectedDDL")
      }
    }
  }

  test("Add duplicated check constraint") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
      assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

      sql(s"ALTER TABLE $t ADD CONSTRAINT abc CHECK (id > 0)")
      // Constraint names are case-insensitive
      Seq("abc", "ABC").foreach { name =>
        val error = intercept[AnalysisException] {
          sql(s"ALTER TABLE $t ADD CONSTRAINT $name CHECK (id>0)")
        }
        checkError(
          exception = error,
          condition = "CONSTRAINT_ALREADY_EXISTS",
          sqlState = "42710",
          parameters = Map("constraintName" -> "abc",
            "oldConstraint" -> "CONSTRAINT abc CHECK (id > 0) ENFORCED UNVALIDATED NORELY")
        )
      }
    }
  }
}
