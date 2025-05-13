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

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
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
      ("NORELY", s"ENFORCED $validStatus NORELY"),
      ("RELY", s"ENFORCED $validStatus RELY"),
      ("ENFORCED", s"ENFORCED $validStatus NORELY"),
      ("ENFORCED NORELY", s"ENFORCED $validStatus NORELY"),
      ("ENFORCED RELY", s"ENFORCED $validStatus RELY")
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
        assert(table.currentVersion() == "1")
        assert(table.validatedVersion() == "0")
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

  test("Check constraint violation on table insert - top level column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, CONSTRAINT positive_id CHECK (id > 0)) $defaultUsing")
      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (-1)")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "positive_id", "expression" -> "id > 0", "values" -> " - id : -1")
      )
    }
  }

  test("Check constraint violation on table insert - nested column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT," +
        s" s STRUCT<num INT, str STRING> CONSTRAINT positive_num CHECK (s.num > 0)) $defaultUsing")

      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (1, struct(-1, 'test'))")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "positive_num",
          "expression" -> "s.num > 0",
          "values" -> " - s.num : -1"
        )
      )
    }
  }

  test("Check constraint violation on table insert - map type column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT," +
        s" m MAP<STRING, INT> CONSTRAINT positive_num CHECK (m['a'] > 0)) $defaultUsing")

      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (1, map('a', -1))")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "positive_num",
          "expression" -> "m['a'] > 0",
          "values" -> " - m['a'] : -1"
        )
      )
    }
  }

  test("Check constraint violation on table insert - array type column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT," +
        s" a ARRAY<INT>, CONSTRAINT positive_array CHECK (a[1] > 0)) $defaultUsing")

      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (1, array(1, -2, 3))")
      }

      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "positive_array",
          "expression" -> "a[1] > 0",
          "values" -> " - a[1] : -2"
        )
      )
    }
  }


  test("Check constraint violation on insert overwrite by position") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" CONSTRAINT positive_value CHECK (value > 0)) $defaultUsing")
      // First insert valid data
      sql(s"INSERT INTO $t VALUES (1, 10)")

      // Try to overwrite with invalid data
      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE TABLE $t SELECT 2, -5")
      }

      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "positive_value",
          "expression" -> "value > 0",
          "values" -> " - value : -5"
        )
      )
    }
  }

  test("Check constraint violation on insert overwrite by name") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" CONSTRAINT positive_value CHECK (value > 0)) $defaultUsing")
      // First insert valid data
      sql(s"INSERT INTO $t VALUES (1, 10)")

      // Try to overwrite with invalid data using column names
      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE TABLE $t BY NAME SELECT -5 as value, 2 as id")
      }

      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "positive_value",
          "expression" -> "value > 0",
          "values" -> " - value : -5"
        )
      )
    }
  }

  test("Check constraint validation succeeds on table insert - top level column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, CONSTRAINT positive_id CHECK (id > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1), (null)")
      checkAnswer(spark.table(t), Seq(Row(1), Row(null)))
    }
  }

  test("Check constraint validation succeeds on table insert - nested column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT," +
        s" s STRUCT<num INT, str STRING> CONSTRAINT positive_num CHECK (s.num > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, struct(5, 'test')), (2, struct(null, 'test'))")
      checkAnswer(spark.table(t), Seq(Row(1, Row(5, "test")), Row(2, Row(null, "test"))))
    }
  }

  test("Check constraint validation succeeds on table insert - map type column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT," +
        s" m MAP<STRING, INT> CONSTRAINT positive_num CHECK (m['a'] > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, map('a', 10, 'b', 20)), (2, map('a', null))")
      checkAnswer(spark.table(t), Seq(Row(1, Map("a" -> 10, "b" -> 20)), Row(2, Map("a" -> null))))
    }
  }

  test("Check constraint validation succeeds on table insert - array type column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT," +
        s" a ARRAY<INT>, CONSTRAINT positive_array CHECK (a[1] > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, array(5, 6, 7)), (2, array(8, null))")
      checkAnswer(spark.table(t), Seq(Row(1, Seq(5, 6, 7)), Row(2, Seq(8, null))))
    }
  }
}
