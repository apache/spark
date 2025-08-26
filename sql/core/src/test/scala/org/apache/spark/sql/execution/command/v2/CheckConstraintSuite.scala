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
        "CONSTRAINT c1 CHECK (from_json(j, 'a INT').a > 1) ENFORCED NORELY")
      assert(constraint.predicateSql() == "from_json(j, 'a INT').a > 1")
      assert(constraint.predicate() == null)
    }
  }

  def getConstraintCharacteristics(): Seq[(String, String)] = {
    Seq(
      ("", s"ENFORCED NORELY"),
      ("NORELY", s"ENFORCED NORELY"),
      ("RELY", s"ENFORCED RELY"),
      ("ENFORCED", s"ENFORCED NORELY"),
      ("ENFORCED NORELY", s"ENFORCED NORELY"),
      ("ENFORCED RELY", s"ENFORCED RELY")
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
              s"CONSTRAINT c1 CHECK (LENGTH(name) > 0) ENFORCED NORELY")
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
        sql(s"INSERT INTO $t VALUES (1, 'a'), (null, 'b')")
        sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (id > 0) $characteristic")
        val table = loadTable(nonPartitionCatalog, "ns", "tbl")
        assert(table.currentVersion() == "2")
        assert(table.validatedVersion() == "1")
        val constraint = getCheckConstraint(table)
        assert(constraint.name() == "c1")
        assert(constraint.toDDL == s"CONSTRAINT c1 CHECK (id > 0) $expectedDDL")
      }
    }
  }

  test("Alter table add new check constraint with violation") {
    getConstraintCharacteristics().foreach { case (characteristic, expectedDDL) =>
      withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
        sql(s"CREATE TABLE $t (id bigint, data string) $defaultUsing")
        assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)
        sql(s"INSERT INTO $t VALUES (-1, 'a'), (2, 'b')")
        val error = intercept[SparkRuntimeException] {
          sql(s"ALTER TABLE $t ADD CONSTRAINT c1 CHECK (id > 0) $characteristic")
        }
        checkError(
          exception = error,
          condition = "NEW_CHECK_CONSTRAINT_VIOLATION",
          parameters = Map("expression" -> "id > 0", "tableName" -> "non_part_test_catalog.ns.tbl")
        )
        assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)
      }
    }
  }

  test("Alter table add new check constraint with nested column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, s STRUCT<num INT, str STRING>) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, struct(-1, 'test')), (2, struct(5, 'valid'))")

      // Add an invalid check constraint
      val error = intercept[SparkRuntimeException] {
        sql(s"ALTER TABLE $t ADD CONSTRAINT positive_num CHECK (s.num > 0)")
      }
      checkError(
        exception = error,
        condition = "NEW_CHECK_CONSTRAINT_VIOLATION",
        parameters = Map("expression" -> "s.num > 0", "tableName" -> "non_part_test_catalog.ns.tbl")
      )
      assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

      // Add a valid check constraint
      sql(s"ALTER TABLE $t ADD CONSTRAINT valid_positive_num CHECK (s.num >= -1)")
      val table = loadTable(nonPartitionCatalog, "ns", "tbl")
      assert(table.currentVersion() == "2")
      assert(table.validatedVersion() == "1")
      val constraint = getCheckConstraint(table)
      assert(constraint.name() == "valid_positive_num")
      assert(constraint.toDDL ==
        "CONSTRAINT valid_positive_num CHECK (s.num >= -1) ENFORCED NORELY")
    }
  }

  test("Alter table add new check constraint with violation - map type column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, m MAP<STRING, INT>) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, map('a', -1, 'b', 2)), (2, map('a', 5))")

      // Add an invalid check constraint
      val error = intercept[SparkRuntimeException] {
        sql(s"ALTER TABLE $t ADD CONSTRAINT positive_map_val CHECK (m['a'] > 0)")
      }
      checkError(
        exception = error,
        condition = "NEW_CHECK_CONSTRAINT_VIOLATION",
        parameters = Map(
          "expression" -> "m['a'] > 0",
          "tableName" -> "non_part_test_catalog.ns.tbl")
      )
      assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

      // Add a valid check constraint
      sql(s"ALTER TABLE $t ADD CONSTRAINT valid_map_val CHECK (m['a'] >= -1)")
      val table = loadTable(nonPartitionCatalog, "ns", "tbl")
      assert(table.currentVersion() == "2")
      assert(table.validatedVersion() == "1")
      val constraint = getCheckConstraint(table)
      assert(constraint.name() == "valid_map_val")
      assert(constraint.toDDL ==
        "CONSTRAINT valid_map_val CHECK (m['a'] >= -1) ENFORCED NORELY")
    }
  }

  test("Alter table add new check constraint with violation - array type column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, a ARRAY<INT>) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, array(1, -2, 3)), (2, array(5, 6, 7))")

      // Add an invalid check constraint
      val error = intercept[SparkRuntimeException] {
        sql(s"ALTER TABLE $t ADD CONSTRAINT positive_array CHECK (a[1] > 0)")
      }
      checkError(
        exception = error,
        condition = "NEW_CHECK_CONSTRAINT_VIOLATION",
        parameters = Map("expression" -> "a[1] > 0", "tableName" -> "non_part_test_catalog.ns.tbl")
      )
      assert(loadTable(nonPartitionCatalog, "ns", "tbl").constraints.isEmpty)

      // Add a valid check constraint
      sql(s"ALTER TABLE $t ADD CONSTRAINT valid_array CHECK (a[1] >= -2)")
      val table = loadTable(nonPartitionCatalog, "ns", "tbl")
      assert(table.currentVersion() == "2")
      assert(table.validatedVersion() == "1")
      val constraint = getCheckConstraint(table)
      assert(constraint.name() == "valid_array")
      assert(constraint.toDDL ==
        "CONSTRAINT valid_array CHECK (a[1] >= -2) ENFORCED NORELY")
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
            "oldConstraint" -> "CONSTRAINT abc CHECK (id > 0) ENFORCED NORELY")
        )
      }
    }
  }

  test("Check constraint violation on table insert - top level column") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, CONSTRAINT positive_id CHECK (id > 0), " +
        s" CONSTRAINT less_than_100 CHECK(abs(id) < 100)) $defaultUsing")
      var error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (-1)")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "positive_id", "expression" -> "id > 0", "values" -> " - id : -1")
      )

      error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (100)")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "less_than_100", "expression" -> "abs(id) < 100",
            "values" -> " - id : 100")
      )
    }
  }

  test("Check constraint with current_timestamp function") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      // Create table with a constraint using current_timestamp
      sql(s"CREATE TABLE $t (id INT, creation_time TIMESTAMP, " +
        s"CONSTRAINT valid_time CHECK (creation_time <= current_timestamp())) $defaultUsing")

      // Insert valid data (current or past timestamp)
      sql(s"INSERT INTO $t VALUES (1, current_timestamp()), (2, TIMESTAMP '2020-01-01 00:00:00')")
      checkAnswer(spark.table(t).select("id"), Seq(Row(1), Row(2)))

      // Insert valid data with null timestamp
      sql(s"INSERT INTO $t VALUES (3, null)")
      checkAnswer(spark.table(t).select("id"), Seq(Row(1), Row(2), Row(3)))

      // Future timestamp should fail validation
      val tomorrow = "current_timestamp() + INTERVAL 1 DAY"
      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (4, $tomorrow)")
      }
      assert(error.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
      assert(error.getMessageParameters.get("constraintName") == "valid_time")
      assert(error.getMessageParameters.get("expression") == "creation_time <= current_timestamp()")
    }
  }

  test("Check constraint with current_timestamp function - update operation") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      // Create table with a constraint using current_timestamp
      sql(s"CREATE TABLE $t (id INT, creation_time TIMESTAMP, " +
        s"CONSTRAINT valid_time CHECK (creation_time <= current_timestamp())) $defaultUsing")

      // Insert initial data
      sql(s"INSERT INTO $t VALUES (1, current_timestamp()), (2, TIMESTAMP '2020-01-01 00:00:00')")

      // Valid update with current or past timestamp
      sql(s"UPDATE $t SET creation_time = TIMESTAMP '2021-01-01 00:00:00' WHERE id = 1")
      checkAnswer(spark.table(t).select("id")
        .where("creation_time = TIMESTAMP '2021-01-01 00:00:00'"),
        Seq(Row(1)))

      // Valid update with null timestamp
      sql(s"UPDATE $t SET creation_time = null WHERE id = 2")
      checkAnswer(sql(s"SELECT * FROM $t WHERE id = 2"), Seq(Row(2, null)))

      // Future timestamp should fail validation
      val tomorrow = "current_timestamp() + INTERVAL 1 DAY"
      val error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET creation_time = $tomorrow WHERE id = 1")
      }
      assert(error.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
      assert(error.getMessageParameters.get("constraintName") == "valid_time")
      assert(error.getMessageParameters.get("expression") == "creation_time <= current_timestamp()")
    }
  }

  test("Check constraint with current_timestamp function - merge operation") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "source", rowLevelOPCatalog) { source =>
        // Create target table with constraint using current_timestamp
        sql(s"CREATE TABLE $target (id INT, creation_time TIMESTAMP, " +
          s"CONSTRAINT valid_time CHECK (creation_time <= current_timestamp())) $defaultUsing")

        // Create source table without constraints
        sql(s"CREATE TABLE $source (id INT, creation_time TIMESTAMP) $defaultUsing")

        // Insert initial data
        sql(s"INSERT INTO $target VALUES (1, TIMESTAMP '2020-01-01 00:00:00')")
        sql(s"INSERT INTO $source VALUES " +
          s"(2, TIMESTAMP '2021-01-01 00:00:00'), " +
          s"(3, current_timestamp()), " +
          s"(4, null)")

        // Valid merge with past timestamps or null
        sql(
          s"""
             |MERGE INTO $target t
             |USING (SELECT * FROM $source WHERE id IN (2, 4)) s
             |ON t.id = s.id
             |WHEN MATCHED THEN UPDATE SET creation_time = s.creation_time
             |WHEN NOT MATCHED THEN INSERT (id, creation_time) VALUES (s.id, s.creation_time)
             |""".stripMargin)

        checkAnswer(spark.table(target).orderBy("id"),
          Seq(Row(1, java.sql.Timestamp.valueOf("2020-01-01 00:00:00")),
              Row(2, java.sql.Timestamp.valueOf("2021-01-01 00:00:00")),
              Row(4, null)))

        // Future timestamp should fail validation
        val tomorrow = "current_timestamp() + INTERVAL 1 DAY"
        sql(s"INSERT INTO $source VALUES (5, $tomorrow)")

        val error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING (SELECT * FROM $source WHERE id = 5) s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT (id, creation_time) VALUES (s.id, s.creation_time)
               |""".stripMargin)
        }

        assert(error.getMessage.contains("CHECK_CONSTRAINT_VIOLATION"))
        assert(error.getMessageParameters.get("constraintName") == "valid_time")
        assert(error.getMessageParameters.get("expression") ==
          "creation_time <= current_timestamp()")
      }
    }
  }

  test("Check constraint with current_date function") {
    // Create another table with other current_* functions
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, creation_date DATE, " +
        s"CONSTRAINT valid_date CHECK (creation_date <= current_date())) $defaultUsing")

      // Insert valid data (current or past timestamp)
      sql(s"INSERT INTO $t VALUES (1, current_date()), (2, DATE'2020-01-01')")
      checkAnswer(spark.table(t).select("id"), Seq(Row(1), Row(2)))

      // Future date should fail validation
      val tomorrow = "DATE'9999-12-31'"
      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (3, $tomorrow)")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "valid_date",
          "expression" -> "creation_date <= current_date()",
          "values" -> " - creation_date : 2932896"
        )
      )
    }
  }

  test("Check constraint with current_database function") {
    withNamespaceAndTable("test_db", "tbl", nonPartitionCatalog) { t =>
      sql(s"USE $nonPartitionCatalog.test_db")
      sql(s"CREATE TABLE $t (id INT, db STRING, " +
        s"CONSTRAINT valid_db CHECK (db = current_database())) $defaultUsing")

      // Insert valid data (current database)
      sql(s"INSERT INTO $t VALUES (1, current_database()), (2, 'test_db')")
      checkAnswer(spark.table(t).select("id"), Seq(Row(1), Row(2)))

      // Invalid database should fail validation
      val error = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO $t VALUES (3, 'invalid_db')")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "valid_db",
          "expression" -> "db = current_database()",
          "values" -> " - db : invalid_db"
        )
      )
    }
  }

  test("Check constraint with current_database function - update operation") {
    withNamespaceAndTable("test_db", "tbl", rowLevelOPCatalog) { t =>
      sql(s"USE $rowLevelOPCatalog.test_db")
      sql(s"CREATE TABLE $t (id INT, db STRING, " +
        s"CONSTRAINT valid_db CHECK (db = current_database())) $defaultUsing")

      // Insert initial valid data
      sql(s"INSERT INTO $t VALUES (1, current_database()), (2, 'test_db')")
      checkAnswer(spark.table(t).select("id"), Seq(Row(1), Row(2)))

      // Valid update with current database value
      sql(s"UPDATE $t SET db = 'test_db' WHERE id = 1")
      checkAnswer(spark.table(t).where("id = 1"), Seq(Row(1, "test_db")))

      // Invalid database should fail validation
      val error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET db = 'invalid_db' WHERE id = 2")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "valid_db",
          "expression" -> "db = current_database()",
          "values" -> " - db : invalid_db"
        )
      )
    }
  }

  test("Check constraint with current_database function - merge operation") {
    withNamespaceAndTable("test_db", "target", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("test_db", "source", rowLevelOPCatalog) { source =>
        sql(s"USE $rowLevelOPCatalog.test_db")
        sql(s"CREATE TABLE $target (id INT, db STRING, " +
          s"CONSTRAINT valid_db CHECK (db = current_database())) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, db STRING) $defaultUsing")

        // Insert initial valid data
        sql(s"INSERT INTO $target VALUES (1, current_database()), (2, 'test_db')")
        sql(s"INSERT INTO $source VALUES (3, 'test_db'), (4, 'invalid_db')")

        // Valid merge with current database value
        sql(
          s"""
             |MERGE INTO $target t
             |USING (SELECT * FROM $source WHERE id = 3) s
             |ON t.id = s.id
             |WHEN NOT MATCHED THEN INSERT (id, db) VALUES (s.id, s.db)
             |""".stripMargin)
        checkAnswer(spark.table(target).orderBy("id"),
          Seq(Row(1, "test_db"), Row(2, "test_db"), Row(3, "test_db")))

        // Invalid database should fail validation
        val error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING (SELECT * FROM $source WHERE id = 4) s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT (id, db) VALUES (s.id, s.db)
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters = Map(
            "constraintName" -> "valid_db",
            "expression" -> "db = current_database()",
            "values" -> " - db : invalid_db"
          )
        )
      }
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

  test("Check constraint violation on table update - top level column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" CONSTRAINT positive_id CHECK (id > 0)," +
        s" CONSTRAINT less_than_100 CHECK(abs(value) < 100)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (5, 10)")
      var error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET id = -1 WHERE value = 10")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "positive_id", "expression" -> "id > 0", "values" -> " - id : -1")
      )

      error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET value = -100 WHERE id = 5")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "less_than_100", "expression" -> "abs(value) < 100",
            "values" -> " - value : -100")
      )
    }
  }

  test("Check constraint violation on table update - nested column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" s STRUCT<num INT, str STRING> CONSTRAINT positive_num CHECK (s.num > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (5, 10, struct(5, 'test'))")
      val error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET s = named_struct('num', -1, 'str', 'test') WHERE value = 10")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "positive_num",
            "expression" -> "s.num > 0", "values" -> " - s.num : -1")
      )
    }
  }

  test("Check constraint violation on table update - map type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" m MAP<STRING, INT> CONSTRAINT positive_num CHECK (m['a'] > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (5, 10, map('a', 5))")
      val error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET m = map('a', -1) WHERE value = 10")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "positive_num",
            "expression" -> "m['a'] > 0", "values" -> " - m['a'] : -1")
      )
    }
  }

  test("Check constraint violation on table update - array type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" a ARRAY<INT>, CONSTRAINT positive_array CHECK (a[1] > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (5, 10, array(5, 6))")
      val error = intercept[SparkRuntimeException] {
        sql(s"UPDATE $t SET a = array(1, -2) WHERE value = 10")
      }
      checkError(
        exception = error,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters =
          Map("constraintName" -> "positive_array",
            "expression" -> "a[1] > 0", "values" -> " - a[1] : -2")
      )
    }
  }

  test("Check constraint violation on table merge - top level column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" CONSTRAINT positive_id CHECK (id > 0)," +
          s" CONSTRAINT less_than_100 CHECK(abs(id) < 100)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (5, 10)")
        sql(s"INSERT INTO $source VALUES (-1, 20)")

        var error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING $source s
               |ON t.value = s.value
               |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (s.id, s.value)
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters =
            Map("constraintName" -> "positive_id", "expression" -> "id > 0",
              "values" -> " - id : -1")
        )

        error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING $source s
               |ON t.value = s.value
               |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (100, s.value)
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters =
            Map("constraintName" -> "less_than_100", "expression" -> "abs(id) < 100",
              "values" -> " - id : 100")
        )

        error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING $source s
               |ON t.value = s.value
               |WHEN MATCHED THEN UPDATE SET id = s.id
               |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (s.id, s.value)
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters =
            Map("constraintName" -> "positive_id", "expression" -> "id > 0",
              "values" -> " - id : -1")
        )
      }
    }
  }

  test("Check constraint violation on table merge - nested column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" s STRUCT<num INT, str STRING> " +
          s"CONSTRAINT positive_num CHECK (s.num > 0)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (5, 10, struct(5, 'test'))")
        sql(s"INSERT INTO $source VALUES (-1, 20)")

        val error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING $source s
               |ON t.value = s.value
               |WHEN NOT MATCHED THEN INSERT(id, value, s) VALUES
               |  (s.id, s.value, named_struct('num', -1, 'str', 'test'))
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters =
            Map("constraintName" -> "positive_num", "expression" -> "s.num > 0",
              "values" -> " - s.num : -1")
        )
      }
    }
  }

  test("Check constraint violation on table merge - map type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" m MAP<STRING, INT> CONSTRAINT positive_num CHECK (m['a'] > 0)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (5, 10, map('a', 5))")
        sql(s"INSERT INTO $source VALUES (-1, 20)")

        val error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING $source s
               |ON t.value = s.value
               |WHEN NOT MATCHED THEN INSERT(id, value, m) VALUES (s.id, s.value, map('a', -1))
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters =
            Map("constraintName" -> "positive_num", "expression" -> "m['a'] > 0",
              "values" -> " - m['a'] : -1")
        )
      }
    }
  }

  test("Check constraint violation on table merge - array type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" a ARRAY<INT>, CONSTRAINT positive_array CHECK (a[1] > 0)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (5, 10, array(5, 6))")
        sql(s"INSERT INTO $source VALUES (-1, 20)")

        val error = intercept[SparkRuntimeException] {
          sql(
            s"""
               |MERGE INTO $target t
               |USING $source s
               |ON t.value = s.value
               |WHEN NOT MATCHED THEN INSERT(id, value, a) VALUES (s.id, s.value, array(1, -2))
               |""".stripMargin)
        }
        checkError(
          exception = error,
          condition = "CHECK_CONSTRAINT_VIOLATION",
          sqlState = "23001",
          parameters =
            Map("constraintName" -> "positive_array", "expression" -> "a[1] > 0",
              "values" -> " - a[1] : -2")
        )
      }
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

  test("Check constraint validation succeeds on table insert - top level column with function") {
    withNamespaceAndTable("ns", "tbl", nonPartitionCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, CONSTRAINT less_than_10 CHECK (abs(id) < 10)) $defaultUsing")
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

  test("Check constraint validation succeeds on table update - top level column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" CONSTRAINT positive_id CHECK (id > 0)," +
        s" CONSTRAINT less_than_100 CHECK(abs(value) < 100)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, 10), (2, 20)")
      sql(s"UPDATE $t SET id = null WHERE value = 10")
      checkAnswer(spark.table(t), Seq(Row(null, 10), Row(2, 20)))
    }
  }

  test("Check constraint validation succeeds on table update - nested column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" s STRUCT<num INT, str STRING> CONSTRAINT positive_num CHECK (s.num > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, 10, struct(5, 'test')), (2, 20, struct(10, 'test'))")
      sql(s"UPDATE $t SET s = named_struct('num', null, 'str', 'test') WHERE value = 10")
      checkAnswer(spark.table(t), Seq(Row(1, 10, Row(null, "test")), Row(2, 20, Row(10, "test"))))
    }
  }

  test("Check constraint validation succeeds on table update - map type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" m MAP<STRING, INT> CONSTRAINT positive_num CHECK (m['a'] > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, 10, map('a', 5)), (2, 20, map('b', 10))")
      sql(s"UPDATE $t SET m = map('a', null) WHERE value = 10")
      checkAnswer(spark.table(t), Seq(Row(1, 10, Map("a" -> null)), Row(2, 20, Map("b" -> 10))))
    }
  }

  test("Check constraint validation succeeds on table update - array type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { t =>
      sql(s"CREATE TABLE $t (id INT, value INT," +
        s" a ARRAY<INT>, CONSTRAINT positive_array CHECK (a[1] > 0)) $defaultUsing")
      sql(s"INSERT INTO $t VALUES (1, 10, array(5, 6)), (2, 20, array(7, 8))")
      sql(s"UPDATE $t SET a = array(null, 1) WHERE value = 10")
      checkAnswer(spark.table(t), Seq(Row(1, 10, Seq(null, 1)), Row(2, 20, Seq(7, 8))))
    }
  }

  test("Check constraint validation succeeds on table merge - top level column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" CONSTRAINT positive_id CHECK (id > 0)," +
          s" CONSTRAINT less_than_100 CHECK(abs(value) < 100)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (1, 10), (2, 20)")
        sql(s"INSERT INTO $source VALUES (3, 30), (4, 40)")

        sql(
          s"""
             |MERGE INTO $target t
             |USING $source s
             |ON t.value = s.value
             |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (s.id, s.value)
             |""".stripMargin)
        checkAnswer(spark.table(target), Seq(Row(1, 10), Row(2, 20), Row(3, 30), Row(4, 40)))
      }
    }
  }

  test("Check constraint validation succeeds on table merge - nested column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" s STRUCT<num INT, str STRING> " +
          s"CONSTRAINT positive_num CHECK (s.num > 0)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (1, 10, struct(5, 'test')), (2, 20, struct(10, 'test'))")
        sql(s"INSERT INTO $source VALUES (3, 30), (4, 40)")

        sql(
          s"""
             |MERGE INTO $target t
             |USING $source s
             |ON t.value = s.value
             |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (s.id, s.value)
             |""".stripMargin)
        checkAnswer(spark.table(target),
          Seq(Row(1, 10, Row(5, "test")), Row(2, 20, Row(10, "test")),
            Row(3, 30, null), Row(4, 40, null)))
      }
    }
  }

  test("Check constraint validation succeeds on table merge - map type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" m MAP<STRING, INT> CONSTRAINT positive_num CHECK (m['a'] > 0)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (1, 10, map('a', 5)), (2, 20, map('b', 10))")
        sql(s"INSERT INTO $source VALUES (3, 30), (4, 40)")

        sql(
          s"""
             |MERGE INTO $target t
             |USING $source s
             |ON t.value = s.value
             |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (s.id, s.value)
             |""".stripMargin)
        checkAnswer(spark.table(target),
          Seq(Row(1, 10, Map("a" -> 5)), Row(2, 20, Map("b" -> 10)),
            Row(3, 30, null), Row(4, 40, null)))
      }
    }
  }

  test("Check constraint validation succeeds on table merge - array type column") {
    withNamespaceAndTable("ns", "tbl", rowLevelOPCatalog) { target =>
      withNamespaceAndTable("ns", "tbl2", rowLevelOPCatalog) { source =>
        sql(s"CREATE TABLE $target (id INT, value INT," +
          s" a ARRAY<INT>, CONSTRAINT positive_array CHECK (a[1] > 0)) $defaultUsing")
        sql(s"CREATE TABLE $source (id INT, value INT) $defaultUsing")
        sql(s"INSERT INTO $target VALUES (1, 10, array(5, 6)), (2, 20, array(7, 8))")
        sql(s"INSERT INTO $source VALUES (3, 30), (4, 40)")

        sql(
          s"""
             |MERGE INTO $target t
             |USING $source s
             |ON t.value = s.value
             |WHEN NOT MATCHED THEN INSERT(id, value) VALUES (s.id, s.value)
             |""".stripMargin)
        checkAnswer(spark.table(target),
          Seq(Row(1, 10, Seq(5, 6)), Row(2, 20, Seq(7, 8)),
            Row(3, 30, null), Row(4, 40, null)))
      }
    }
  }
}
