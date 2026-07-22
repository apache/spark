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

package org.apache.spark.sql.connector

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.CheckInvariant
import org.apache.spark.sql.connector.catalog.{InMemoryCatalog, InMemoryRowLevelOperationTableCatalog,
  TableCatalogCapability}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.internal.SQLConf

/**
 * Tests for generated column auto-fill and constraint enforcement during writes.
 */
class GeneratedColumnWriteSuite extends QueryTest with DatasourceV2SQLBase {

  private val rowLevelCat = "rowlevelcat"

  private def withRowLevelCatalog(f: => Unit): Unit = {
    withSQLConf(s"spark.sql.catalog.$rowLevelCat" ->
      classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      f
    }
  }

  // A catalog that supports creating generated columns but does NOT declare
  // SUPPORT_GENERATED_COLUMN_ON_WRITE, so Spark must not auto-fill or enforce them.
  private val noWriteCapCat = "nowritecapcat"

  private def withNoWriteCapCatalog(f: => Unit): Unit = {
    withSQLConf(s"spark.sql.catalog.$noWriteCapCat" ->
      classOf[InMemoryNoGenColWriteCatalog].getName) {
      f
    }
  }

  private def hasCheckInvariant(sqlText: String): Boolean = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sqlText)
    val analyzed = spark.sessionState.analyzer.executeAndCheck(parsed, new QueryPlanningTracker)
    analyzed.exists { node =>
      node.expressions.exists(_.exists(_.isInstanceOf[CheckInvariant]))
    }
  }

  private def sqlDate(str: String): java.sql.Date = java.sql.Date.valueOf(str)

  private def sqlTimestamp(str: String): java.sql.Timestamp = java.sql.Timestamp.valueOf(str)

  // Default Spark DSV2 generated-column write table used by the write-path cases below.
  private def createDefaultGeneratedColumnWriteTable(tableName: String): Unit = {
    sql(s"""CREATE TABLE testcat.$tableName(
           |  c1 BIGINT,
           |  c2_g BIGINT GENERATED ALWAYS AS (c1 + 10),
           |  c3_p STRING,
           |  c4_g_p DATE GENERATED ALWAYS AS (CAST(c5 AS DATE)),
           |  c5 TIMESTAMP,
           |  c6 INT,
           |  c7_g_p INT GENERATED ALWAYS AS (c6 * 10),
           |  c8 DATE
           |) USING foo PARTITIONED BY (c3_p, c4_g_p, c7_g_p)""".stripMargin)
  }

  private def defaultGeneratedColumnWriteRow: Row = {
    Row(1L, 11L, "foo", sqlDate("2020-10-11"),
      sqlTimestamp("2020-10-11 12:30:30"), 100, 1000, sqlDate("2020-11-12"))
  }

  private def testGeneratedColumnWrite(
      testName: String)(updateFunc: String => Seq[Row]): Unit = {
    test(s"DSV2 generated column write - $testName") {
      val tableName = testName
      withTable(s"testcat.$tableName") {
        createDefaultGeneratedColumnWriteTable(tableName)
        val expected = updateFunc(s"testcat.$tableName")
        checkAnswer(spark.table(s"testcat.$tableName"), expected)
      }
    }
  }

  private def testGeneratedColumnDynamicOverwrite(
      testName: String)(updateFunc: String => Seq[Row]): Unit = {
    testGeneratedColumnWrite(s"dpo_$testName") { table =>
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
          SQLConf.PartitionOverwriteMode.DYNAMIC.toString) {
        updateFunc(table)
      }
    }
  }

  testGeneratedColumnWrite("append_data_v2") { table =>
    import testImplicits._
    Seq(Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12"))
      .toDF("c1", "c3_p", "c5", "c6", "c8")
      .withColumn("c5", $"c5".cast("timestamp"))
      .withColumn("c8", $"c8".cast("date"))
      .writeTo(table)
      .append()
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("append_data_in_different_column_order_v2") { table =>
    import testImplicits._
    Seq(Tuple5("2020-10-11 12:30:30", 100, "2020-11-12", 1L, "foo"))
      .toDF("c5", "c6", "c8", "c1", "c3_p")
      .withColumn("c5", $"c5".cast("timestamp"))
      .withColumn("c8", $"c8".cast("date"))
      .writeTo(table)
      .append()
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_values_provide_all_columns") { table =>
    sql(s"""INSERT INTO $table VALUES (
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_provide_all_columns") { table =>
    sql(s"""INSERT INTO $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12',
           |  1L, 11L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_not_provide_generated_columns") { table =>
    sql(s"""INSERT INTO $table (c6, c8, c1, c3_p, c5)
           |VALUES (100, DATE'2020-11-12', 1L, 'foo',
           |  TIMESTAMP'2020-10-11 12:30:30')""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_with_some_generated_columns") { table =>
    sql(s"""INSERT INTO $table (c5, c6, c8, c1, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, DATE'2020-11-12',
           |  1L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_select_provide_all_columns") { table =>
    sql(s"""INSERT INTO $table SELECT
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_not_provide_normal_columns") { table =>
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      val ex = intercept[AnalysisException] {
        sql(s"INSERT INTO $table (c6, c8, c1, c3_p) " +
          s"VALUES (100, DATE'2020-11-12', 1L, 'foo')")
      }
      assert(ex.getMessage.contains("c5"))
    }
    Nil
  }

  testGeneratedColumnWrite("insert_overwrite_values_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table VALUES (
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_select_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table SELECT
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12',
           |  1L, 11L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_not_provide_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c6, c8, c1, c3_p, c5)
           |VALUES (100, DATE'2020-11-12', 1L, 'foo',
           |  TIMESTAMP'2020-10-11 12:30:30')""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_with_some_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c8, c1, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, DATE'2020-11-12',
           |  1L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_not_provide_normal_columns") { table =>
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      val ex = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p) " +
          s"VALUES (100, DATE'2020-11-12', 1L, 'foo')")
      }
      assert(ex.getMessage.contains("c5"))
    }
    Nil
  }

  testGeneratedColumnDynamicOverwrite("insert_overwrite_values_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table VALUES (
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite("insert_overwrite_select_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table SELECT
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_values_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12',
           |  1L, 11L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_not_provide_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c6, c8, c1, c3_p, c5)
           |VALUES (100, DATE'2020-11-12', 1L, 'foo',
           |  TIMESTAMP'2020-10-11 12:30:30')""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_with_some_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c8, c1, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, DATE'2020-11-12',
           |  1L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_not_provide_normal_columns") { table =>
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      val ex = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p) " +
          s"VALUES (100, DATE'2020-11-12', 1L, 'foo')")
      }
      assert(ex.getMessage.contains("c5"))
    }
    Nil
  }

  test("INSERT by name auto-fills missing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2024-06-15')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT by name computes generated column from the stored (post-cast) value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)
      // 2.9 is cast to INT (= 2) when stored in `id`. The generated column must be computed from
      // the stored value (2 * 2 = 4), not from the pre-cast value (2.9 * 2 = 5.8 -> 5). This
      // mirrors the by-position path, which resolves the generation expression against the
      // post-cast columns.
      sql(s"INSERT INTO testcat.$tblName(id) VALUES (2.9)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(2, 4))
    }
  }

  test("INSERT by position validates generated column against the stored (post-cast) value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName VALUES (2.9, 4)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(2, 4))
    }
  }

  test("INSERT by name with matching explicit value succeeds") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate, eventYear) VALUES (DATE'2024-06-15', 2024)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT by name with non-matching explicit value fails") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(eventDate, eventYear) VALUES (DATE'2024-06-15', 2025)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT by position does not auto-fill trailing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      // By-position writes require values for all target columns. To omit generated columns,
      // use a by-name write with an explicit column list.
      val ex = intercept[AnalysisException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-06-15')")
      }
      assert(ex.getMessage.contains("not enough data columns"))
    }
  }

  test("INSERT by position with matching explicit value succeeds") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-06-15', 2024)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT by position with non-matching explicit value fails") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-06-15', 2025)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT auto-fills multiple generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate)),
             |  eventMonth INT GENERATED ALWAYS AS (month(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2024-06-15')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024, 6))
    }
  }

  test("INSERT with expression referencing multiple columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT,
             |  c INT GENERATED ALWAYS AS (a + b)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (3, 5)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(3, 5, 8))
    }
  }

  test("allowNullableIngest config controls missing non-generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b STRING,
             |  c INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      // Config ON (default): missing nullable column 'b' is filled with null
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, null, 2))
      // Config OFF: missing nullable column 'b' causes an error
      withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
        val ex = intercept[AnalysisException] {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES (2)")
        }
        assert(ex.getMessage.contains("b"))
      }
    }
  }

  test("DataFrame writeTo append auto-fills generated columns with input columns reordered") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING,
             |  d INT GENERATED ALWAYS AS (length(c))
             |) USING foo""".stripMargin)
      Seq(("hello", 7), ("spark", 3)).toDF("c", "a").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(7, 8, "hello", 5) :: Row(3, 4, "spark", 5) :: Nil)
    }
  }

  test("DataFrame writeTo append obeys allowNullableIngest for missing non-generated column") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  optional STRING,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)

      Seq((1, "one")).toDF("a", "optional").writeTo(s"testcat.$tblName").append()
      Seq(2).toDF("a").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, "one", 2) :: Row(2, null, 3) :: Nil)

      withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
        val ex = intercept[AnalysisException] {
          Seq(3).toDF("a").writeTo(s"testcat.$tblName").append()
        }
        assert(ex.getMessage.contains("optional"))
      }
    }
  }

  test("missing NOT NULL column fails with generated columns") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT NOT NULL,
             |  required STRING NOT NULL,
             |  b INT GENERATED ALWAYS AS (a + 1) NOT NULL
             |) USING foo""".stripMargin)

      val ex = intercept[AnalysisException] {
        Seq(1).toDF("a").writeTo(s"testcat.$tblName").append()
      }
      assert(ex.getMessage.contains("required"))
    }
  }

  test("works alongside table CHECK constraints") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      sql(s"ALTER TABLE testcat.$tblName ADD CONSTRAINT positive_a CHECK (a > 0)")

      // Both constraints pass: a > 0 and b = a + 1 (auto-filled)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (5)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(5, 6))

      // Table CHECK constraint fails: a <= 0
      val ex1 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a) VALUES (-1)")
      }
      assert(ex1.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex1.getMessage.contains("positive_a"))

      // Generated column constraint fails: user provides wrong b
      val ex2 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, 999)")
      }
      assert(ex2.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex2.getMessage.contains("Generated Column"))
    }
  }

  test("NULL input produces NULL generated column value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (NULL)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(null, null))
    }
  }

  test("type coercion in generated column expression") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b LONG GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (100)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(100, 101L))
    }
  }

  test("multiple rows each get their own generated value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1), (2), (3)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, 10) :: Row(2, 20) :: Row(3, 30) :: Nil)
    }
  }

  test("generated column in the middle of schema") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (5, 'hello')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(5, 6, "hello"))
    }
  }

  test("NULL explicit value matching NULL generation result succeeds") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      // NULL <=> year(NULL) -> NULL <=> NULL -> true (EqualNullSafe)
      sql(s"INSERT INTO testcat.$tblName(eventDate, eventYear) VALUES (NULL, NULL)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(null, null))
    }
  }

  test("NULL explicit value for generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      // NULL <=> (a + 1) where a=5 -> NULL <=> 6 -> false -> violation
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, NULL)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT OVERWRITE with generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      sql(s"INSERT OVERWRITE testcat.$tblName(a) VALUES (10)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(10, 11))
    }
  }

  test("INSERT OVERWRITE by name with all generated columns validates constraints") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING,
             |  d INT GENERATED ALWAYS AS (length(c))
             |) USING foo""".stripMargin)

      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (1, 'old')")
      sql(s"INSERT OVERWRITE testcat.$tblName(d, c, b, a) VALUES (3, 'new', 8, 7)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(7, 8, "new", 3))

      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE testcat.$tblName(d, c, b, a) VALUES (3, 'bad', 99, 7)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex.getMessage.contains("Generated Column"))
    }
  }

  test("INSERT OVERWRITE by name with some generated columns auto-fills the rest") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)

      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      sql(s"INSERT OVERWRITE testcat.$tblName(c, a) VALUES (70, 7)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(7, 8, 70))

      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE testcat.$tblName(c, a) VALUES (99, 7)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT OVERWRITE SELECT with all generated column values validates constraints") {
    val tblName = "my_tab"
    val srcName = "src_tab"
    withTable(s"testcat.$tblName", s"testcat.$srcName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      sql(s"CREATE TABLE testcat.$srcName(a INT, b INT, c STRING) USING foo")
      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (0, 'old')")
      sql(s"INSERT INTO testcat.$srcName VALUES (1, 2, 'ok')")
      sql(s"INSERT OVERWRITE testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(1, 2, "ok"))

      sql(s"INSERT OVERWRITE testcat.$srcName VALUES (2, 99, 'bad')")
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex.getMessage.contains("Generated Column"))
    }
  }

  test("DataFrame writeTo append with missing generated column") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a * 3)
             |) USING foo""".stripMargin)
      Seq(4, 5).toDF("a").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(4, 12) :: Row(5, 15) :: Nil)
    }
  }

  test("INSERT by position does not auto-fill non-trailing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT GENERATED ALWAYS AS (b + 1),
             |  b INT
             |) USING foo""".stripMargin)
      // By-position writes require values for all target columns. To omit generated columns,
      // use a by-name write with an explicit column list.
      val ex = intercept[AnalysisException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (10)")
      }
      assert(ex.getMessage.contains("not enough data columns"))
    }
  }

  test("INSERT by name with columns in different order") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      // Provide columns in reverse order
      sql(s"INSERT INTO testcat.$tblName(c, a) VALUES ('hello', 7)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(7, 8, "hello"))
    }
  }

  test("INSERT SELECT auto-fills generated columns") {
    val tblName = "my_tab"
    val srcName = "src_tab"
    withTable(s"testcat.$tblName", s"testcat.$srcName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)
      sql(s"CREATE TABLE testcat.$srcName(a INT) USING foo")
      sql(s"INSERT INTO testcat.$srcName VALUES (1), (2), (3)")
      sql(s"INSERT INTO testcat.$tblName(a) SELECT a FROM testcat.$srcName")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, 10) :: Row(2, 20) :: Row(3, 30) :: Nil)
    }
  }

  test("INSERT SELECT with all generated column values validates constraints") {
    val tblName = "my_tab"
    val srcName = "src_tab"
    withTable(s"testcat.$tblName", s"testcat.$srcName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      sql(s"CREATE TABLE testcat.$srcName(a INT, b INT, c STRING) USING foo")
      sql(s"INSERT INTO testcat.$srcName VALUES (1, 2, 'ok')")
      sql(s"INSERT INTO testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(1, 2, "ok"))

      sql(s"INSERT OVERWRITE testcat.$srcName VALUES (2, 99, 'bad')")
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex.getMessage.contains("Generated Column"))
    }
  }

  test("INSERT with complex generation expression using CAST") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  ts TIMESTAMP,
             |  ts_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(ts) VALUES (TIMESTAMP'2024-06-15 10:30:00')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Timestamp.valueOf("2024-06-15 10:30:00"),
          java.sql.Date.valueOf("2024-06-15")))
    }
  }

  test("INSERT with case-insensitive column matching") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  EventDate DATE,
             |  EventYear INT GENERATED ALWAYS AS (year(EventDate))
             |) USING foo""".stripMargin)
      // Use different case in INSERT column list
      sql(s"INSERT INTO testcat.$tblName(eventdate) VALUES (DATE'2024-06-15')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT missing required non-generated column fails") {
    val tblName = "my_tab"
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      withTable(s"testcat.$tblName") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a INT,
               |  b STRING,
               |  c INT GENERATED ALWAYS AS (a + 1)
               |) USING foo""".stripMargin)
        // Missing non-generated, non-nullable column 'b' should fail
        // when allowNullableIngest is off
        val ex = intercept[AnalysisException] {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
        }
        assert(ex.getMessage.contains("b"))
      }
    }
  }

  test("INSERT with generated partition column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo PARTITIONED BY (eventYear)""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2024-06-15')")
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2023-01-01')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024) ::
          Row(java.sql.Date.valueOf("2023-01-01"), 2023) :: Nil)
    }
  }

  test("CTAS auto-fills generated columns") {
    val src = "src_tab"
    val tgt = "tgt_tab"
    withTable(s"testcat.$src", s"testcat.$tgt") {
      sql(s"CREATE TABLE testcat.$src(a INT, b INT) USING foo")
      sql(s"INSERT INTO testcat.$src VALUES (3, 5)")
      sql(s"""CREATE TABLE testcat.$tgt(
             |  a INT,
             |  b INT,
             |  c INT GENERATED ALWAYS AS (a + b)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tgt(a, b) SELECT a, b FROM testcat.$src")
      checkAnswer(
        spark.table(s"testcat.$tgt"),
        Row(3, 5, 8))
    }
  }

  test("generated column constraint supports null values") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  c1 STRING,
             |  c2 STRING GENERATED ALWAYS AS (concat(c1, 'y'))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName VALUES ('x', 'xy')")
      sql(s"INSERT INTO testcat.$tblName VALUES (NULL, NULL)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("x", "xy") :: Row(null, null) :: Nil)

      val ex1 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName VALUES ('foo', NULL)")
      }
      assert(ex1.getCondition == "CHECK_CONSTRAINT_VIOLATION")

      val ex2 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (NULL, 'foo')")
      }
      assert(ex2.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("generated column with dotted column names and case-insensitive insert columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  `a.b` STRING GENERATED ALWAYS AS (concat(`c.d`, 'y')),
             |  `c.d` STRING
             |) USING foo""".stripMargin)

      sql(s"INSERT INTO testcat.$tblName VALUES ('xy', 'x')")
      sql(s"INSERT INTO testcat.$tblName(`c.D`, `a.B`) VALUES ('x', 'xy')")
      sql(s"INSERT INTO testcat.$tblName(`c.D`) VALUES ('x')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("xy", "x") :: Row("xy", "x") :: Row("xy", "x") :: Nil)

      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(`a.B`) VALUES ('xy')")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("generated column with complex type extractors") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  `a.b` STRING,
             |  a STRUCT<b: INT, c: STRING>,
             |  array ARRAY<INT>,
             |  c1 STRING GENERATED ALWAYS AS (concat(`a.b`, 'b')),
             |  c2 INT GENERATED ALWAYS AS (a.b + 100),
             |  c3 INT GENERATED ALWAYS AS (array[1])
             |) USING foo""".stripMargin)
      sql(s"""INSERT INTO testcat.$tblName(`a.b`, a, array)
             |VALUES ('a', named_struct('b', 100, 'c', 'foo'), array(1000, 1001))
             |""".stripMargin)
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("a", Row(100, "foo"), Seq(1000, 1001), "ab", 200, 1001))
    }
  }

  test("TIME type in generated columns is not supported") {
    val tblName = "my_tab"
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      withTable(s"testcat.$tblName") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""CREATE TABLE testcat.$tblName(
                   |  id INT,
                   |  event_time TIME GENERATED ALWAYS AS (CAST('12:00:00' AS TIME))
                   |) USING foo""".stripMargin)
          },
          condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
          parameters = Map(
            "fieldName" -> "event_time",
            "expressionStr" -> "CAST('12:00:00' AS TIME)",
            "reason" -> "TIME type is not supported in generated columns"))
      }
    }
  }

  test("streaming write with generated columns is blocked") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withTempDir { checkpointDir =>
        sql(s"""CREATE TABLE testcat.$tblName(
               |  id INT,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val inputData = MemoryStream[Int]
        val df = inputData.toDF().toDF("id")
        val ex = intercept[AnalysisException] {
          df.writeStream
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .toTable(s"testcat.$tblName")
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("streaming"))
        assert(ex.getMessage.contains("generated columns"))
      }
    }
  }

  test("INSERT by position validates generated column with type cast on source column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      // Table expects LONG for 'a', generation expression is a + 1
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a LONG,
             |  b LONG GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      // Insert INT values by position -- 'a' gets cast to LONG, then the generated column
      // constraint validates against the cast value.
      sql(s"INSERT INTO testcat.$tblName VALUES (42, 43)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(42L, 43L))
    }
  }

  test("mix of auto-filled and user-provided generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)
      // Provide 'a' and 'c' (user-provided), omit 'b' (auto-filled)
      // b is auto-filled with a + 1 = 6
      // c is user-provided with correct value a * 10 = 50 -> passes constraint
      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (5, 50)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(5, 6, 50))

      // Now provide wrong value for c -> constraint violation on c only
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (5, 999)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("plan has constraint for user-provided but not auto-filled generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)

      // Auto-filled: no CheckInvariant in analyzed plan
      assert(!hasCheckInvariant(s"INSERT INTO testcat.$tblName(a) VALUES (5)"),
        "Auto-filled generated columns should not have CheckInvariant in plan")

      // User-provided: CheckInvariant should appear
      assert(hasCheckInvariant(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, 6)"),
        "User-provided generated columns should have CheckInvariant in plan")
    }
  }

  // MERGE/UPDATE with generated columns are blocked for now; DELETE is allowed.

  test("UPDATE with generated columns is blocked") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val ex = intercept[AnalysisException] {
          sql(s"UPDATE $rowLevelCat.$tblName SET data = 'x' WHERE id = 1")
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("UPDATE with generated columns"))
      }
    }
  }

  test("MERGE with generated columns is blocked") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val ex = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $rowLevelCat.$tblName AS t
               |USING source AS s
               |ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET t.data = s.data
               |WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)
               |""".stripMargin)
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("MERGE with generated columns"))
      }
    }
  }

  test("MERGE with only NOT MATCHED actions and generated columns is blocked") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val ex = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $rowLevelCat.$tblName AS t
               |USING source AS s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)
               |""".stripMargin)
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("MERGE with generated columns"))
      }
    }
  }

  test("DELETE with generated columns is allowed") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        // Partition by id so the metadata delete-by-filter path works on the in-memory table.
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo PARTITIONED BY (id)""".stripMargin)
        sql(s"INSERT INTO $rowLevelCat.$tblName(id, data) VALUES (1, 'a'), (2, 'b')")
        // DELETE is not blocked (only MERGE/UPDATE are), and auto-fill still applies on INSERT.
        sql(s"DELETE FROM $rowLevelCat.$tblName WHERE id = 1")
        checkAnswer(spark.table(s"$rowLevelCat.$tblName"), Row(2, "b", 4))
      }
    }
  }

  test("catalog without write capability does not auto-fill or enforce generated columns") {
    withNoWriteCapCatalog {
      val tblName = "my_tab"
      withTable(s"$noWriteCapCat.$tblName") {
        sql(s"""CREATE TABLE $noWriteCapCat.$tblName(
               |  a INT,
               |  b INT GENERATED ALWAYS AS (a + 1)
               |) USING foo""".stripMargin)

        // A user-provided value that does NOT match the generation expression is written
        // as-is: no constraint is enforced because the catalog does not opt in.
        sql(s"INSERT INTO $noWriteCapCat.$tblName(a, b) VALUES (5, 999)")
        checkAnswer(spark.table(s"$noWriteCapCat.$tblName"), Row(5, 999))

        // Omitting the generated column does not auto-fill; it is treated as a regular
        // nullable column and filled with null.
        sql(s"INSERT INTO $noWriteCapCat.$tblName(a) VALUES (7)")
        checkAnswer(
          spark.table(s"$noWriteCapCat.$tblName"),
          Row(5, 999) :: Row(7, null) :: Nil)

        // No CheckInvariant is added to the plan for the generated column.
        assert(!hasCheckInvariant(s"INSERT INTO $noWriteCapCat.$tblName(a, b) VALUES (5, 6)"),
          "No CheckInvariant should be added when the catalog lacks the write capability")
      }
    }
  }
}

/**
 * A catalog that supports creating tables with generated columns but does NOT declare
 * [[TableCatalogCapability.SUPPORT_GENERATED_COLUMN_ON_WRITE]], so Spark leaves generated
 * column handling to the connector (no auto-fill, no constraint enforcement on write).
 */
class InMemoryNoGenColWriteCatalog extends InMemoryCatalog {
  override def capabilities: java.util.Set[TableCatalogCapability] = {
    val caps = new java.util.HashSet[TableCatalogCapability](super.capabilities)
    caps.remove(TableCatalogCapability.SUPPORT_GENERATED_COLUMN_ON_WRITE)
    caps
  }
}
