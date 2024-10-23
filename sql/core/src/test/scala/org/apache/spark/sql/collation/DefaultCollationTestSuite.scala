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

package org.apache.spark.sql.collation

import org.apache.spark.sql.{AnalysisException, DataFrame, Row}
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.types.StringType

class DefaultCollationTestSuite extends DatasourceV2SQLBase {

  val dataSource: String = "parquet"

  def withSessionCollationAndTable(collation: String, tableNames: String*)(f: => Unit): Unit = {
    withTable(tableNames: _*) {
      withSessionCollation(collation) {
        f
      }
    }
  }

  def withSessionCollationAndView(collation: String, viewNames: String*)(f: => Unit): Unit = {
    withView(viewNames: _*) {
      withSessionCollation(collation) {
        f
      }
    }
  }

  def withSessionCollation(collation: String)(f: => Unit): Unit = {
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collation) {
      f
    }
  }

  def assertTableColumnCollation(
      table: String,
      column: String,
      expectedCollation: String): Unit = {
    val colType = spark.table(table).schema(column).dataType
    assert(colType === StringType(expectedCollation))
  }

  def assertThrowsImplicitMismatch(f: => DataFrame): Unit = {
    val exception = intercept[AnalysisException] {
      f
    }
    assert(exception.getCondition === "COLLATION_MISMATCH.IMPLICIT")
  }

  // region DDL tests

  test("create/alter table") {
    val tableName = "tbl"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      // create table with implicit collation
      sql(s"CREATE TABLE $tableName (c1 STRING) USING $dataSource")
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")

      // alter table add column with implicit collation
      sql(s"ALTER TABLE $tableName ADD COLUMN c2 STRING")
      assertTableColumnCollation(tableName, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $tableName ALTER COLUMN c2 TYPE STRING COLLATE UNICODE")
      assertTableColumnCollation(tableName, "c2", "UNICODE")

      sql(s"ALTER TABLE $tableName ALTER COLUMN c2 TYPE STRING")
      assertTableColumnCollation(tableName, "c2", "UTF8_BINARY")
    }
  }

  test("create table with explicit collation") {
    val tableName = "tbl_explicit_collation"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      assertTableColumnCollation(tableName, "c1", "UTF8_LCASE")
    }

    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE UNICODE) USING $dataSource")
      assertTableColumnCollation(tableName, "c1", "UNICODE")
    }
  }

  test("create table as select") {
    val tableName = "tbl"

    // literals in select do not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"""
           |CREATE TABLE $tableName USING $dataSource AS SELECT
           |  'a' AS c1,
           |  'a' || 'a' AS c2,
           |  SUBSTRING('a', 1, 1) AS c3,
           |  SUBSTRING(SUBSTRING('ab', 1, 1), 1, 1) AS c4,
           |  'a' = 'A' AS truthy
           |""".stripMargin)
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")
      assertTableColumnCollation(tableName, "c2", "UTF8_BINARY")
      assertTableColumnCollation(tableName, "c3", "UTF8_BINARY")
      assertTableColumnCollation(tableName, "c4", "UTF8_BINARY")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName WHERE truthy"), Seq(Row(0)))
    }

    // literals in inline table do not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"""
           |CREATE TABLE $tableName USING $dataSource AS
           |SELECT c1, c1 = 'A' as c2 FROM VALUES ('a'), ('A') AS vals(c1)
           |""".stripMargin)
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName WHERE c2"), Seq(Row(1)))
    }

    // cast in select does not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName USING $dataSource AS SELECT cast('a' AS STRING) AS c1")
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")
    }
  }

  test("ctas with complex types") {
    val tableName = "tbl_complex"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"""
           |CREATE TABLE $tableName USING $dataSource AS
           |SELECT
           |  struct('a') AS c1,
           |  map('a', 'b') AS c2,
           |  array('a') AS c3
           |""".stripMargin)

      checkAnswer(sql(s"SELECT COLLATION(c1.col1) FROM $tableName"), Seq(Row("UTF8_BINARY")))
      checkAnswer(
        // TODO: other PR is supposed to fix explicit collation here
        sql(s"SELECT COLLATION(c2['a' collate UTF8_BINARY]) FROM $tableName"),
        Seq(Row("UTF8_BINARY")))
      checkAnswer(sql(s"SELECT COLLATION(c3[0]) FROM $tableName"), Seq(Row("UTF8_BINARY")))
    }
  }

  test("ctas with union") {
    val tableName = "tbl_union"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"""
           |CREATE TABLE $tableName USING $dataSource AS
           |SELECT 'a' = 'A' AS c1
           |UNION
           |SELECT 'b' = 'B' AS c1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(false)))
    }

    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"""
             |CREATE TABLE $tableName USING $dataSource AS
             |SELECT 'a' = 'A' AS c1
             |UNION ALL
             |SELECT 'b' = 'B' AS c1
             |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(false), Row(false)))
    }
  }

  test("add column") {
    val tableName = "tbl_add_col"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      assertTableColumnCollation(tableName, "c1", "UTF8_LCASE")

      sql(s"ALTER TABLE $tableName ADD COLUMN c2 STRING")
      assertTableColumnCollation(tableName, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $tableName ADD COLUMN c3 STRING COLLATE UNICODE")
      assertTableColumnCollation(tableName, "c3", "UNICODE")
    }
  }

  test("create/alter view created from a table") {
    val tableName = "tbl_view"
    val viewName = "view2"
    val sessionCollation = "UTF8_LCASE"
    withSessionCollationAndTable(sessionCollation, tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING, c2 STRING COLLATE UNICODE_CI) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a', 'a'), ('A', 'A')")

      withView(viewName) {
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $tableName")

        assertTableColumnCollation(viewName, "c1", "UTF8_BINARY")
        assertTableColumnCollation(viewName, "c2", "UNICODE_CI")
        checkAnswer(
          sql(s"SELECT DISTINCT COLLATION(c1), COLLATION('a') FROM $viewName"),
          Row("UTF8_BINARY", sessionCollation)
        )

        // filter should use session collation
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $viewName WHERE 'a' = 'A'"),
          Row(2)
        )

        // filter should use column collation
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $viewName WHERE c1 = 'A'"),
          Row(1)
        )

        // literal with explicit collation wins
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $viewName WHERE c1 = 'A' collate UNICODE_CI"),
          Row(2)
        )

        // two implicit collations -> errors out
        assertThrowsImplicitMismatch(sql(s"SELECT c1 = substring('A', 0, 1) FROM $viewName"))

        sql(s"ALTER VIEW $viewName AS SELECT c1 COLLATE UNICODE_CI AS c1, c2 FROM $tableName")
        assertTableColumnCollation(viewName, "c1", "UNICODE_CI")
        assertTableColumnCollation(viewName, "c2", "UNICODE_CI")
        checkAnswer(
          sql(s"SELECT DISTINCT COLLATION(c1), COLLATION('a') FROM $viewName"),
          Row("UNICODE_CI", sessionCollation)
        )

        // after alter both rows should be returned
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $viewName WHERE c1 = 'A'"),
          Row(2)
        )
      }
    }
  }

  test("join view with table") {
    val viewTableName = "view_table"
    val joinTableName = "join_table"
    val viewName = "view"
    val sessionCollation = "sr"

    withSessionCollationAndTable(sessionCollation, viewTableName, joinTableName) {
      sql(s"CREATE TABLE $viewTableName (c1 STRING COLLATE UNICODE_CI) USING $dataSource")
      sql(s"CREATE TABLE $joinTableName (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      sql(s"INSERT INTO $viewTableName VALUES ('a')")
      sql(s"INSERT INTO $joinTableName VALUES ('A')")

      withView(viewName) {
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $viewTableName")

        assertThrowsImplicitMismatch(
          sql(s"SELECT * FROM $viewName JOIN $joinTableName ON $viewName.c1 = $joinTableName.c1")
        )

        checkAnswer(
          sql(s"""
               |SELECT COLLATION($viewName.c1), COLLATION($joinTableName.c1)
               |FROM $viewName JOIN $joinTableName
               |ON $viewName.c1 = $joinTableName.c1 COLLATE UNICODE_CI
               |""".stripMargin),
          Row("UNICODE_CI", "UTF8_LCASE")
        )
      }
    }
  }

  // endregion

  // region DML tests

  test("literals with default collation") {
    withSessionCollation("UTF8_LCASE") {

      // literal without collation
      checkAnswer(sql("SELECT COLLATION('a')"), Seq(Row("UTF8_LCASE")))

      checkAnswer(sql("SELECT COLLATION(map('a', 'b')['a'])"), Seq(Row("UTF8_LCASE")))

      checkAnswer(sql("SELECT COLLATION(array('a')[0])"), Seq(Row("UTF8_LCASE")))

      checkAnswer(sql("SELECT COLLATION(struct('a' as c)['c'])"), Seq(Row("UTF8_LCASE")))
    }
  }

  test("literals with explicit collation") {
    withSessionCollation("UTF8_LCASE") {
      checkAnswer(sql("SELECT COLLATION('a' collate unicode)"), Seq(Row("UNICODE")))

      checkAnswer(
        sql("SELECT COLLATION(map('a', 'b' collate unicode)['a'])"),
        Seq(Row("UNICODE")))

      checkAnswer(sql("SELECT COLLATION(array('a' collate unicode)[0])"), Seq(Row("UNICODE")))

      checkAnswer(
        sql("SELECT COLLATION(struct('a' collate unicode as c)['c'])"),
        Seq(Row("UNICODE")))
    }
  }

  test("cast is aware of session collation") {
    withSessionCollation("UTF8_LCASE") {
      checkAnswer(
        sql("SELECT COLLATION(cast('a' collate unicode as STRING))"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql("SELECT COLLATION(cast(map('a', 'b' collate unicode) as MAP<STRING, STRING>)['a'])"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql("SELECT COLLATION(cast(array('a' collate unicode) as ARRAY<STRING>)[0])"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql("SELECT COLLATION(cast(struct('a' collate unicode as c) as STRUCT<c: STRING>)['c'])"),
        Seq(Row("UTF8_LCASE")))
    }
  }

  test("expressions in where are aware of session collation") {
    withSessionCollation("UTF8_LCASE") {
      // expression in where is aware of session collation
      checkAnswer(sql("SELECT 1 WHERE 'a' = 'A'"), Seq(Row(1)))

      checkAnswer(sql("SELECT 1 WHERE 'a' = cast('A' as STRING)"), Seq(Row(1)))
    }
  }

  test("having group by is aware of session collation") {
    val tableName = "tbl_grp_by"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a'), ('A')")

      // having clause uses session (default) collation
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tableName GROUP BY c1 HAVING 'a' = 'A'"),
        Seq(Row(1), Row(1)))

      // having clause uses column (implicit) collation
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tableName GROUP BY c1 HAVING c1 = 'A'"),
        Seq(Row(1)))
    }
  }

  test("min/max are aware of session collation") {
    // scalastyle:off nonascii
    val tableName = "tbl_min_max"
    withSessionCollationAndTable("UNICODE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('1'), ('½')")

      checkAnswer(sql(s"SELECT MIN(c1) FROM $tableName"), Seq(Row("1")))

      checkAnswer(sql(s"SELECT MAX(c1) FROM $tableName"), Seq(Row("½")))
    }
    // scalastyle:on nonascii
  }

  test("union operation with subqueries") {
    withSessionCollation("UTF8_LCASE") {
      checkAnswer(
        sql(s"""
             |SELECT 'a' = 'A'
             |UNION
             |SELECT 'b' = 'B'
             |""".stripMargin),
        Seq(Row(true)))

      checkAnswer(
        sql(s"""
               |SELECT 'a' = 'A'
               |UNION ALL
               |SELECT 'b' = 'B'
               |""".stripMargin),
        Seq(Row(true), Row(true)))
    }
  }

  test("literals in insert inherit session level collation") {
    val tableName = "tbl_insert"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 BOOLEAN) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a' = 'A')")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName WHERE c1"), Seq(Row(1)))

      sql(s"INSERT INTO $tableName VALUES (array_contains(array('a'), 'A'))")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName WHERE c1"), Seq(Row(2)))
    }
  }

  test("delete behavior") {
    val tableName = "testcat.tbl_delete"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a'), ('A')")

      sql(s"DELETE FROM $tableName WHERE 'a' = 'A'")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $tableName"), Seq(Row(0)))
    }
  }
  // endregion
}
