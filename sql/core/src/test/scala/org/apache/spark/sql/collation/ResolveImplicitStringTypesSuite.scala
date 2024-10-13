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

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Utils

class ResolveImplicitStringTypesSuite extends DatasourceV2SQLBase {

  def withSessionCollationAndTable(collation: String, tableName: String)(f: => Unit): Unit = {
    withTable(tableName) {
      withSessionCollation(collation) {
        f
      }
    }
  }

  def withSessionCollation(collation: String)(f: => Unit): Unit = {
    sql(s"SET COLLATION $collation")
    Utils.tryWithSafeFinally {
      f
    } {
      sql(s"SET COLLATION UTF8_BINARY")
    }
  }

  def assertTableColumnCollation(table: String, column: String, expectedCollation: String): Unit = {
    val colType = spark.table(table).schema(column).dataType
    assert(colType === StringType(expectedCollation))
  }

  // region DDL tests
  test("create/alter table") {
    val tableName = "testcat.tbl"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      // create table with implicit collation
      sql(s"CREATE TABLE $tableName (c1 STRING) USING parquet")
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")

      // alter table add column with implicit collation
      sql(s"ALTER TABLE $tableName ADD COLUMN c2 STRING")
      assertTableColumnCollation(tableName, "c2", "UTF8_BINARY")

      // alter table change column with explicit collation
//      sql(s"ALTER TABLE $tableName ALTER COLUMN c2 TYPE STRING COLLATE UTF8_LCASE")
//      assertTableColumnCollation(tableName, "c2", "UTF8_LCASE")

      // alter table change column with implicit collation
//      sql(s"ALTER TABLE $tableName ALTER COLUMN c2 TYPE STRING")
      assertTableColumnCollation(tableName, "c2", "UTF8_BINARY")
    }
  }

  test("create table as select") {
    val tableName = "testcat.tbl"

    // literals in select do not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName USING parquet AS SELECT 'a' AS c1")
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")
    }

    // cast in select does not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName USING parquet AS SELECT cast('a' AS STRING) AS c1")
      assertTableColumnCollation(tableName, "c1", "UTF8_BINARY")
    }
  }

  // TODO: does not work
//  test("create/alter view") {
//    val viewName = "view_test"
//    withSessionCollation("UTF8_LCASE") {
//      withTempView(viewName) {
//        sql(s"CREATE TEMP VIEW $viewName AS SELECT 'a' AS c1")
//        checkAnswer(
//          sql(s"SELECT COLLATION(c1) FROM $viewName"),
//          Seq(Row("UTF8_BINARY"))
//        )
//      }
//    }
//  }
  // endregion

  // region DML tests
  test("basic") {
    withSessionCollation("UTF8_LCASE") {

      // literal without collation
      checkAnswer(
        sql("SELECT COLLATION('a')"),
        Seq(Row("UTF8_LCASE")))

      // literal with explicit collation
      checkAnswer(
        sql("SELECT COLLATION('a' collate unicode)"),
        Seq(Row("UNICODE")))

      // cast is aware of session collation
      checkAnswer(
        sql("SELECT COLLATION(cast('a' as STRING))"),
        Seq(Row("UTF8_LCASE")))

      // expression in where is aware of session collation
      checkAnswer(
        sql("SELECT 1 WHERE 'a' = 'A'"),
        Seq(Row(1)))

      checkAnswer(
        sql("SELECT 1 WHERE 'a' = cast('A' as STRING)"),
        Seq(Row(1)))
    }
  }

  test("having group by is aware of session collation") {
    val tableName = "testcat.tbl_grp_by"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING) USING parquet")
      sql(s"INSERT INTO $tableName VALUES ('a'), ('A')")

      // having clause uses session collation
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tableName GROUP BY c1 HAVING 'a' = 'A'"),
        Seq(Row(1), Row(1)))

      // having clause uses column collation
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tableName GROUP BY c1 HAVING c1 = 'A'"),
        Seq(Row(1)))
    }
  }

  test("min/max are aware of session collation") {
    // scalastyle:off nonascii
    val tableName = "testcat.tbl_min_max"
    withSessionCollationAndTable("UNICODE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING) USING parquet")
      sql(s"INSERT INTO $tableName VALUES ('1'), ('½')")

      checkAnswer(
        sql(s"SELECT MIN(c1) FROM $tableName"),
        Seq(Row("1")))

      checkAnswer(
        sql(s"SELECT MAX(c1) FROM $tableName"),
        Seq(Row("½")))
    }
    // scalastyle:on nonascii
  }

  test("literals in insert inherit session level collation") {
    val tableName = "testcat.tbl_insert"
    withSessionCollationAndTable("UTF8_LCASE", tableName) {
      sql(s"CREATE TABLE $tableName (c1 BOOLEAN) USING parquet")
      sql(s"INSERT INTO $tableName VALUES ('a' = 'A')")

      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tableName WHERE c1"),
        Seq(Row(1)))

      sql(s"INSERT INTO $tableName VALUES (array_contains(array('a'), 'A'))")
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $tableName WHERE c1"),
        Seq(Row(2)))
    }
  }

//  test("update behavior") {
//    val tableName = "testcat.tbl_update"
//    withTableAndSessionCollation("UTF8_LCASE", tableName) {
//      sql(s"CREATE TABLE $tableName (c1 STRING, c2 INT) USING parquet")
//      sql(s"INSERT INTO $tableName VALUES ('a', 0), ('A', 0)")
//
//      sql(s"UPDATE $tableName SET c1 = 2 WHERE 'a' = 'A'")
//      checkAnswer(
//        sql(s"SELECT SUM(c2) FROM $tableName"),
//        Seq(Row(4)))
//
//      sql(s"UPDATE $tableName SET c1 = 2 WHERE c1 = 'A'")
//      checkAnswer(
//        sql(s"SELECT SUM(c2) FROM $tableName"),
//        Seq(Row(2)))
//    }
//  }

//  test("delete behavior") {
//    val tableName = "testcat.tbl_delete"
//    withTableAndSessionCollation("UTF8_LCASE", tableName) {
//      sql(s"CREATE TABLE $tableName (c1 STRING) USING parquet")
//      sql(s"INSERT INTO $tableName VALUES ('a'), ('A')")
//
//      sql(s"DELETE FROM $tableName WHERE c1 = 'A'")
//      checkAnswer(
//        sql(s"SELECT COUNT(*) FROM $tableName"),
//        Seq(Row(1)))
//
//      sql(s"DELETE FROM $tableName WHERE 'a' = 'A'")
//      checkAnswer(
//        sql(s"SELECT COUNT(*) FROM $tableName"),
//        Seq(Row(0)))
//    }
//  }
  // endregion
}
