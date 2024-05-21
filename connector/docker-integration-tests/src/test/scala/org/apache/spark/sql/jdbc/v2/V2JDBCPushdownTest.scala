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

package org.apache.spark.sql.jdbc.v2

import scala.collection.immutable.Seq

import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalLimit}
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.test.SharedSparkSession

trait V2JDBCPushdownTest extends SharedSparkSession {
  protected def isFilterRemoved(df: DataFrame): Boolean = {
    df.queryExecution.sparkPlan.collectFirst {
      case f: FilterExec => f
    }.isEmpty
  }

  protected def isAggregateRemoved(df: DataFrame): Boolean = {
    df.queryExecution.optimizedPlan.collect {
      case agg: Aggregate => agg
    }.isEmpty
  }

  private def isLimitPushed(df: DataFrame): Boolean = {
    df.queryExecution.optimizedPlan.collect {
      case lim: LocalLimit => lim
    }.isEmpty
  }

  protected val catalog: String

  protected val tablePrefix: String

  protected val schema: String

  protected def executeUpdate(sql: String): Unit

  protected def commonAssertionOnDataFrame(df: DataFrame): Unit

  protected def prepareTable(): Unit = {
    executeUpdate(
      s"""CREATE SCHEMA "$schema""""
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."$tablePrefix"
         | (id INTEGER, st STRING, num_col INT);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_coalesce"
         | (id INTEGER, col1 VARCHAR(128), col2 INT);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_string_test"
         | (id INTEGER, st STRING, num_col INT);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_with_nulls"
         | (id INTEGER, st STRING);""".stripMargin
    )

    executeUpdate(
      s"""CREATE TABLE "$schema"."${tablePrefix}_numeric_test"
         | (id INTEGER, dec_col DECIMAL(10, 2));""".stripMargin
    )
  }

  protected def prepareData(): Unit = {

    prepareTable()

    executeUpdate(s"""INSERT INTO "$schema"."${tablePrefix}_coalesce" VALUES (1, NULL, 1)""")
    executeUpdate(s"""INSERT INTO "$schema"."${tablePrefix}_coalesce" VALUES (2, '2', NULL)""")
    executeUpdate(s"""INSERT INTO "$schema"."${tablePrefix}_coalesce" VALUES (3, NULL, NULL)""")

    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_with_nulls" VALUES (1, 'first')""")
    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_with_nulls" VALUES (2, 'second')""")
    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_with_nulls" VALUES (3, 'third')""")
    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_with_nulls" VALUES (NULL, 'null')""")

    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_string_test" VALUES (0, 'ab''', 1000)""")
    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_string_test" VALUES (0, 'FiRs''T', 1000)""")
    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_string_test" VALUES (0, 'sE Co nD', 1000)""")
    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_string_test" VALUES (0, '   forth   ', 1000)""")

    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (1, 'ab', 1000)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (2, 'aba', NULL)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (3, 'abb', 800)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (4, 'abc', NULL)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (5, 'abd', 1200)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (6, 'abe', 1250)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (7, 'abf', 1200)""")
    executeUpdate(s"""INSERT INTO "$schema"."$tablePrefix" VALUES (8, 'abg', -1300)""")

    executeUpdate(
      s"""INSERT INTO "$schema"."${tablePrefix}_numeric_test" VALUES (1, 42.42)""")
  }

  protected def checkAnswer(df: DataFrame, expectedAnswer: Seq[Row]): Unit =
    QueryTest.checkAnswer(df, expectedAnswer)

  protected def checkAnswer(df: DataFrame, expectedAnswer: Row): Unit =
    QueryTest.checkAnswer(df, Seq(expectedAnswer))

  protected def cleanData(): Unit = {
    executeUpdate(s"""DROP TABLE IF EXISTS "$schema"."$tablePrefix"""")
    executeUpdate(s"""DROP TABLE IF EXISTS "$schema"."${tablePrefix}_string_test"""")
    executeUpdate(s"""DROP TABLE IF EXISTS "$schema"."${tablePrefix}_with_nulls"""")
    executeUpdate(s"""DROP SCHEMA IF EXISTS "$schema"""")
  }

  test("string escaping test") {
    val df = sql(
      s"SELECT 'ab\\'', st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where st = 'ab\\''")
    checkAnswer(df, Row("ab'", "ab'"))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("boolean AND and OR predicate push down") {
    val df = sql(
      s"SELECT id FROM `$catalog`.`$schema`.`$tablePrefix` where (id > 1 AND id < 4) OR id = 7"
    )
    checkAnswer(df, Seq(Row(2), Row(3), Row(7)))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("in predicate push down") {
    // even mix types, we expect spark to insert casts
    val df =
      sql(s"SELECT * FROM `$catalog`.`$schema`.`${tablePrefix}_with_nulls` " +
        s"where id in (1, '2', NULL)")

    // we do not expect NULL equality to work in IN
    checkAnswer(df, Seq(Row(1, "first"), Row(2, "second")))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)

    val df2 =
      sql(s"SELECT * FROM `$catalog`.`$schema`.`${tablePrefix}_with_nulls` " +
        s"where NOT id in (1, '2')")

    // we do not expect NULL equality to work in IN
    checkAnswer(df2, Seq(Row(3, "third")))
    assert(isFilterRemoved(df2))
    commonAssertionOnDataFrame(df2)
  }

  test("case when in predicate push down") {
    val df = sql(
      s"""SELECT id FROM `$catalog`.`$schema`.`$tablePrefix` WHERE
         |CASE WHEN id = 3 THEN true ELSE false END""".stripMargin)
    checkAnswer(df, Seq(Row(3)))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)

    val df2 = sql(
      s"""SELECT id FROM `$catalog`.`$schema`.`$tablePrefix` WHERE
         |CASE WHEN id = 3 THEN 2 ELSE 3 END = 2""".stripMargin)
    checkAnswer(df2, Seq(Row(3)))
    assert(isFilterRemoved(df2))
    commonAssertionOnDataFrame(df2)
  }

  test("coalesce predicate push down") {
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      val cases = Seq(
        "COALESCE(col1, col2) = 1" -> Seq(Row(1)),
        "COALESCE(col1, col2) = 2" -> Seq(Row(2)),
        "COALESCE(col1, col2) IS NULL" -> Seq(Row(3)),
        "COALESCE(col1, col2) IS NOT NULL" -> Seq(Row(1), Row(2))
      )

      cases.foreach({ case (predicate, expected) =>
        val df =
          sql(s"SELECT id FROM `$catalog`.`$schema`.`${tablePrefix}_coalesce` WHERE $predicate")
        checkAnswer(df, expected)
        assert(isFilterRemoved(df))
        commonAssertionOnDataFrame(df)
      })
    }
  }

  test("unary minus predicate push down") {
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      val df = sql(
        s"""SELECT id FROM `$catalog`.`$schema`.`$tablePrefix` WHERE
           |WHERE -id=3""".stripMargin)
      checkAnswer(df, Seq.empty)
      assert(isFilterRemoved(df))
      commonAssertionOnDataFrame(df)
    }
  }

  test("not predicate push down") {
    val df = sql(s"SELECT id FROM `$catalog`.`$schema`.`$tablePrefix` where NOT id = 1")
    checkAnswer(df, (2 to 8).map(Row(_)))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("null predicate push down") {
    val df =
      sql(s"SELECT * FROM `$catalog`.`$schema`.`${tablePrefix}_with_nulls` where id is NULL")
    checkAnswer(df, Row(null, "null"))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)

    val df2 = sql(
      s"SELECT id FROM `$catalog`.`$schema`.`${tablePrefix}_with_nulls` where id is not NULL")
    checkAnswer(df2, Seq(Row(1), Row(2), Row(3)))
    assert(isFilterRemoved(df2))
    commonAssertionOnDataFrame(df2)
  }

  test("LOWER and UPPER predicate push down") {
    val df = sql(
      s"SELECT st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where lower(st) = 'firs\\'t'")
    checkAnswer(df, Row("FiRs'T"))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)

    val df2 = sql(
      s"SELECT st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where upper(st) = 'FIRS\\'T'")
    checkAnswer(df2, Row("FiRs'T"))
    assert(isFilterRemoved(df2))
    commonAssertionOnDataFrame(df2)

    val df3 = sql(
      s"SELECT st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where lower(st) = 'se co nd'")
    checkAnswer(df3, Row("sE Co nD"))
    assert(isFilterRemoved(df3))
    commonAssertionOnDataFrame(df3)

    val df4 = sql(
      s"SELECT st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where upper(st) = 'SE CO ND'")
    checkAnswer(df4, Row("sE Co nD"))
    assert(isFilterRemoved(df4))
    commonAssertionOnDataFrame(df4)
  }

  test("LENGTH predicate push down") {
    val df = sql(
      s"SELECT st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where length(st) = 11")
    checkAnswer(df, Row("   forth   "))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)

    val df2 = sql(
      s"SELECT st " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_string_test` where length(id) = 1")
    assert(df2.collect().length == 4)
    assert(isFilterRemoved(df2))
    commonAssertionOnDataFrame(df2)
  }

  test("ABS predicate push down") {
    val df = sql(
      s"SELECT id " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix` where abs(num_col) = 1300")
    checkAnswer(df, Row(8))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("FLOOR predicate push down") {
    val df = sql(
      s"SELECT id " +
        s"FROM `$catalog`.`$schema`.`${tablePrefix}_numeric_test` where floor(dec_col) = 42")
    checkAnswer(df, Row(1))
    assert(isFilterRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("MAX AND MIN aggregate push down") {
    val df = sql(
      s"SELECT MAX(num_col) " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix`")
    checkAnswer(df, Row(1250))
    assert(isAggregateRemoved(df))
    commonAssertionOnDataFrame(df)

    val df2 = sql(
      s"SELECT MIN(num_col) " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix`")
    checkAnswer(df2, Row(-1300))
    assert(isAggregateRemoved(df2))
    commonAssertionOnDataFrame(df2)
  }

  test("AVG aggregate push down") {
    val df = sql(
      s"SELECT AVG(id) " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix`")
    checkAnswer(df, Row(4.5))
    assert(isAggregateRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("COUNT aggregate push down") {
    val df = sql(
      s"SELECT COUNT(num_col) " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix`")
    checkAnswer(df, Row(6))
    assert(isAggregateRemoved(df))
    commonAssertionOnDataFrame(df)
  }

  test("LIMIT with SORT push down") {
    val df = sql(
      s"SELECT id " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix` " +
        s"ORDER BY num_col NULLS FIRST, id " +
        s"LIMIT 2 "
    )
    checkAnswer(df, Seq(Row(2), Row(4)))
    assert(isLimitPushed(df))
    commonAssertionOnDataFrame(df)

    val df2 = sql(
      s"SELECT id " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix` " +
        s"ORDER BY num_col NULLS LAST, id " +
        s"LIMIT 2 "
    )
    checkAnswer(df2, Seq(Row(8), Row(3)))
    assert(isLimitPushed(df2))
    commonAssertionOnDataFrame(df2)

    val df3 = sql(
      s"SELECT id " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix` " +
        s"ORDER BY num_col DESC NULLS FIRST, id " +
        s"LIMIT 2 "
    )
    checkAnswer(df3, Seq(Row(2), Row(4)))
    assert(isLimitPushed(df3))
    commonAssertionOnDataFrame(df3)

    val df4 = sql(
      s"SELECT id " +
        s"FROM `$catalog`.`$schema`.`$tablePrefix` " +
        s"ORDER BY num_col DESC NULLS LAST, id " +
        s"LIMIT 2 "
    )
    checkAnswer(df4, Seq(Row(6), Row(5)))
    assert(isLimitPushed(df4))
    commonAssertionOnDataFrame(df4)

  }
}
