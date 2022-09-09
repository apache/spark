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

package org.apache.spark.sql

import java.io.File
import java.net.{MalformedURLException, URL}
import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}
import java.util.Locale
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.mutable

import org.apache.commons.io.FileUtils

import org.apache.spark.{AccumulatorSuite, SparkException}
import org.apache.spark.scheduler.{SparkListener, SparkListenerJobStart}
import org.apache.spark.sql.catalyst.expressions.{GenericRow, Hex}
import org.apache.spark.sql.catalyst.expressions.aggregate.{Complete, Partial}
import org.apache.spark.sql.catalyst.optimizer.{ConvertToLocalRelation, NestedColumnAliasingSuite}
import org.apache.spark.sql.catalyst.plans.logical.{LocalLimit, Project, RepartitionByExpression, Sort}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.execution.{CommandResultExec, UnionExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.aggregate._
import org.apache.spark.sql.execution.columnar.InMemoryTableScanExec
import org.apache.spark.sql.execution.command.DataWritingCommandExec
import org.apache.spark.sql.execution.datasources.{InsertIntoHadoopFsRelationCommand, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.execution.datasources.v2.orc.OrcScan
import org.apache.spark.sql.execution.datasources.v2.parquet.ParquetScan
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, CartesianProductExec, SortMergeJoinExec}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._
import org.apache.spark.tags.ExtendedSQLTest
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}
import org.apache.spark.util.ResetSystemProperties

@ExtendedSQLTest
class SQLQuerySuite extends QueryTest with SharedSparkSession with AdaptiveSparkPlanHelper
    with ResetSystemProperties {
  import testImplicits._

  setupTestData()

  test("SPARK-8010: promote numeric to string") {
    withTempView("src") {
      val df = Seq((1, 1)).toDF("key", "value")
      df.createOrReplaceTempView("src")
      val queryCaseWhen = sql("select case when true then 1.0 else '1' end from src ")
      val queryCoalesce = sql("select coalesce(null, 1, '1') from src ")

      if (!conf.ansiEnabled) {
        checkAnswer(queryCaseWhen, Row("1.0") :: Nil)
        checkAnswer(queryCoalesce, Row("1") :: Nil)
      }
    }
  }

  test("describe functions") {
    checkKeywordsExist(sql("describe function extended upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns `str` with all characters changed to uppercase",
      "Extended Usage:",
      "Examples:",
      "> SELECT upper('SparkSql');",
      "SPARKSQL")

    checkKeywordsExist(sql("describe functioN Upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns `str` with all characters changed to uppercase")

    checkKeywordsNotExist(sql("describe functioN Upper"), "Extended Usage")

    val e = intercept[AnalysisException](sql("describe functioN abcadf"))
    assert(e.message.contains("Undefined function: abcadf. This function is neither a " +
      "built-in/temporary function, nor a persistent function"))
  }

  test("SPARK-34678: describe functions for table-valued functions") {
    checkKeywordsExist(sql("describe function range"),
      "Function: range",
      "Class: org.apache.spark.sql.catalyst.plans.logical.Range",
      "range(end: long)"
    )
  }

  test("SPARK-14415: All functions should have own descriptions") {
    for (f <- spark.sessionState.functionRegistry.listFunction()) {
      if (!Seq("cube", "grouping", "grouping_id", "rollup").contains(f.unquotedString)) {
        checkKeywordsNotExist(sql(s"describe function $f"), "N/A.")
      }
    }
  }

  test("SPARK-6743: no columns from cache") {
    withTempView("cachedData") {
      Seq(
        (83, 0, 38),
        (26, 0, 79),
        (43, 81, 24)
      ).toDF("a", "b", "c").createOrReplaceTempView("cachedData")

      spark.catalog.cacheTable("cachedData")
      withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
        checkAnswer(
          sql("SELECT t1.b FROM cachedData, cachedData t1 GROUP BY t1.b"),
          Row(0) :: Row(81) :: Nil)
      }
    }
  }

  test("self join with aliases") {
    withTempView("df") {
      Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str").createOrReplaceTempView("df")

      checkAnswer(
        sql(
          """
            |SELECT x.str, COUNT(*)
            |FROM df x JOIN df y ON x.str = y.str
            |GROUP BY x.str
        """.stripMargin),
        Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
    }
  }

  test("support table.star") {
    checkAnswer(
      sql(
        """
          |SELECT r.*
          |FROM testData l join testData2 r on (l.key = r.a)
        """.stripMargin),
      Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)
  }

  test("self join with alias in agg") {
    withTempView("df") {
      Seq(1, 2, 3)
        .map(i => (i, i.toString))
        .toDF("int", "str")
        .groupBy("str")
        .agg($"str", count("str").as("strCount"))
        .createOrReplaceTempView("df")

      checkAnswer(
        sql(
          """
            |SELECT x.str, SUM(x.strCount)
            |FROM df x JOIN df y ON x.str = y.str
            |GROUP BY x.str
        """.stripMargin),
        Row("1", 1) :: Row("2", 1) :: Row("3", 1) :: Nil)
    }
  }

  test("SPARK-8668 expr function") {
    checkAnswer(Seq((1, "Bobby G."))
      .toDF("id", "name")
      .select(expr("length(name)"), expr("abs(id)")), Row(8, 1))

    checkAnswer(Seq((1, "building burrito tunnels"), (1, "major projects"))
      .toDF("id", "saying")
      .groupBy(expr("length(saying)"))
      .count(), Row(24, 1) :: Row(14, 1) :: Nil)
  }

  test("SPARK-4625 support SORT BY in SimpleSQLParser & DSL") {
    checkAnswer(
      sql("SELECT a FROM testData2 SORT BY a"),
      Seq(1, 1, 2, 2, 3, 3).map(Row(_))
    )
  }

  test("SPARK-7158 collect and take return different results") {
    import java.util.UUID

    val df = Seq(Tuple1(1), Tuple1(2), Tuple1(3)).toDF("index")
    // we except the id is materialized once
    val idUDF = org.apache.spark.sql.functions.udf(() => UUID.randomUUID().toString)

    val dfWithId = df.withColumn("id", idUDF())
    // Make a new DataFrame (actually the same reference to the old one)
    val cached = dfWithId.cache()
    // Trigger the cache
    val d0 = dfWithId.collect()
    val d1 = cached.collect()
    val d2 = cached.collect()

    // Since the ID is only materialized once, then all of the records
    // should come from the cache, not by re-computing. Otherwise, the ID
    // will be different
    assert(d0.map(_(0)) === d2.map(_(0)))
    assert(d0.map(_(1)) === d2.map(_(1)))

    assert(d1.map(_(0)) === d2.map(_(0)))
    assert(d1.map(_(1)) === d2.map(_(1)))
  }

  test("grouping on nested fields") {
    withTempView("rows") {
      spark.read
        .json(Seq("""{"nested": {"attribute": 1}, "value": 2}""").toDS())
       .createOrReplaceTempView("rows")

      checkAnswer(
        sql(
          """
            |select attribute, sum(cnt)
            |from (
            |  select nested.attribute, count(*) as cnt
            |  from rows
            |  group by nested.attribute) a
            |group by attribute
        """.stripMargin),
        Row(1, 1) :: Nil)
    }
  }

  test("SPARK-6201 IN type conversion") {
    withTempView("d") {
      spark.read
        .json(Seq("{\"a\": \"1\"}}", "{\"a\": \"2\"}}", "{\"a\": \"3\"}}").toDS())
        .createOrReplaceTempView("d")

      checkAnswer(
        sql("select * from d where d.a in (1,2)"),
        Seq(Row("1"), Row("2")))
    }
  }

  test("SPARK-11226 Skip empty line in json file") {
    withTempView("d") {
      spark.read
        .json(Seq("{\"a\": \"1\"}}", "{\"a\": \"2\"}}", "{\"a\": \"3\"}}", "").toDS())
        .createOrReplaceTempView("d")

      checkAnswer(
        sql("select count(1) from d"),
        Seq(Row(3)))
    }
  }

  test("SPARK-8828 sum should return null if all input values are null") {
    checkAnswer(
      sql("select sum(a), avg(a) from allNulls"),
      Seq(Row(null, null))
    )
  }

  private def testCodeGen(sqlText: String, expectedResults: Seq[Row]): Unit = {
    val df = sql(sqlText)
    // First, check if we have GeneratedAggregate.
    val hasGeneratedAgg = df.queryExecution.sparkPlan
      .collect { case _: HashAggregateExec => true }
      .nonEmpty
    if (!hasGeneratedAgg) {
      fail(
        s"""
           |Codegen is enabled, but query $sqlText does not have HashAggregate in the plan.
           |${df.queryExecution.simpleString}
         """.stripMargin)
    }
    // Then, check results.
    checkAnswer(df, expectedResults)
  }

  test("aggregation with codegen") {
    // Prepare a table that we can group some rows.
    spark.table("testData")
      .union(spark.table("testData"))
      .union(spark.table("testData"))
      .createOrReplaceTempView("testData3x")

    try {
      // Just to group rows.
      testCodeGen(
        "SELECT key FROM testData3x GROUP BY key",
        (1 to 100).map(Row(_)))
      // COUNT
      testCodeGen(
        "SELECT key, count(value) FROM testData3x GROUP BY key",
        (1 to 100).map(i => Row(i, 3)))
      testCodeGen(
        "SELECT count(key) FROM testData3x",
        Row(300) :: Nil)
      // COUNT DISTINCT ON int
      testCodeGen(
        "SELECT value, count(distinct key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, 1)))
      testCodeGen(
        "SELECT count(distinct key) FROM testData3x",
        Row(100) :: Nil)
      // SUM
      testCodeGen(
        "SELECT value, sum(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, 3 * i)))
      testCodeGen(
        "SELECT sum(key), SUM(CAST(key as Double)) FROM testData3x",
        Row(5050 * 3, 5050 * 3.0) :: Nil)
      // AVERAGE
      testCodeGen(
        "SELECT value, avg(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT avg(key) FROM testData3x",
        Row(50.5) :: Nil)
      // MAX
      testCodeGen(
        "SELECT value, max(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT max(key) FROM testData3x",
        Row(100) :: Nil)
      // MIN
      testCodeGen(
        "SELECT value, min(key) FROM testData3x GROUP BY value",
        (1 to 100).map(i => Row(i.toString, i)))
      testCodeGen(
        "SELECT min(key) FROM testData3x",
        Row(1) :: Nil)
      // Some combinations.
      testCodeGen(
        """
          |SELECT
          |  value,
          |  sum(key),
          |  max(key),
          |  min(key),
          |  avg(key),
          |  count(key),
          |  count(distinct key)
          |FROM testData3x
          |GROUP BY value
        """.stripMargin,
        (1 to 100).map(i => Row(i.toString, i*3, i, i, i, 3, 1)))
      testCodeGen(
        "SELECT max(key), min(key), avg(key), count(key), count(distinct key) FROM testData3x",
        Row(100, 1, 50.5, 300, 100) :: Nil)
      // Aggregate with Code generation handling all null values.
      // If ANSI mode is on, there will be an error since 'a' cannot converted as Numeric.
      // Here we simply test it when ANSI mode is off.
      if (!conf.ansiEnabled) {
        testCodeGen(
          "SELECT  sum('a'), avg('a'), count(null) FROM testData",
          Row(null, null, 0) :: Nil)
      }
    } finally {
      spark.catalog.dropTempView("testData3x")
    }
  }

  test("Add Parser of SQL COALESCE()") {
    checkAnswer(
      sql("""SELECT COALESCE(1, 2)"""),
      Row(1))
    checkAnswer(
      sql("SELECT COALESCE(null, 1, 1.5)"),
      Row(BigDecimal(1)))
    checkAnswer(
      sql("SELECT COALESCE(null, null, null)"),
      Row(null))
  }

  test("SPARK-3176 Added Parser of SQL LAST()") {
    checkAnswer(
      sql("SELECT LAST(n) FROM lowerCaseData"),
      Row(4))
  }

  test("SPARK-2041 column name equals tablename") {
    checkAnswer(
      sql("SELECT tableName FROM tableName"),
      Row("test"))
  }

  test("SQRT") {
    checkAnswer(
      sql("SELECT SQRT(key) FROM testData"),
      (1 to 100).map(x => Row(math.sqrt(x.toDouble))).toSeq
    )
  }

  test("SQRT with automatic string casts") {
    checkAnswer(
      sql("SELECT SQRT(CAST(key AS STRING)) FROM testData"),
      (1 to 100).map(x => Row(math.sqrt(x.toDouble))).toSeq
    )
  }

  test("SPARK-2407 Added Parser of SQL SUBSTR()") {
    checkAnswer(
      sql("SELECT substr(tableName, 1, 2) FROM tableName"),
      Row("te"))
    checkAnswer(
      sql("SELECT substr(tableName, 3) FROM tableName"),
      Row("st"))
    checkAnswer(
      sql("SELECT substring(tableName, 1, 2) FROM tableName"),
      Row("te"))
    checkAnswer(
      sql("SELECT substring(tableName, 3) FROM tableName"),
      Row("st"))
  }

  test("SPARK-3173 Timestamp support in the parser") {
    withTempView("timestamps") {
      (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time").createOrReplaceTempView("timestamps")

      checkAnswer(sql(
        "SELECT time FROM timestamps WHERE time='1969-12-31 16:00:00.0'"),
        Row(Timestamp.valueOf("1969-12-31 16:00:00")))

      checkAnswer(sql(
        "SELECT time FROM timestamps WHERE time=CAST('1969-12-31 16:00:00.001' AS TIMESTAMP)"),
        Row(Timestamp.valueOf("1969-12-31 16:00:00.001")))

      checkAnswer(sql(
        "SELECT time FROM timestamps WHERE time='1969-12-31 16:00:00.001'"),
        Row(Timestamp.valueOf("1969-12-31 16:00:00.001")))

      checkAnswer(sql(
        "SELECT time FROM timestamps WHERE '1969-12-31 16:00:00.001'=time"),
        Row(Timestamp.valueOf("1969-12-31 16:00:00.001")))

      checkAnswer(sql(
        """SELECT time FROM timestamps WHERE time<'1969-12-31 16:00:00.003'
          AND time>'1969-12-31 16:00:00.001'"""),
        Row(Timestamp.valueOf("1969-12-31 16:00:00.002")))

      checkAnswer(sql(
        """
          |SELECT time FROM timestamps
          |WHERE time IN ('1969-12-31 16:00:00.001','1969-12-31 16:00:00.002')
      """.stripMargin),
        Seq(Row(Timestamp.valueOf("1969-12-31 16:00:00.001")),
          Row(Timestamp.valueOf("1969-12-31 16:00:00.002"))))

      if (!conf.ansiEnabled) {
        checkAnswer(sql(
          "SELECT time FROM timestamps WHERE time='123'"),
          Nil)
      }
    }
  }

  test("left semi greater than predicate") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      checkAnswer(
        sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.a >= y.a + 2"),
        Seq(Row(3, 1), Row(3, 2))
      )
    }
  }

  test("left semi greater than predicate and equal operator") {
    checkAnswer(
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.b = y.b and x.a >= y.a + 2"),
      Seq(Row(3, 1), Row(3, 2))
    )

    checkAnswer(
      sql("SELECT * FROM testData2 x LEFT SEMI JOIN testData2 y ON x.b = y.a and x.a >= y.b + 1"),
      Seq(Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2))
    )
  }

  test("select *") {
    checkAnswer(
      sql("SELECT * FROM testData"),
      testData.collect().toSeq)
  }

  test("simple select") {
    checkAnswer(
      sql("SELECT value FROM testData WHERE key = 1"),
      Row("1"))
  }

  def sortTest(): Unit = {
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b ASC"),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a ASC, b DESC"),
      Seq(Row(1, 2), Row(1, 1), Row(2, 2), Row(2, 1), Row(3, 2), Row(3, 1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b DESC"),
      Seq(Row(3, 2), Row(3, 1), Row(2, 2), Row(2, 1), Row(1, 2), Row(1, 1)))

    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY a DESC, b ASC"),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2)))

    checkAnswer(
      sql("SELECT b FROM binaryData ORDER BY a ASC"),
      (1 to 5).map(Row(_)))

    checkAnswer(
      sql("SELECT b FROM binaryData ORDER BY a DESC"),
      (1 to 5).map(Row(_)).toSeq.reverse)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] ASC"),
      arrayData.collect().sortBy(_.data(0)).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData ORDER BY data[0] DESC"),
      arrayData.collect().sortBy(_.data(0)).reverse.map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] ASC"),
      mapData.collect().sortBy(_.data(1)).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData ORDER BY data[1] DESC"),
      mapData.collect().sortBy(_.data(1)).reverse.map(Row.fromTuple).toSeq)
  }

  test("external sorting") {
    sortTest()
  }

  test("CTE feature") {
    checkAnswer(
      sql("with q1 as (select * from testData limit 10) select * from q1"),
      testData.take(10).toSeq)

    checkAnswer(
      sql("""
        |with q1 as (select * from testData where key= '5'),
        |q2 as (select * from testData where key = '4')
        |select * from q1 union all select * from q2""".stripMargin),
      Row(5, "5") :: Row(4, "4") :: Nil)

    // inner CTE relation refers to outer CTE relation.
    withSQLConf(SQLConf.LEGACY_CTE_PRECEDENCE_POLICY.key -> "CORRECTED") {
      checkAnswer(
        sql(
          """
            |with temp1 as (select 1 col),
            |temp2 as (
            |  with temp1 as (select col + 1 AS col from temp1),
            |  temp3 as (select col + 1 from temp1)
            |  select * from temp3
            |)
            |select * from temp2
            |""".stripMargin),
        Row(3))
      }
  }

  test("Allow only a single WITH clause per query") {
    intercept[AnalysisException] {
      sql(
        "with q1 as (select * from testData) with q2 as (select * from q1) select * from q2")
    }
  }

  test("date row") {
    checkAnswer(sql(
      """select cast("2015-01-28" as date) from testData limit 1"""),
      Row(Date.valueOf("2015-01-28"))
    )
  }

  test("from follow multiple brackets") {
    checkAnswer(sql(
      """
        |select key from ((select * from testData)
        |  union all (select * from testData)) x limit 1
      """.stripMargin),
      Row(1)
    )

    checkAnswer(sql(
      "select key from (select * from testData) x limit 1"),
      Row(1)
    )

    checkAnswer(sql(
      """
        |select key from
        |  (select * from testData union all select * from testData) x
        |  limit 1
      """.stripMargin),
      Row(1)
    )
  }

  test("average") {
    checkAnswer(
      sql("SELECT AVG(a) FROM testData2"),
      Row(2.0))
  }

  test("average overflow") {
    checkAnswer(
      sql("SELECT AVG(a),b FROM largeAndSmallInts group by b"),
      Seq(Row(2147483645.0, 1), Row(2.0, 2)))
  }

  test("count") {
    checkAnswer(
      sql("SELECT COUNT(*) FROM testData2"),
      Row(testData2.count()))
  }

  test("count distinct") {
    checkAnswer(
      sql("SELECT COUNT(DISTINCT b) FROM testData2"),
      Row(2))
  }

  test("approximate count distinct") {
    checkAnswer(
      sql("SELECT APPROX_COUNT_DISTINCT(a) FROM testData2"),
      Row(3))
  }

  test("approximate count distinct with user provided standard deviation") {
    checkAnswer(
      sql("SELECT APPROX_COUNT_DISTINCT(a, 0.04) FROM testData2"),
      Row(3))
  }

  test("null count") {
    checkAnswer(
      sql("SELECT a, COUNT(b) FROM testData3 GROUP BY a"),
      Seq(Row(1, 0), Row(2, 1)))

    checkAnswer(
      sql(
        "SELECT COUNT(a), COUNT(b), COUNT(1), COUNT(DISTINCT a), COUNT(DISTINCT b) FROM testData3"),
      Row(2, 1, 2, 2, 1))
  }

  test("count of empty table") {
    withTempView("t") {
      Seq.empty[(Int, Int)].toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(
        sql("select count(a) from t"),
        Row(0))
    }
  }

  test("inner join where, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        sql("SELECT * FROM uppercasedata JOIN lowercasedata WHERE n = N"),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")))
    }
  }

  test("inner join ON, one match per row") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        sql("SELECT * FROM uppercasedata JOIN lowercasedata ON n = N"),
        Seq(
          Row(1, "A", 1, "a"),
          Row(2, "B", 2, "b"),
          Row(3, "C", 3, "c"),
          Row(4, "D", 4, "d")))
    }
  }

  test("inner join, where, multiple matches") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        sql(
          """
          |SELECT * FROM
          |  (SELECT * FROM testdata2 WHERE a = 1) x JOIN
          |  (SELECT * FROM testdata2 WHERE a = 1) y
          |WHERE x.a = y.a""".stripMargin),
        Row(1, 1, 1, 1) ::
        Row(1, 1, 1, 2) ::
        Row(1, 2, 1, 1) ::
        Row(1, 2, 1, 2) :: Nil)
    }
  }

  test("inner join, no matches") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData2 WHERE a = 1) x JOIN
          |  (SELECT * FROM testData2 WHERE a = 2) y
          |WHERE x.a = y.a""".stripMargin),
      Nil)
  }

  test("big inner join, 4 matches per row") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) x JOIN
          |  (SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData UNION ALL
          |   SELECT * FROM testData) y
          |WHERE x.key = y.key""".stripMargin),
      testData.rdd.flatMap { row =>
        Seq.fill(16)(new GenericRow(Seq(row, row).flatMap(_.toSeq).toArray))
      }.collect().toSeq)
  }

  test("cartesian product join") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      checkAnswer(
        testData3.join(testData3),
        Row(1, null, 1, null) ::
          Row(1, null, 2, 2) ::
          Row(2, 2, 1, null) ::
          Row(2, 2, 2, 2) :: Nil)
    }
  }

  test("left outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        sql("SELECT * FROM uppercasedata LEFT OUTER JOIN lowercasedata ON n = N"),
        Row(1, "A", 1, "a") ::
          Row(2, "B", 2, "b") ::
          Row(3, "C", 3, "c") ::
          Row(4, "D", 4, "d") ::
          Row(5, "E", null, null) ::
          Row(6, "F", null, null) :: Nil)
    }
  }

  test("right outer join") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      checkAnswer(
        sql("SELECT * FROM lowercasedata RIGHT OUTER JOIN uppercasedata ON n = N"),
        Row(1, "a", 1, "A") ::
          Row(2, "b", 2, "B") ::
          Row(3, "c", 3, "C") ::
          Row(4, "d", 4, "D") ::
          Row(null, null, 5, "E") ::
          Row(null, null, 6, "F") :: Nil)
    }
  }

  test("full outer join") {
    checkAnswer(
      sql(
        """
          |SELECT * FROM
          |  (SELECT * FROM upperCaseData WHERE N <= 4) leftTable FULL OUTER JOIN
          |  (SELECT * FROM upperCaseData WHERE N >= 3) rightTable
          |    ON leftTable.N = rightTable.N
        """.stripMargin),
      Row(1, "A", null, null) ::
      Row(2, "B", null, null) ::
      Row(3, "C", 3, "C") ::
      Row (4, "D", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("SPARK-11111 null-safe join should not use cartesian product") {
    val df = sql("select count(*) from testData a join testData b on (a.key <=> b.key)")
    val cp = df.queryExecution.sparkPlan.collect {
      case cp: CartesianProductExec => cp
    }
    assert(cp.isEmpty, "should not use CartesianProduct for null-safe join")
    val smj = df.queryExecution.sparkPlan.collect {
      case smj: SortMergeJoinExec => smj
      case j: BroadcastHashJoinExec => j
    }
    assert(smj.size > 0, "should use SortMergeJoin or BroadcastHashJoin")
    checkAnswer(df, Row(100) :: Nil)
  }

  test("SPARK-3349 partitioning after limit") {
    withTempView("subset1", "subset2") {
      sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n DESC")
        .limit(2)
        .createOrReplaceTempView("subset1")
      sql("SELECT DISTINCT n FROM lowerCaseData ORDER BY n ASC")
        .limit(2)
        .createOrReplaceTempView("subset2")
      checkAnswer(
        sql("SELECT * FROM lowerCaseData INNER JOIN subset1 ON subset1.n = lowerCaseData.n"),
        Row(3, "c", 3) ::
        Row(4, "d", 4) :: Nil)
      checkAnswer(
        sql("SELECT * FROM lowerCaseData INNER JOIN subset2 ON subset2.n = lowerCaseData.n"),
        Row(1, "a", 1) ::
        Row(2, "b", 2) :: Nil)
    }
  }

  test("mixed-case keywords") {
    checkAnswer(
      sql(
        """
          |SeleCT * from
          |  (select * from upperCaseData WherE N <= 4) leftTable fuLL OUtER joiN
          |  (sElEcT * FROM upperCaseData whERe N >= 3) rightTable
          |    oN leftTable.N = rightTable.N
        """.stripMargin),
      Row(1, "A", null, null) ::
      Row(2, "B", null, null) ::
      Row(3, "C", 3, "C") ::
      Row(4, "D", 4, "D") ::
      Row(null, null, 5, "E") ::
      Row(null, null, 6, "F") :: Nil)
  }

  test("select with table name as qualifier") {
    checkAnswer(
      sql("SELECT testData.value FROM testData WHERE testData.key = 1"),
      Row("1"))
  }

  test("inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT * FROM upperCaseData JOIN lowerCaseData ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        Row(1, "A", 1, "a"),
        Row(2, "B", 2, "b"),
        Row(3, "C", 3, "c"),
        Row(4, "D", 4, "d")))
  }

  test("qualified select with inner join ON with table name as qualifier") {
    checkAnswer(
      sql("SELECT upperCaseData.N, upperCaseData.L FROM upperCaseData JOIN lowerCaseData " +
        "ON lowerCaseData.n = upperCaseData.N"),
      Seq(
        Row(1, "A"),
        Row(2, "B"),
        Row(3, "C"),
        Row(4, "D")))
  }

  test("system function upper()") {
    checkAnswer(
      sql("SELECT n,UPPER(l) FROM lowerCaseData"),
      Seq(
        Row(1, "A"),
        Row(2, "B"),
        Row(3, "C"),
        Row(4, "D")))

    checkAnswer(
      sql("SELECT n, UPPER(s) FROM nullStrings"),
      Seq(
        Row(1, "ABC"),
        Row(2, "ABC"),
        Row(3, null)))
  }

  test("system function lower()") {
    checkAnswer(
      sql("SELECT N,LOWER(L) FROM upperCaseData"),
      Seq(
        Row(1, "a"),
        Row(2, "b"),
        Row(3, "c"),
        Row(4, "d"),
        Row(5, "e"),
        Row(6, "f")))

    checkAnswer(
      sql("SELECT n, LOWER(s) FROM nullStrings"),
      Seq(
        Row(1, "abc"),
        Row(2, "abc"),
        Row(3, null)))
  }

  test("UNION") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION SELECT * FROM upperCaseData"),
      Row(1, "A") :: Row(1, "a") :: Row(2, "B") :: Row(2, "b") :: Row(3, "C") :: Row(3, "c") ::
      Row(4, "D") :: Row(4, "d") :: Row(5, "E") :: Row(6, "F") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION SELECT * FROM lowerCaseData"),
      Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData UNION ALL SELECT * FROM lowerCaseData"),
      Row(1, "a") :: Row(1, "a") :: Row(2, "b") :: Row(2, "b") :: Row(3, "c") :: Row(3, "c") ::
      Row(4, "d") :: Row(4, "d") :: Nil)
  }

  test("UNION with column mismatches") {
    // Column name mismatches are allowed.
    checkAnswer(
      sql("SELECT n,l FROM lowerCaseData UNION SELECT N as x1, L as x2 FROM upperCaseData"),
      Row(1, "A") :: Row(1, "a") :: Row(2, "B") :: Row(2, "b") :: Row(3, "C") :: Row(3, "c") ::
      Row(4, "D") :: Row(4, "d") :: Row(5, "E") :: Row(6, "F") :: Nil)
    // Column type mismatches are not allowed, forcing a type coercion.
    // When ANSI mode is on, the String input will be cast as Int in the following Union, which will
    // cause a runtime error. Here we simply test the case when ANSI mode is off.
    if (!conf.ansiEnabled) {
      checkAnswer(
        sql("SELECT n FROM lowerCaseData UNION SELECT L FROM upperCaseData"),
        ("1" :: "2" :: "3" :: "4" :: "A" :: "B" :: "C" :: "D" :: "E" :: "F" :: Nil).map(Row(_)))
    }
    // Column type mismatches where a coercion is not possible, in this case between integer
    // and array types, trigger a TreeNodeException.
    intercept[AnalysisException] {
      sql("SELECT data FROM arrayData UNION SELECT 1 FROM arrayData").collect()
    }
  }

  test("EXCEPT") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData EXCEPT SELECT * FROM upperCaseData"),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData EXCEPT SELECT * FROM lowerCaseData"), Nil)
    checkAnswer(
      sql("SELECT * FROM upperCaseData EXCEPT SELECT * FROM upperCaseData"), Nil)
  }

  test("MINUS") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData MINUS SELECT * FROM upperCaseData"),
      Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData MINUS SELECT * FROM lowerCaseData"), Nil)
    checkAnswer(
      sql("SELECT * FROM upperCaseData MINUS SELECT * FROM upperCaseData"), Nil)
  }

  test("INTERSECT") {
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INTERSECT SELECT * FROM lowerCaseData"),
      Row(1, "a") ::
      Row(2, "b") ::
      Row(3, "c") ::
      Row(4, "d") :: Nil)
    checkAnswer(
      sql("SELECT * FROM lowerCaseData INTERSECT SELECT * FROM upperCaseData"), Nil)
  }

  test("apply schema") {
    withTempView("applySchema1", "applySchema2", "applySchema3") {
      val schema1 = StructType(
        StructField("f1", IntegerType, false) ::
        StructField("f2", StringType, false) ::
        StructField("f3", BooleanType, false) ::
        StructField("f4", IntegerType, true) :: Nil)

      val rowRDD1 = unparsedStrings.map { r =>
        val values = r.split(",").map(_.trim)
        val v4 = try values(3).toInt catch {
          case _: NumberFormatException => null
        }
        Row(values(0).toInt, values(1), values(2).toBoolean, v4)
      }

      val df1 = spark.createDataFrame(rowRDD1, schema1)
      df1.createOrReplaceTempView("applySchema1")
      checkAnswer(
        sql("SELECT * FROM applySchema1"),
        Row(1, "A1", true, null) ::
        Row(2, "B2", false, null) ::
        Row(3, "C3", true, null) ::
        Row(4, "D4", true, 2147483644) :: Nil)

      checkAnswer(
        sql("SELECT f1, f4 FROM applySchema1"),
        Row(1, null) ::
        Row(2, null) ::
        Row(3, null) ::
        Row(4, 2147483644) :: Nil)

      val schema2 = StructType(
        StructField("f1", StructType(
          StructField("f11", IntegerType, false) ::
          StructField("f12", BooleanType, false) :: Nil), false) ::
        StructField("f2", MapType(StringType, IntegerType, true), false) :: Nil)

      val rowRDD2 = unparsedStrings.map { r =>
        val values = r.split(",").map(_.trim)
        val v4 = try values(3).toInt catch {
          case _: NumberFormatException => null
        }
        Row(Row(values(0).toInt, values(2).toBoolean), Map(values(1) -> v4))
      }

      val df2 = spark.createDataFrame(rowRDD2, schema2)
      df2.createOrReplaceTempView("applySchema2")
      checkAnswer(
        sql("SELECT * FROM applySchema2"),
        Row(Row(1, true), Map("A1" -> null)) ::
        Row(Row(2, false), Map("B2" -> null)) ::
        Row(Row(3, true), Map("C3" -> null)) ::
        Row(Row(4, true), Map("D4" -> 2147483644)) :: Nil)

      // If ANSI mode is on, there will be an error "Key D4 does not exist".
      if (!conf.ansiEnabled) {
        checkAnswer(
          sql("SELECT f1.f11, f2['D4'] FROM applySchema2"),
          Row(1, null) ::
            Row(2, null) ::
            Row(3, null) ::
            Row(4, 2147483644) :: Nil)

        // The value of a MapType column can be a mutable map.
        val rowRDD3 = unparsedStrings.map { r =>
          val values = r.split(",").map(_.trim)
          val v4 = try values(3).toInt catch {
            case _: NumberFormatException => null
          }
          Row(Row(values(0).toInt, values(2).toBoolean),
            scala.collection.mutable.Map(values(1) -> v4))
        }

        val df3 = spark.createDataFrame(rowRDD3, schema2)
        df3.createOrReplaceTempView("applySchema3")

        checkAnswer(
          sql("SELECT f1.f11, f2['D4'] FROM applySchema3"),
          Row(1, null) ::
            Row(2, null) ::
            Row(3, null) ::
            Row(4, 2147483644) :: Nil)
      }
    }
  }

  test("SPARK-3423 BETWEEN") {
    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 5 and 7"),
      Seq(Row(5, "5"), Row(6, "6"), Row(7, "7"))
    )

    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 7 and 7"),
      Row(7, "7")
    )

    checkAnswer(
      sql("SELECT key, value FROM testData WHERE key BETWEEN 9 and 7"),
      Nil
    )
  }

  test("cast boolean to string") {
    // TODO Ensure true/false string letter casing is consistent with Hive in all cases.
    checkAnswer(
      sql("SELECT CAST(TRUE AS STRING), CAST(FALSE AS STRING) FROM testData LIMIT 1"),
      Row("true", "false"))
  }

  test("metadata is propagated correctly") {
    withTempView("personWithMeta") {
      val person: DataFrame = sql("SELECT * FROM person")
      val schema = person.schema
      val docKey = "doc"
      val docValue = "first name"
      val metadata = new MetadataBuilder()
        .putString(docKey, docValue)
        .build()
      val schemaWithMeta = new StructType(Array(
        schema("id"), schema("name").copy(metadata = metadata), schema("age")))
      val personWithMeta = spark.createDataFrame(person.rdd, schemaWithMeta)
      def validateMetadata(rdd: DataFrame): Unit = {
        assert(rdd.schema("name").metadata.getString(docKey) == docValue)
      }
      personWithMeta.createOrReplaceTempView("personWithMeta")
      validateMetadata(personWithMeta.select($"name"))
      validateMetadata(personWithMeta.select($"name"))
      validateMetadata(personWithMeta.select($"id", $"name"))
      validateMetadata(sql("SELECT * FROM personWithMeta"))
      validateMetadata(sql("SELECT id, name FROM personWithMeta"))
      validateMetadata(sql("SELECT * FROM personWithMeta JOIN salary ON id = personId"))
      validateMetadata(sql(
        "SELECT name, salary FROM personWithMeta JOIN salary ON id = personId"))
    }
  }

  test("SPARK-3371 Renaming a function expression with group by gives error") {
    spark.udf.register("len", (s: String) => s.length)
    checkAnswer(
      sql("SELECT len(value) as temp FROM testData WHERE key = 1 group by len(value)"),
      Row(1))
  }

  test("SPARK-3813 CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END") {
    checkAnswer(
      sql("SELECT CASE key WHEN 1 THEN 1 ELSE 0 END FROM testData WHERE key = 1 group by key"),
      Row(1))
  }

  test("SPARK-3813 CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END") {
    checkAnswer(
      sql("SELECT CASE WHEN key = 1 THEN 1 ELSE 2 END FROM testData WHERE key = 1 group by key"),
      Row(1))
  }

  testQuietly(
    "SPARK-16748: SparkExceptions during planning should not wrapped in TreeNodeException") {
    intercept[SparkException] {
      val df = spark.range(0, 5).map(x => (1 / x).toString).toDF("a").orderBy("a")
      df.queryExecution.toRdd // force physical planning, but not execution of the plan
    }
  }

  test("Multiple join") {
    checkAnswer(
      sql(
        """SELECT a.key, b.key, c.key
          |FROM testData a
          |JOIN testData b ON a.key = b.key
          |JOIN testData c ON a.key = c.key
        """.stripMargin),
      (1 to 100).map(i => Row(i, i, i)))
  }

  test("SPARK-3483 Special chars in column names") {
    withTempView("records") {
      val data = Seq("""{"key?number1": "value1", "key.number2": "value2"}""").toDS()
      spark.read.json(data).createOrReplaceTempView("records")
      withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
        sql("SELECT `key?number1`, `key.number2` FROM records")
      }
    }
  }

  test("SPARK-3814 Support Bitwise & operator") {
    checkAnswer(sql("SELECT key&1 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise | operator") {
    checkAnswer(sql("SELECT key|0 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise ^ operator") {
    checkAnswer(sql("SELECT key^0 FROM testData WHERE key = 1 "), Row(1))
  }

  test("SPARK-3814 Support Bitwise ~ operator") {
    checkAnswer(sql("SELECT ~key FROM testData WHERE key = 1 "), Row(-2))
  }

  test("SPARK-4120 Join of multiple tables does not work in SparkSQL") {
    checkAnswer(
      sql(
        """SELECT a.key, b.key, c.key
          |FROM testData a,testData b,testData c
          |where a.key = b.key and a.key = c.key
        """.stripMargin),
      (1 to 100).map(i => Row(i, i, i)))
  }

  test("SPARK-4154 Query does not work if it has 'not between' in Spark SQL and HQL") {
    checkAnswer(sql("SELECT key FROM testData WHERE key not between 0 and 10 order by key"),
        (11 to 100).map(i => Row(i)))
  }

  test("SPARK-4207 Query which has syntax like 'not like' is not working in Spark SQL") {
    checkAnswer(sql("SELECT key FROM testData WHERE value not like '100%' order by key"),
        (1 to 99).map(i => Row(i)))
  }

  test("SPARK-4322 Grouping field with struct field as sub expression") {
    spark.read.json(Seq("""{"a": {"b": [{"c": 1}]}}""").toDS())
      .createOrReplaceTempView("data")
    checkAnswer(sql("SELECT a.b[0].c FROM data GROUP BY a.b[0].c"), Row(1))
    spark.catalog.dropTempView("data")

    spark.read.json(Seq("""{"a": {"b": 1}}""").toDS())
      .createOrReplaceTempView("data")
    checkAnswer(sql("SELECT a.b + 1 FROM data GROUP BY a.b + 1"), Row(2))
    spark.catalog.dropTempView("data")
  }

  test("SPARK-4432 Fix attribute reference resolution error when using ORDER BY") {
    checkAnswer(
      sql("SELECT a + b FROM testData2 ORDER BY a"),
      Seq(2, 3, 3, 4, 4, 5).map(Row(_))
    )
  }

  test("order by asc by default when not specify ascending and descending") {
    checkAnswer(
      sql("SELECT a, b FROM testData2 ORDER BY a desc, b"),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2))
    )
  }

  test("Supporting relational operator '<=>' in Spark SQL") {
    withTempView("nulldata1", "nulldata2") {
      val nullCheckData1 = TestData(1, "1") :: TestData(2, null) :: Nil
      val rdd1 = sparkContext.parallelize((0 to 1).map(i => nullCheckData1(i)))
      rdd1.toDF().createOrReplaceTempView("nulldata1")
      val nullCheckData2 = TestData(1, "1") :: TestData(2, null) :: Nil
      val rdd2 = sparkContext.parallelize((0 to 1).map(i => nullCheckData2(i)))
      rdd2.toDF().createOrReplaceTempView("nulldata2")
      checkAnswer(sql("SELECT nulldata1.key FROM nulldata1 join " +
        "nulldata2 on nulldata1.value <=> nulldata2.value"),
        (1 to 2).map(i => Row(i)))
    }
  }

  test("Multi-column COUNT(DISTINCT ...)") {
    withTempView("distinctData") {
      val data = TestData(1, "val_1") :: TestData(2, "val_2") :: Nil
      val rdd = sparkContext.parallelize((0 to 1).map(i => data(i)))
      rdd.toDF().createOrReplaceTempView("distinctData")
      checkAnswer(sql("SELECT COUNT(DISTINCT key,value) FROM distinctData"), Row(2))
    }
  }

  test("SPARK-4699 case sensitivity SQL query") {
    withTempView("testTable1") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        val data = TestData(1, "val_1") :: TestData(2, "val_2") :: Nil
        val rdd = sparkContext.parallelize((0 to 1).map(i => data(i)))
        rdd.toDF().createOrReplaceTempView("testTable1")
        checkAnswer(sql("SELECT VALUE FROM TESTTABLE1 where KEY = 1"), Row("val_1"))
      }
    }
  }

  test("SPARK-6145: ORDER BY test for nested fields") {
    withTempView("nestedOrder") {
      spark.read
        .json(Seq("""{"a": {"b": 1, "a": {"a": 1}}, "c": [{"d": 1}]}""").toDS())
        .createOrReplaceTempView("nestedOrder")

      checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY a.b"), Row(1))
      checkAnswer(sql("SELECT a.b FROM nestedOrder ORDER BY a.b"), Row(1))
      checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY a.a.a"), Row(1))
      checkAnswer(sql("SELECT a.a.a FROM nestedOrder ORDER BY a.a.a"), Row(1))
      checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY c[0].d"), Row(1))
      checkAnswer(sql("SELECT c[0].d FROM nestedOrder ORDER BY c[0].d"), Row(1))
    }
  }

  test("SPARK-6145: special cases") {
    withTempView("t") {
      spark.read
        .json(Seq("""{"a": {"b": [1]}, "b": [{"a": 1}], "_c0": {"a": 1}}""").toDS())
        .createOrReplaceTempView("t")

      checkAnswer(sql("SELECT a.b[0] FROM t ORDER BY _c0.a"), Row(1))
      checkAnswer(sql("SELECT b[0].a FROM t ORDER BY _c0.a"), Row(1))
    }
  }

  test("SPARK-6898: complete support for special chars in column names") {
    withTempView("t") {
      spark.read
        .json(Seq("""{"a": {"c.b": 1}, "b.$q": [{"a@!.q": 1}], "q.w": {"w.i&": [1]}}""").toDS())
        .createOrReplaceTempView("t")

      withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
        checkAnswer(sql("SELECT a.`c.b`, `b.$q`[0].`a@!.q`, `q.w`.`w.i&`[0] FROM t"), Row(1, 1, 1))
      }
    }
  }

  test("SPARK-6583 order by aggregated function") {
    withTempView("orderByData") {
      Seq("1" -> 3, "1" -> 4, "2" -> 7, "2" -> 8, "3" -> 5, "3" -> 6, "4" -> 1, "4" -> 2)
        .toDF("a", "b").createOrReplaceTempView("orderByData")

      checkAnswer(
        sql(
          """
            |SELECT a
            |FROM orderByData
            |GROUP BY a
            |ORDER BY sum(b)
        """.stripMargin),
        Row("4") :: Row("1") :: Row("3") :: Row("2") :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT sum(b)
            |FROM orderByData
            |GROUP BY a
            |ORDER BY sum(b)
        """.stripMargin),
        Row(3) :: Row(7) :: Row(11) :: Row(15) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT sum(b)
            |FROM orderByData
            |GROUP BY a
            |ORDER BY sum(b), max(b)
        """.stripMargin),
        Row(3) :: Row(7) :: Row(11) :: Row(15) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, sum(b)
            |FROM orderByData
            |GROUP BY a
            |ORDER BY sum(b)
        """.stripMargin),
        Row("4", 3) :: Row("1", 7) :: Row("3", 11) :: Row("2", 15) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, sum(b)
            |FROM orderByData
            |GROUP BY a
            |ORDER BY sum(b) + 1
          """.stripMargin),
        Row("4", 3) :: Row("1", 7) :: Row("3", 11) :: Row("2", 15) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT count(*)
            |FROM orderByData
            |GROUP BY a
            |ORDER BY count(*)
          """.stripMargin),
        Row(2) :: Row(2) :: Row(2) :: Row(2) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a
            |FROM orderByData
            |GROUP BY a
            |ORDER BY a, count(*), sum(b)
          """.stripMargin),
        Row("1") :: Row("2") :: Row("3") :: Row("4") :: Nil)
    }
  }

  test("SPARK-7952: fix the equality check between boolean and numeric types") {
    // If ANSI mode is on, Spark disallows comparing Int with Boolean.
    if (!conf.ansiEnabled) {
      withTempView("t") {
        // numeric field i, boolean field j, result of i = j, result of i <=> j
        Seq[(Integer, java.lang.Boolean, java.lang.Boolean, java.lang.Boolean)](
          (1, true, true, true),
          (0, false, true, true),
          (2, true, false, false),
          (2, false, false, false),
          (null, true, null, false),
          (null, false, null, false),
          (0, null, null, false),
          (1, null, null, false),
          (null, null, null, true)
        ).toDF("i", "b", "r1", "r2").createOrReplaceTempView("t")

        checkAnswer(sql("select i = b from t"), sql("select r1 from t"))
        checkAnswer(sql("select i <=> b from t"), sql("select r2 from t"))
      }
    }
  }

  test("SPARK-7067: order by queries for complex ExtractValue chain") {
    withTempView("t") {
      spark.read
        .json(Seq("""{"a": {"b": [{"c": 1}]}, "b": [{"d": 1}]}""").toDS())
        .createOrReplaceTempView("t")
      checkAnswer(sql("SELECT a.b FROM t ORDER BY b[0].d"), Row(Seq(Row(1))))
    }
  }

  test("SPARK-8782: ORDER BY NULL") {
    withTempView("t") {
      Seq((1, 2), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(sql("SELECT * FROM t ORDER BY NULL"), Seq(Row(1, 2), Row(1, 2)))
    }
  }

  test("SPARK-8837: use keyword in column name") {
    withTempView("t") {
      val df = Seq(1 -> "a").toDF("count", "sort")
      checkAnswer(df.filter("count > 0"), Row(1, "a"))
      df.createOrReplaceTempView("t")
      checkAnswer(sql("select count, sort from t"), Row(1, "a"))
    }
  }

  test("SPARK-8753: add interval type") {
    import org.apache.spark.unsafe.types.CalendarInterval

    val ymDF = sql("select interval 3 years -3 month")
    checkAnswer(ymDF, Row(Period.of(2, 9, 0)))

    val dtDF = sql("select interval 5 days 8 hours 12 minutes 50 seconds")
    checkAnswer(dtDF, Row(Duration.ofDays(5).plusHours(8).plusMinutes(12).plusSeconds(50)))

    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      val df = sql("select interval 3 years -3 month 7 week 123 microseconds")
      checkAnswer(df, Row(new CalendarInterval(12 * 3 - 3, 7 * 7, 123)))
    }
  }

  test("SPARK-8945: add and subtract expressions for interval type") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      val df = sql("select interval 3 years -3 month 7 week 123 microseconds as i")
      checkAnswer(df, Row(new CalendarInterval(12 * 3 - 3, 7 * 7, 123)))

      checkAnswer(df.select(df("i") + new CalendarInterval(2, 1, 123)),
        Row(new CalendarInterval(12 * 3 - 3 + 2, 7 * 7 + 1, 123 + 123)))

      checkAnswer(df.select(df("i") - new CalendarInterval(2, 1, 123)),
        Row(new CalendarInterval(12 * 3 - 3 - 2, 7 * 7 - 1, 123 - 123)))

      // unary minus
      checkAnswer(df.select(-df("i")),
        Row(new CalendarInterval(-(12 * 3 - 3), -7 * 7, -123)))
    }
  }

  test("aggregation with codegen updates peak execution memory") {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sparkContext, "aggregation with codegen") {
      testCodeGen(
        "SELECT key, count(value) FROM testData GROUP BY key",
        (1 to 100).map(i => Row(i, 1)))
    }
  }

  test("SPARK-10215 Div of Decimal returns null") {
    val d = Decimal(1.12321).toBigDecimal
    val df = Seq((d, 1)).toDF("a", "b")

    checkAnswer(
      df.selectExpr("b * a / b"),
      Seq(Row(d)))
    checkAnswer(
      df.selectExpr("b * a / b / b"),
      Seq(Row(d)))
    checkAnswer(
      df.selectExpr("b * a + b"),
      Seq(Row(BigDecimal("2.12321"))))
    checkAnswer(
      df.selectExpr("b * a - b"),
      Seq(Row(BigDecimal("0.12321"))))
    checkAnswer(
      df.selectExpr("b * a * b"),
      Seq(Row(d)))
  }

  test("precision smaller than scale") {
    checkAnswer(sql("select 10.00"), Row(BigDecimal("10.00")))
    checkAnswer(sql("select 1.00"), Row(BigDecimal("1.00")))
    checkAnswer(sql("select 0.10"), Row(BigDecimal("0.10")))
    checkAnswer(sql("select 0.01"), Row(BigDecimal("0.01")))
    checkAnswer(sql("select 0.001"), Row(BigDecimal("0.001")))
    checkAnswer(sql("select -0.01"), Row(BigDecimal("-0.01")))
    checkAnswer(sql("select -0.001"), Row(BigDecimal("-0.001")))
  }

  test("external sorting updates peak execution memory") {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sparkContext, "external sort") {
      sql("SELECT * FROM testData2 ORDER BY a ASC, b ASC").collect()
    }
  }

  test("SPARK-9511: error with table starting with number") {
    withTempView("1one") {
      sparkContext.parallelize(1 to 10).map(i => (i, i.toString))
        .toDF("num", "str")
        .createOrReplaceTempView("1one")
      checkAnswer(sql("select count(num) from 1one"), Row(10))
    }
  }

  test("specifying database name for a temporary view is not allowed") {
    withTempPath { dir =>
      withTempView("db.t") {
        val path = dir.toURI.toString
        val df =
          sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")
        df
          .write
          .format("parquet")
          .save(path)

        // We don't support creating a temporary table while specifying a database
        intercept[AnalysisException] {
          spark.sql(
            s"""
              |CREATE TEMPORARY VIEW db.t
              |USING parquet
              |OPTIONS (
              |  path '$path'
              |)
             """.stripMargin)
        }.getMessage

        // If you use backticks to quote the name then it's OK.
        spark.sql(
          s"""
            |CREATE TEMPORARY VIEW `db.t`
            |USING parquet
            |OPTIONS (
            |  path '$path'
            |)
           """.stripMargin)
        checkAnswer(spark.table("`db.t`"), df)
      }
    }
  }

  test("SPARK-10130 type coercion for IF should have children resolved first") {
    withTempView("src") {
      Seq((1, 1), (-1, 1)).toDF("key", "value").createOrReplaceTempView("src")
      checkAnswer(
        sql("SELECT IF(a > 0, a, 0) FROM (SELECT key a FROM src) temp"), Seq(Row(1), Row(0)))
    }
  }

  test("SPARK-10389: order by non-attribute grouping expression on Aggregate") {
    withTempView("src") {
      Seq((1, 1), (-1, 1)).toDF("key", "value").createOrReplaceTempView("src")
      checkAnswer(sql("SELECT MAX(value) FROM src GROUP BY key + 1 ORDER BY key + 1"),
        Seq(Row(1), Row(1)))
      checkAnswer(sql("SELECT MAX(value) FROM src GROUP BY key + 1 ORDER BY (key + 1) * 2"),
        Seq(Row(1), Row(1)))
    }
  }

  test("SPARK-23281: verify the correctness of sort direction on composite order by clause") {
    withTempView("src") {
      Seq[(Integer, Integer)](
        (1, 1),
        (1, 3),
        (2, 3),
        (3, 3),
        (4, null),
        (5, null)
      ).toDF("key", "value").createOrReplaceTempView("src")

      checkAnswer(sql(
        """
          |SELECT MAX(value) as value, key as col2
          |FROM src
          |GROUP BY key
          |ORDER BY value desc, key
        """.stripMargin),
        Seq(Row(3, 1), Row(3, 2), Row(3, 3), Row(null, 4), Row(null, 5)))

      checkAnswer(sql(
        """
          |SELECT MAX(value) as value, key as col2
          |FROM src
          |GROUP BY key
          |ORDER BY value desc, key desc
        """.stripMargin),
        Seq(Row(3, 3), Row(3, 2), Row(3, 1), Row(null, 5), Row(null, 4)))

      checkAnswer(sql(
        """
          |SELECT MAX(value) as value, key as col2
          |FROM src
          |GROUP BY key
          |ORDER BY value asc, key desc
        """.stripMargin),
        Seq(Row(null, 5), Row(null, 4), Row(3, 3), Row(3, 2), Row(3, 1)))
    }
  }

  test("run sql directly on files") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.json(f.getCanonicalPath)
      checkAnswer(sql(s"select id from json.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select id from `org.apache.spark.sql.json`.`${f.getCanonicalPath}`"),
        df)
      checkAnswer(sql(s"select a.id from json.`${f.getCanonicalPath}` as a"),
        df)
    })

    var e = intercept[AnalysisException] {
      sql("select * from in_valid_table")
    }
    assert(e.message.contains("Table or view not found"))

    e = intercept[AnalysisException] {
      sql("select * from no_db.no_table").show()
    }
    assert(e.message.contains("Table or view not found"))

    e = intercept[AnalysisException] {
      sql("select * from json.invalid_file")
    }
    assert(e.message.contains("Path does not exist"))

    e = intercept[AnalysisException] {
      sql(s"select id from `org.apache.spark.sql.hive.orc`.`file_path`")
    }
    assert(e.message.contains("Hive built-in ORC data source must be used with Hive support"))

    e = intercept[AnalysisException] {
      sql(s"select id from `org.apache.spark.sql.sources.HadoopFsRelationProvider`.`file_path`")
    }
    assert(e.message.contains("Table or view not found: " +
      "`org.apache.spark.sql.sources.HadoopFsRelationProvider`.file_path"))

    e = intercept[AnalysisException] {
      sql(s"select id from `Jdbc`.`file_path`")
    }
    assert(e.message.contains("Unsupported data source type for direct query on files: Jdbc"))

    e = intercept[AnalysisException] {
      sql(s"select id from `org.apache.spark.sql.execution.datasources.jdbc`.`file_path`")
    }
    assert(e.message.contains("Unsupported data source type for direct query on files: " +
      "org.apache.spark.sql.execution.datasources.jdbc"))
  }

  test("SortMergeJoin returns wrong results when using UnsafeRows") {
    // This test is for the fix of https://issues.apache.org/jira/browse/SPARK-10737.
    // This bug will be triggered when Tungsten is enabled and there are multiple
    // SortMergeJoin operators executed in the same task.
    val confs = SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "1" :: Nil
    withSQLConf(confs: _*) {
      val df1 = (1 to 50).map(i => (s"str_$i", i)).toDF("i", "j")
      val df2 =
        df1
          .join(df1.select(df1("i")), "i")
          .select(df1("i"), df1("j"))

      val df3 = df2.withColumnRenamed("i", "i1").withColumnRenamed("j", "j1")
      val df4 =
        df2
          .join(df3, df2("i") === df3("i1"))
          .withColumn("diff", $"j" - $"j1")
          .select(df2("i"), df2("j"), $"diff")

      checkAnswer(
        df4,
        df1.withColumn("diff", lit(0)))
    }
  }

  test("SPARK-11303: filter should not be pushed down into sample") {
    val df = spark.range(100)
    List(true, false).foreach { withReplacement =>
      val sampled = df.sample(withReplacement, 0.1, 1)
      val sampledOdd = sampled.filter("id % 2 != 0")
      val sampledEven = sampled.filter("id % 2 = 0")
      assert(sampled.count() == sampledOdd.count() + sampledEven.count())
    }
  }

  test("Struct Star Expansion") {
    withTempView("structTable", "nestedStructTable", "specialCharacterTable", "nameConflict") {
      val structDf = testData2.select("a", "b").as("record")

      checkAnswer(
        structDf.select($"record.a", $"record.b"),
        Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)

      checkAnswer(
        structDf.select($"record.*"),
        Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)

      checkAnswer(
        structDf.select($"record.*", $"record.*"),
        Row(1, 1, 1, 1) :: Row(1, 2, 1, 2) :: Row(2, 1, 2, 1) :: Row(2, 2, 2, 2) ::
          Row(3, 1, 3, 1) :: Row(3, 2, 3, 2) :: Nil)

      checkAnswer(
        sql("select struct(a, b) as r1, struct(b, a) as r2 from testData2")
          .select($"r1.*", $"r2.*"),
        Row(1, 1, 1, 1) :: Row(1, 2, 2, 1) :: Row(2, 1, 1, 2) :: Row(2, 2, 2, 2) ::
          Row(3, 1, 1, 3) :: Row(3, 2, 2, 3) :: Nil)

      // Try with a temporary view
      sql("select struct(a, b) as record from testData2").createOrReplaceTempView("structTable")
      checkAnswer(
        sql("SELECT record.* FROM structTable"),
        Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)

      checkAnswer(sql(
        """
          | SELECT min(struct(record.*)) FROM
          |   (select struct(a,b) as record from testData2) tmp
      """.stripMargin),
        Row(Row(1, 1)) :: Nil)

      // Try with an alias on the select list
      checkAnswer(sql(
        """
          | SELECT max(struct(record.*)) as r FROM
          |   (select struct(a,b) as record from testData2) tmp
      """.stripMargin).select($"r.*"),
        Row(3, 2) :: Nil)

      // With GROUP BY
      checkAnswer(sql(
        """
          | SELECT min(struct(record.*)) FROM
          |   (select a as a, struct(a,b) as record from testData2) tmp
          | GROUP BY a
      """.stripMargin),
        Row(Row(1, 1)) :: Row(Row(2, 1)) :: Row(Row(3, 1)) :: Nil)

      // With GROUP BY and alias
      checkAnswer(sql(
        """
          | SELECT max(struct(record.*)) as r FROM
          |   (select a as a, struct(a,b) as record from testData2) tmp
          | GROUP BY a
      """.stripMargin).select($"r.*"),
        Row(1, 2) :: Row(2, 2) :: Row(3, 2) :: Nil)

      // With GROUP BY and alias and additional fields in the struct
      checkAnswer(sql(
        """
          | SELECT max(struct(a, record.*, b)) as r FROM
          |   (select a as a, b as b, struct(a,b) as record from testData2) tmp
          | GROUP BY a
      """.stripMargin).select($"r.*"),
        Row(1, 1, 2, 2) :: Row(2, 2, 2, 2) :: Row(3, 3, 2, 2) :: Nil)

      // Create a data set that contains nested structs.
      val nestedStructData = sql(
        """
          | SELECT struct(r1, r2) as record FROM
          |   (SELECT struct(a, b) as r1, struct(b, a) as r2 FROM testData2) tmp
      """.stripMargin)

      checkAnswer(nestedStructData.select($"record.*"),
        Row(Row(1, 1), Row(1, 1)) :: Row(Row(1, 2), Row(2, 1)) :: Row(Row(2, 1), Row(1, 2)) ::
          Row(Row(2, 2), Row(2, 2)) :: Row(Row(3, 1), Row(1, 3)) :: Row(Row(3, 2), Row(2, 3)) ::
          Nil)
      checkAnswer(nestedStructData.select($"record.r1"),
        Row(Row(1, 1)) :: Row(Row(1, 2)) :: Row(Row(2, 1)) :: Row(Row(2, 2)) ::
          Row(Row(3, 1)) :: Row(Row(3, 2)) :: Nil)
      checkAnswer(
        nestedStructData.select($"record.r1.*"),
        Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)

      // Try with a temporary view
      withTempView("nestedStructTable") {
        nestedStructData.createOrReplaceTempView("nestedStructTable")
        checkAnswer(
          sql("SELECT record.* FROM nestedStructTable"),
          nestedStructData.select($"record.*"))
        checkAnswer(
          sql("SELECT record.r1 FROM nestedStructTable"),
          nestedStructData.select($"record.r1"))
        checkAnswer(
          sql("SELECT record.r1.* FROM nestedStructTable"),
          nestedStructData.select($"record.r1.*"))

        // Try resolving something not there.
        assert(intercept[AnalysisException](sql("SELECT abc.* FROM nestedStructTable"))
          .getMessage.contains("cannot resolve"))
      }

      // Create paths with unusual characters
      withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "false") {
        val specialCharacterPath = sql(
          """
          | SELECT struct(`col$.a_`, `a.b.c.`) as `r&&b.c` FROM
          |   (SELECT struct(a, b) as `col$.a_`, struct(b, a) as `a.b.c.` FROM testData2) tmp
      """.stripMargin)
        withTempView("specialCharacterTable") {
          specialCharacterPath.createOrReplaceTempView("specialCharacterTable")
          checkAnswer(
            specialCharacterPath.select($"`r&&b.c`.*"),
            nestedStructData.select($"record.*"))
          checkAnswer(
          sql(
            "SELECT `r&&b.c`.`col$.a_` FROM specialCharacterTable"),
          nestedStructData.select($"record.r1"))
          checkAnswer(
            sql("SELECT `r&&b.c`.`a.b.c.` FROM specialCharacterTable"),
            nestedStructData.select($"record.r2"))
          checkAnswer(
            sql("SELECT `r&&b.c`.`col$.a_`.* FROM specialCharacterTable"),
            nestedStructData.select($"record.r1.*"))
        }
      }

      // Try star expanding a scalar. This should fail.
      assert(intercept[AnalysisException](sql("select a.* from testData2")).getMessage.contains(
        "Can only star expand struct data types."))
    }
  }

  test("Struct Star Expansion - Name conflict") {
    // Create a data set that contains a naming conflict
    val nameConflict = sql("SELECT struct(a, b) as nameConflict, a as a FROM testData2")
    withTempView("nameConflict") {
      nameConflict.createOrReplaceTempView("nameConflict")
      // Unqualified should resolve to table.
      checkAnswer(sql("SELECT nameConflict.* FROM nameConflict"),
        Row(Row(1, 1), 1) :: Row(Row(1, 2), 1) :: Row(Row(2, 1), 2) :: Row(Row(2, 2), 2) ::
          Row(Row(3, 1), 3) :: Row(Row(3, 2), 3) :: Nil)
      // Qualify the struct type with the table name.
      checkAnswer(sql("SELECT nameConflict.nameConflict.* FROM nameConflict"),
        Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)
    }
  }

  test("Star Expansion - group by") {
    withSQLConf(SQLConf.DATAFRAME_RETAIN_GROUP_COLUMNS.key -> "false") {
      checkAnswer(
        testData2.groupBy($"a", $"b").agg($"*"),
        sql("SELECT * FROM testData2 group by a, b"))
    }
  }

  test("Star Expansion - table with zero column") {
    withTempView("temp_table_no_cols") {
      val rddNoCols = sparkContext.parallelize(1 to 10).map(_ => Row.empty)
      val dfNoCols = spark.createDataFrame(rddNoCols, StructType(Seq.empty))
      dfNoCols.createTempView("temp_table_no_cols")

      // ResolvedStar
      checkAnswer(
        dfNoCols,
        dfNoCols.select(dfNoCols.col("*")))

      // UnresolvedStar
      checkAnswer(
        dfNoCols,
        sql("SELECT * FROM temp_table_no_cols"))
      checkAnswer(
        dfNoCols,
        dfNoCols.select($"*"))

      var e = intercept[AnalysisException] {
        sql("SELECT a.* FROM temp_table_no_cols a")
      }.getMessage
      assert(e.contains("cannot resolve 'a.*' given input columns ''"))

      e = intercept[AnalysisException] {
        dfNoCols.select($"b.*")
      }.getMessage
      assert(e.contains("cannot resolve 'b.*' given input columns ''"))
    }
  }

  test("Common subexpression elimination") {
    // TODO: support subexpression elimination in whole stage codegen
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      // select from a table to prevent constant folding.
      val df = sql("SELECT a, b from testData2 limit 1")
      checkAnswer(df, Row(1, 1))

      checkAnswer(df.selectExpr("a + 1", "a + 1"), Row(2, 2))
      checkAnswer(df.selectExpr("a + 1", "a + 1 + 1"), Row(2, 3))

      // This does not work because the expressions get grouped like (a + a) + 1
      checkAnswer(df.selectExpr("a + 1", "a + a + 1"), Row(2, 3))
      checkAnswer(df.selectExpr("a + 1", "a + (a + 1)"), Row(2, 3))

      // Identity udf that tracks the number of times it is called.
      val countAcc = sparkContext.longAccumulator("CallCount")
      spark.udf.register("testUdf", (x: Int) => {
        countAcc.add(1)
        x
      })

      // Evaluates df, verifying it is equal to the expectedResult and the accumulator's value
      // is correct.
      def verifyCallCount(df: DataFrame, expectedResult: Row, expectedCount: Int): Unit = {
        countAcc.setValue(0)
        QueryTest.checkAnswer(
          df, Seq(expectedResult), checkToRDD = false /* avoid duplicate exec */)
        assert(countAcc.value == expectedCount)
      }

      verifyCallCount(df.selectExpr("testUdf(a)"), Row(1), 1)
      verifyCallCount(df.selectExpr("testUdf(a)", "testUdf(a)"), Row(1, 1), 1)
      verifyCallCount(df.selectExpr("testUdf(a + 1)", "testUdf(a + 1)"), Row(2, 2), 1)
      verifyCallCount(df.selectExpr("testUdf(a + 1)", "testUdf(a)"), Row(2, 1), 2)
      verifyCallCount(
        df.selectExpr("testUdf(a + 1) + testUdf(a + 1)", "testUdf(a + 1)"), Row(4, 2), 1)

      verifyCallCount(
        df.selectExpr("testUdf(a + 1) + testUdf(1 + b)", "testUdf(a + 1)"), Row(4, 2), 2)

      val testUdf = functions.udf((x: Int) => {
        countAcc.add(1)
        x
      })
      verifyCallCount(
        df.agg(sum(testUdf($"b") + testUdf($"b") + testUdf($"b"))), Row(3.0), 1)

      verifyCallCount(
        df.selectExpr("testUdf(a + 1) + testUdf(1 + a)", "testUdf(a + 1)"), Row(4, 2), 1)

      // Try disabling it via configuration.
      spark.conf.set(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key, "false")
      verifyCallCount(df.selectExpr("testUdf(a)", "testUdf(a)"), Row(1, 1), 2)
      spark.conf.set(SQLConf.SUBEXPRESSION_ELIMINATION_ENABLED.key, "true")
      verifyCallCount(df.selectExpr("testUdf(a)", "testUdf(a)"), Row(1, 1), 1)
    }
  }

  test("SPARK-10707: nullability should be correctly propagated through set operations (1)") {
    // This test produced an incorrect result of 1 before the SPARK-10707 fix because of the
    // NullPropagation rule: COUNT(v) got replaced with COUNT(1) because the output column of
    // UNION was incorrectly considered non-nullable:
    checkAnswer(
      sql("""SELECT count(v) FROM (
            |  SELECT v FROM (
            |    SELECT 'foo' AS v UNION ALL
            |    SELECT NULL AS v
            |  ) my_union WHERE isnull(v)
            |) my_subview""".stripMargin),
      Seq(Row(0)))
  }

  test("SPARK-10707: nullability should be correctly propagated through set operations (2)") {
    // This test uses RAND() to stop column pruning for Union and checks the resulting isnull
    // value. This would produce an incorrect result before the fix in SPARK-10707 because the "v"
    // column of the union was considered non-nullable.
    checkAnswer(
      sql(
        """
          |SELECT a FROM (
          |  SELECT ISNULL(v) AS a, RAND() FROM (
          |    SELECT 'foo' AS v UNION ALL SELECT null AS v
          |  ) my_union
          |) my_view
        """.stripMargin),
      Row(false) :: Row(true) :: Nil)
  }

  test("filter on a grouping column that is not presented in SELECT") {
    checkAnswer(
      sql("select count(1) from (select 1 as a) t group by a having a > 0"),
      Row(1) :: Nil)
  }

  test("SPARK-13056: Null in map value causes NPE") {
    val df = Seq(1 -> Map("abc" -> "somestring", "cba" -> null)).toDF("key", "value")
    withTempView("maptest") {
      df.createOrReplaceTempView("maptest")
      // local optimization will by pass codegen code, so we should keep the filter `key=1`
      checkAnswer(sql("SELECT value['abc'] FROM maptest where key = 1"), Row("somestring"))
      checkAnswer(sql("SELECT value['cba'] FROM maptest where key = 1"), Row(null))
    }
  }

  test("hash function") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    withTempView("tbl") {
      df.createOrReplaceTempView("tbl")
      checkAnswer(
        df.select(hash($"i", $"j")),
        sql("SELECT hash(i, j) from tbl")
      )
    }
  }

  test("SPARK-27619: Throw analysis exception when hash and xxhash64 is used on MapType") {
    Seq("hash", "xxhash64").foreach {
      case hashExpression =>
        intercept[AnalysisException] {
          spark.createDataset(Map(1 -> 10, 2 -> 20) :: Nil).selectExpr(s"$hashExpression(*)")
        }
    }
  }

  test(s"SPARK-27619: When ${SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE.key} is true, hash can be " +
    "used on Maptype") {
    Seq("hash", "xxhash64").foreach {
      case hashExpression =>
        withSQLConf(SQLConf.LEGACY_ALLOW_HASH_ON_MAPTYPE.key -> "true") {
          val df = spark.createDataset(Map() :: Nil)
          checkAnswer(df.selectExpr(s"$hashExpression(*)"), sql(s"SELECT $hashExpression(map())"))
        }
    }
  }

  test("xxhash64 function") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    withTempView("tbl") {
      df.createOrReplaceTempView("tbl")
      checkAnswer(
        df.select(xxhash64($"i", $"j")),
        sql("SELECT xxhash64(i, j) from tbl")
      )
    }
  }

  test("join with using clause") {
    val df1 = Seq(("r1c1", "r1c2", "t1r1c3"),
      ("r2c1", "r2c2", "t1r2c3"), ("r3c1x", "r3c2", "t1r3c3")).toDF("c1", "c2", "c3")
    val df2 = Seq(("r1c1", "r1c2", "t2r1c3"),
      ("r2c1", "r2c2", "t2r2c3"), ("r3c1y", "r3c2", "t2r3c3")).toDF("c1", "c2", "c3")
    val df3 = Seq((null, "r1c2", "t3r1c3"),
      ("r2c1", "r2c2", "t3r2c3"), ("r3c1y", "r3c2", "t3r3c3")).toDF("c1", "c2", "c3")
    withTempView("t1", "t2", "t3") {
      df1.createOrReplaceTempView("t1")
      df2.createOrReplaceTempView("t2")
      df3.createOrReplaceTempView("t3")
      // inner join with one using column
      checkAnswer(
        sql("SELECT * FROM t1 join t2 using (c1)"),
        Row("r1c1", "r1c2", "t1r1c3", "r1c2", "t2r1c3") ::
          Row("r2c1", "r2c2", "t1r2c3", "r2c2", "t2r2c3") :: Nil)

      // inner join with two using columns
      checkAnswer(
        sql("SELECT * FROM t1 join t2 using (c1, c2)"),
        Row("r1c1", "r1c2", "t1r1c3", "t2r1c3") ::
          Row("r2c1", "r2c2", "t1r2c3", "t2r2c3") :: Nil)

      // Left outer join with one using column.
      checkAnswer(
        sql("SELECT * FROM t1 left join t2 using (c1)"),
        Row("r1c1", "r1c2", "t1r1c3", "r1c2", "t2r1c3") ::
          Row("r2c1", "r2c2", "t1r2c3", "r2c2", "t2r2c3") ::
          Row("r3c1x", "r3c2", "t1r3c3", null, null) :: Nil)

      // Right outer join with one using column.
      checkAnswer(
        sql("SELECT * FROM t1 right join t2 using (c1)"),
        Row("r1c1", "r1c2", "t1r1c3", "r1c2", "t2r1c3") ::
          Row("r2c1", "r2c2", "t1r2c3", "r2c2", "t2r2c3") ::
          Row("r3c1y", null, null, "r3c2", "t2r3c3") :: Nil)

      // Full outer join with one using column.
      checkAnswer(
        sql("SELECT * FROM t1 full outer join t2 using (c1)"),
        Row("r1c1", "r1c2", "t1r1c3", "r1c2", "t2r1c3") ::
          Row("r2c1", "r2c2", "t1r2c3", "r2c2", "t2r2c3") ::
          Row("r3c1x", "r3c2", "t1r3c3", null, null) ::
          Row("r3c1y", null,
            null, "r3c2", "t2r3c3") :: Nil)

      // Full outer join with null value in join column.
      checkAnswer(
        sql("SELECT * FROM t1 full outer join t3 using (c1)"),
        Row("r1c1", "r1c2", "t1r1c3", null, null) ::
          Row("r2c1", "r2c2", "t1r2c3", "r2c2", "t3r2c3") ::
          Row("r3c1x", "r3c2", "t1r3c3", null, null) ::
          Row("r3c1y", null, null, "r3c2", "t3r3c3") ::
          Row(null, null, null, "r1c2", "t3r1c3") :: Nil)

      // Self join with using columns.
      checkAnswer(
        sql("SELECT * FROM t1 join t1 using (c1)"),
        Row("r1c1", "r1c2", "t1r1c3", "r1c2", "t1r1c3") ::
          Row("r2c1", "r2c2", "t1r2c3", "r2c2", "t1r2c3") ::
          Row("r3c1x", "r3c2", "t1r3c3", "r3c2", "t1r3c3") :: Nil)
    }
  }

  test("SPARK-15327: fail to compile generated code with complex data structure") {
    withTempDir { dir =>
      val json =
        """
          |{"h": {"b": {"c": [{"e": "adfgd"}], "a": [{"e": "testing", "count": 3}],
          |"b": [{"e": "test", "count": 1}]}}, "d": {"b": {"c": [{"e": "adfgd"}],
          |"a": [{"e": "testing", "count": 3}], "b": [{"e": "test", "count": 1}]}},
          |"c": {"b": {"c": [{"e": "adfgd"}], "a": [{"count": 3}],
          |"b": [{"e": "test", "count": 1}]}}, "a": {"b": {"c": [{"e": "adfgd"}],
          |"a": [{"count": 3}], "b": [{"e": "test", "count": 1}]}},
          |"e": {"b": {"c": [{"e": "adfgd"}], "a": [{"e": "testing", "count": 3}],
          |"b": [{"e": "test", "count": 1}]}}, "g": {"b": {"c": [{"e": "adfgd"}],
          |"a": [{"e": "testing", "count": 3}], "b": [{"e": "test", "count": 1}]}},
          |"f": {"b": {"c": [{"e": "adfgd"}], "a": [{"e": "testing", "count": 3}],
          |"b": [{"e": "test", "count": 1}]}}, "b": {"b": {"c": [{"e": "adfgd"}],
          |"a": [{"count": 3}], "b": [{"e": "test", "count": 1}]}}}'
          |
        """.stripMargin
      spark.read.json(Seq(json).toDS()).write.mode("overwrite").parquet(dir.toString)
      spark.read.parquet(dir.toString).collect()
    }
  }

  test("data source table created in InMemoryCatalog should be able to read/write") {
    withTable("tbl") {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE tbl(i INT, j STRING) USING $provider")
      checkAnswer(sql("SELECT i, j FROM tbl"), Nil)

      Seq(1 -> "a", 2 -> "b").toDF("i", "j").write.mode("overwrite").insertInto("tbl")
      checkAnswer(sql("SELECT i, j FROM tbl"), Row(1, "a") :: Row(2, "b") :: Nil)

      Seq(3 -> "c", 4 -> "d").toDF("i", "j").write.mode("append").saveAsTable("tbl")
      checkAnswer(
        sql("SELECT i, j FROM tbl"),
        Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Row(4, "d") :: Nil)
    }
  }

  test("Eliminate noop ordinal ORDER BY") {
    withSQLConf(SQLConf.ORDER_BY_ORDINAL.key -> "true") {
      val plan1 = sql("SELECT 1.0, 'abc', year(current_date()) ORDER BY 1, 2, 3")
      val plan2 = sql("SELECT 1.0, 'abc', year(current_date())")
      comparePlans(plan1.queryExecution.optimizedPlan, plan2.queryExecution.optimizedPlan)
    }
  }

  test("check code injection is prevented") {
    // The end of comment (*/) should be escaped.
    var literal =
      """|*/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    var expected =
      """|*/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    // `\u002A` is `*` and `\u002F` is `/`
    // so if the end of comment consists of those characters in queries, we need to escape them.
    literal =
      """|\\u002A/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"\\u002A/"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\\\u002A/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      """|\\u002A/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\u002a/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"\\u002a/"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\\\u002a/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      """|\\u002a/
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|*\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"*\\u002F"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|*\\\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      """|*\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|*\\u002f
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"*\\u002f"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|*\\\\u002f
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      """|*\\u002f
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\u002A\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"\\u002A\\u002F"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\\\u002A\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"\\\\u002A\\u002F"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\u002A\\\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      s"""|${"\\u002A\\\\u002F"}
          |{
          |  new Object() {
          |    void f() { throw new RuntimeException("This exception is injected."); }
          |  }.f();
          |}
          |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)

    literal =
      """|\\\\u002A\\\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    expected =
      """|\\u002A\\u002F
         |{
         |  new Object() {
         |    void f() { throw new RuntimeException("This exception is injected."); }
         |  }.f();
         |}
         |/*""".stripMargin
    checkAnswer(
      sql(s"SELECT '$literal' AS DUMMY"),
      Row(s"$expected") :: Nil)
  }

  test("SPARK-15752 optimize metadata only query for datasource table") {
    withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "true") {
      withTable("srcpart_15752") {
        val data = (1 to 10).map(i => (i, s"data-$i", i % 2, if ((i % 2) == 0) "a" else "b"))
          .toDF("col1", "col2", "partcol1", "partcol2")
        data.write.partitionBy("partcol1", "partcol2").mode("append").saveAsTable("srcpart_15752")
        checkAnswer(
          sql("select partcol1 from srcpart_15752 group by partcol1"),
          Row(0) :: Row(1) :: Nil)
        checkAnswer(
          sql("select partcol1 from srcpart_15752 where partcol1 = 1 group by partcol1"),
          Row(1))
        checkAnswer(
          sql("select partcol1, count(distinct partcol2) from srcpart_15752 group by partcol1"),
          Row(0, 1) :: Row(1, 1) :: Nil)
        checkAnswer(
          sql("select partcol1, count(distinct partcol2) from srcpart_15752  where partcol1 = 1 " +
            "group by partcol1"),
          Row(1, 1) :: Nil)
        checkAnswer(sql("select distinct partcol1 from srcpart_15752"), Row(0) :: Row(1) :: Nil)
        checkAnswer(sql("select distinct partcol1 from srcpart_15752 where partcol1 = 1"), Row(1))
        checkAnswer(
          sql("select distinct col from (select partcol1 + 1 as col from srcpart_15752 " +
            "where partcol1 = 1) t"),
          Row(2))
        checkAnswer(sql("select max(partcol1) from srcpart_15752"), Row(1))
        checkAnswer(sql("select max(partcol1) from srcpart_15752 where partcol1 = 1"), Row(1))
        checkAnswer(sql("select max(partcol1) from (select partcol1 from srcpart_15752) t"), Row(1))
        checkAnswer(
          sql("select max(col) from (select partcol1 + 1 as col from srcpart_15752 " +
            "where partcol1 = 1) t"),
          Row(2))
      }
    }
  }

  test("SPARK-16975: Column-partition path starting '_' should be handled correctly") {
    withTempDir { dir =>
      val dataDir = new File(dir, "data").getCanonicalPath
      spark.range(10).withColumn("_col", $"id").write.partitionBy("_col").save(dataDir)
      spark.read.load(dataDir)
    }
  }

  test("SPARK-16644: Aggregate should not put aggregate expressions to constraints") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b INT) USING parquet")
      checkAnswer(sql(
        """
          |SELECT
          |  a,
          |  MAX(b) AS c1,
          |  b AS c2
          |FROM tbl
          |WHERE a = b
          |GROUP BY a, b
          |HAVING c1 = 1
        """.stripMargin), Nil)
    }
  }

  test("SPARK-16674: field names containing dots for both fields and partitioned fields") {
    withTempPath { path =>
      val data = (1 to 10).map(i => (i, s"data-$i", i % 2, if ((i % 2) == 0) "a" else "b"))
        .toDF("col.1", "col.2", "part.col1", "part.col2")
      data.write
        .format("parquet")
        .partitionBy("part.col1", "part.col2")
        .save(path.getCanonicalPath)
      val readBack = spark.read.format("parquet").load(path.getCanonicalPath)
      checkAnswer(
        readBack.selectExpr("`part.col1`", "`col.1`"),
        data.selectExpr("`part.col1`", "`col.1`"))
    }
  }

  test("SPARK-17515: CollectLimit.execute() should perform per-partition limits") {
    val numRecordsRead = spark.sparkContext.longAccumulator
    spark.range(1, 100, 1, numPartitions = 10).map { x =>
      numRecordsRead.add(1)
      x
    }.limit(1).queryExecution.toRdd.count()
    assert(numRecordsRead.value === 10)
  }

  test("CREATE TABLE USING should not fail if a same-name temp view exists") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        sql("CREATE TABLE same_name(i int) USING json")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        assert(spark.table("default.same_name").collect().isEmpty)
      }
    }
  }

  test("SPARK-18053: ARRAY equality is broken") {
    withTable("array_tbl") {
      spark.range(10).select(array($"id").as("arr")).write.saveAsTable("array_tbl")
      assert(sql("SELECT * FROM array_tbl where arr = ARRAY(1L)").count == 1)
    }
  }

  test("SPARK-19157: should be able to change spark.sql.runSQLOnFiles at runtime") {
    withTempPath { path =>
      Seq(1 -> "a").toDF("i", "j").write.parquet(path.getCanonicalPath)

      val newSession = spark.newSession()
      val originalValue = newSession.sessionState.conf.runSQLonFile

      try {
        newSession.conf.set(SQLConf.RUN_SQL_ON_FILES, false)
        intercept[AnalysisException] {
          newSession.sql(s"SELECT i, j FROM parquet.`${path.getCanonicalPath}`")
        }

        newSession.conf.set(SQLConf.RUN_SQL_ON_FILES, true)
        checkAnswer(
          newSession.sql(s"SELECT i, j FROM parquet.`${path.getCanonicalPath}`"),
          Row(1, "a"))
      } finally {
        newSession.conf.set(SQLConf.RUN_SQL_ON_FILES, originalValue)
      }
    }
  }

  test("should be able to resolve a persistent view") {
    withTable("t1", "t2") {
      withView("v1") {
        sql("CREATE TABLE `t1` USING parquet AS SELECT * FROM VALUES(1, 1) AS t1(a, b)")
        sql("CREATE TABLE `t2` USING parquet AS SELECT * FROM VALUES('a', 2, 1.0) AS t2(d, e, f)")
        sql("CREATE VIEW `v1`(x, y) AS SELECT * FROM t1")
        checkAnswer(spark.table("v1").orderBy("x"), Row(1, 1))

        sql("ALTER VIEW `v1` AS SELECT * FROM t2")
        checkAnswer(spark.table("v1").orderBy("f"), Row("a", 2, 1.0))
      }
    }
  }

  test("SPARK-19059: read file based table whose name starts with underscore") {
    withTable("_tbl") {
      sql("CREATE TABLE `_tbl`(i INT) USING parquet")
      sql("INSERT INTO `_tbl` VALUES (1), (2), (3)")
      checkAnswer( sql("SELECT * FROM `_tbl`"), Row(1) :: Row(2) :: Row(3) :: Nil)
    }
  }

  test("SPARK-19334: check code injection is prevented") {
    // The end of comment (*/) should be escaped.
    val badQuery =
      """|SELECT inline(array(cast(struct(1) AS
         |  struct<`=
         |    new Object() {
         |      {f();}
         |      public void f() {throw new RuntimeException("This exception is injected.");}
         |      public int x;
         |    }.x
         |  `:int>)))""".stripMargin.replaceAll("\n", "")

    checkAnswer(sql(badQuery), Row(1) :: Nil)
  }

  test("SPARK-19650: An action on a Command should not trigger a Spark job") {
    // Create a listener that checks if new jobs have started.
    val jobStarted = new AtomicBoolean(false)
    val listener = new SparkListener {
      override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
        jobStarted.set(true)
      }
    }

    // Make sure no spurious job starts are pending in the listener bus.
    sparkContext.listenerBus.waitUntilEmpty()
    sparkContext.addSparkListener(listener)
    try {
      // Execute the command.
      sql("show databases").head()

      // Make sure we have seen all events triggered by DataFrame.show()
      sparkContext.listenerBus.waitUntilEmpty()
    } finally {
      sparkContext.removeSparkListener(listener)
    }
    assert(!jobStarted.get(), "Command should not trigger a Spark job.")
  }

  test("SPARK-20164: AnalysisException should be tolerant to null query plan") {
    try {
      throw new AnalysisException("", None, None, plan = null)
    } catch {
      case ae: AnalysisException => assert(ae.plan == null && ae.getMessage == ae.getSimpleMessage)
    }
  }

  test("SPARK-12868: Allow adding jars from hdfs ") {
    val jarFromHdfs = "hdfs://doesnotmatter/test.jar"
    val jarFromInvalidFs = "fffs://doesnotmatter/test.jar"

    // if 'hdfs' is not supported, MalformedURLException will be thrown
    new URL(jarFromHdfs)

    intercept[MalformedURLException] {
      new URL(jarFromInvalidFs)
    }
  }

  test("RuntimeReplaceable functions should not take extra parameters") {
    val e = intercept[AnalysisException](sql("SELECT nvl(1, 2, 3)"))
    assert(e.message.contains("Invalid number of arguments"))
  }

  test("SPARK-21228: InSet incorrect handling of structs") {
    withTempView("A") {
      // reduce this from the default of 10 so the repro query text is not too long
      withSQLConf((SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "3")) {
        // a relation that has 1 column of struct type with values (1,1), ..., (9, 9)
        spark.range(1, 10).selectExpr("named_struct('a', id, 'b', id) as a")
          .createOrReplaceTempView("A")
        val df = sql(
          """
            |SELECT * from
            | (SELECT MIN(a) as minA FROM A) AA -- this Aggregate will return UnsafeRows
            | -- the IN will become InSet with a Set of GenericInternalRows
            | -- a GenericInternalRow is never equal to an UnsafeRow so the query would
            | -- returns 0 results, which is incorrect
            | WHERE minA IN (NAMED_STRUCT('a', 1L, 'b', 1L), NAMED_STRUCT('a', 2L, 'b', 2L),
            |   NAMED_STRUCT('a', 3L, 'b', 3L))
          """.stripMargin)
        checkAnswer(df, Row(Row(1, 1)))
      }
    }
  }

  test("SPARK-21247: Allow case-insensitive type equality in Set operation") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      sql("SELECT struct(1 a) UNION ALL (SELECT struct(2 A))")
      sql("SELECT struct(1 a) EXCEPT (SELECT struct(2 A))")

      withTable("t", "S") {
        sql("CREATE TABLE t(c struct<f:int>) USING parquet")
        sql("CREATE TABLE S(C struct<F:int>) USING parquet")
        Seq(("c", "C"), ("C", "c"), ("c.f", "C.F"), ("C.F", "c.f")).foreach {
          case (left, right) =>
            checkAnswer(sql(s"SELECT * FROM t, S WHERE t.$left = S.$right"), Seq.empty)
        }
      }
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      // Union resolves nested columns by position too.
      checkAnswer(sql("SELECT struct(1 a) UNION ALL (SELECT struct(2 A))"),
        Row(Row(1)) :: Row(Row(2)) :: Nil)

      val m2 = intercept[AnalysisException] {
        sql("SELECT struct(1 a) EXCEPT (SELECT struct(2 A))")
      }.message
      assert(m2.contains("Except can only be performed on tables with the compatible column types"))

      withTable("t", "S") {
        sql("CREATE TABLE t(c struct<f:int>) USING parquet")
        sql("CREATE TABLE S(C struct<F:int>) USING parquet")
        checkAnswer(sql("SELECT * FROM t, S WHERE t.c.f = S.C.F"), Seq.empty)
        checkError(
          exception = intercept[AnalysisException] {
            sql("SELECT * FROM t, S WHERE c = C")
          },
          errorClass = "DATATYPE_MISMATCH",
          errorSubClass = Some("BINARY_OP_DIFF_TYPES"),
          parameters = Map(
            "sqlExpr" -> "\"(c = C)\"",
            "left" -> "\"STRUCT<f: INT>\"",
            "right" -> "\"STRUCT<F: INT>\""))
      }
    }
  }

  test("SPARK-21743: top-most limit should not cause memory leak") {
    // In unit test, Spark will fail the query if memory leak detected.
    spark.range(100).groupBy("id").count().limit(1).collect()
  }

  test("SPARK-21652: rule confliction of InferFiltersFromConstraints and ConstantPropagation") {
    withTempView("t1", "t2") {
      Seq((1, 1)).toDF("col1", "col2").createOrReplaceTempView("t1")
      Seq(1, 2).toDF("col").createOrReplaceTempView("t2")
      val df = sql(
        """
          |SELECT *
          |FROM t1, t2
          |WHERE t1.col1 = 1 AND 1 = t1.col2 AND t1.col1 = t2.col AND t1.col2 = t2.col
        """.stripMargin)
      checkAnswer(df, Row(1, 1, 1))
    }
  }

  test("SPARK-23079: constraints should be inferred correctly with aliases") {
    withTable("t") {
      spark.range(5).write.saveAsTable("t")
      val t = spark.read.table("t")
      val left = t.withColumn("xid", $"id" + lit(1)).as("x")
      val right = t.withColumnRenamed("id", "xid").as("y")
      val df = left.join(right, "xid").filter("id = 3").toDF()
      checkAnswer(df, Row(4, 3))
    }
  }

  test("SPARK-22266: the same aggregate function was calculated multiple times") {
    val query = "SELECT a, max(b+1), max(b+1) + 1 FROM testData2 GROUP BY a"
    val df = sql(query)
    val physical = df.queryExecution.sparkPlan
    val aggregateExpressions = physical.collectFirst {
      case agg : HashAggregateExec => agg.aggregateExpressions
      case agg : SortAggregateExec => agg.aggregateExpressions
    }
    assert (aggregateExpressions.isDefined)
    assert (aggregateExpressions.get.size == 1)
    checkAnswer(df, Row(1, 3, 4) :: Row(2, 3, 4) :: Row(3, 3, 4) :: Nil)
  }

  test("Support filter clause for aggregate function with hash aggregate") {
    Seq(("COUNT(a)", 3), ("COLLECT_LIST(a)", Seq(1, 2, 3))).foreach { funcToResult =>
      val query = s"SELECT ${funcToResult._1} FILTER (WHERE b > 1) FROM testData2"
      val df = sql(query)
      val physical = df.queryExecution.sparkPlan
      val aggregateExpressions = physical.collect {
        case agg: HashAggregateExec => agg.aggregateExpressions
        case agg: ObjectHashAggregateExec => agg.aggregateExpressions
      }.flatten
      aggregateExpressions.foreach { expr =>
        if (expr.mode == Complete || expr.mode == Partial) {
          assert(expr.filter.isDefined)
        } else {
          assert(expr.filter.isEmpty)
        }
      }
      checkAnswer(df, Row(funcToResult._2))
    }
  }

  test("Support filter clause for aggregate function uses SortAggregateExec") {
    withSQLConf(SQLConf.USE_OBJECT_HASH_AGG.key -> "false") {
      val df = sql("SELECT PERCENTILE(a, 1) FILTER (WHERE b > 1) FROM testData2")
      val physical = df.queryExecution.sparkPlan
      val aggregateExpressions = physical.collect {
        case agg: SortAggregateExec => agg.aggregateExpressions
      }.flatten
      aggregateExpressions.foreach { expr =>
        if (expr.mode == Complete || expr.mode == Partial) {
          assert(expr.filter.isDefined)
        } else {
          assert(expr.filter.isEmpty)
        }
      }
      checkAnswer(df, Row(3))
    }
  }

  test("Non-deterministic aggregate functions should not be deduplicated") {
    withUserDefinedFunction("sumND" -> true) {
      spark.udf.register("sumND", udaf(new Aggregator[Long, Long, Long] {
        def zero: Long = 0L
        def reduce(b: Long, a: Long): Long = b + a
        def merge(b1: Long, b2: Long): Long = b1 + b2
        def finish(r: Long): Long = r
        def bufferEncoder: Encoder[Long] = Encoders.scalaLong
        def outputEncoder: Encoder[Long] = Encoders.scalaLong
      }).asNondeterministic())

      val query = "SELECT a, sumND(b), sumND(b) + 1 FROM testData2 GROUP BY a"
      val df = sql(query)
      val physical = df.queryExecution.sparkPlan
      val aggregateExpressions = physical.collectFirst {
        case agg: BaseAggregateExec => agg.aggregateExpressions
      }
      assert(aggregateExpressions.isDefined)
      assert(aggregateExpressions.get.size == 2)
    }
  }

  test("SPARK-22356: overlapped columns between data and partition schema in data source tables") {
    withTempPath { path =>
      Seq((1, 1, 1), (1, 2, 1)).toDF("i", "p", "j")
        .write.mode("overwrite").parquet(new File(path, "p=1").getCanonicalPath)
      withTable("t") {
        sql(s"create table t using parquet options(path='${path.getCanonicalPath}')")
        // We should respect the column order in data schema.
        assert(spark.table("t").columns === Array("i", "p", "j"))
        checkAnswer(spark.table("t"), Row(1, 1, 1) :: Row(1, 1, 1) :: Nil)
        // The DESC TABLE should report same schema as table scan.
        assert(sql("desc t").select("col_name")
          .as[String].collect().mkString(",").contains("i,p,j"))
      }
    }
  }

  test("SPARK-24696 ColumnPruning rule fails to remove extra Project") {
    withTable("fact_stats", "dim_stats") {
      val factData = Seq((1, 1, 99, 1), (2, 2, 99, 2), (3, 1, 99, 3), (4, 2, 99, 4))
      val storeData = Seq((1, "BW", "DE"), (2, "AZ", "US"))
      spark.udf.register("filterND", udf((value: Int) => value > 2).asNondeterministic)
      factData.toDF("date_id", "store_id", "product_id", "units_sold")
        .write.mode("overwrite").partitionBy("store_id").format("parquet").saveAsTable("fact_stats")
      storeData.toDF("store_id", "state_province", "country")
        .write.mode("overwrite").format("parquet").saveAsTable("dim_stats")
      val df = sql(
        """
          |SELECT f.date_id, f.product_id, f.store_id FROM
          |(SELECT date_id, product_id, store_id
          |   FROM fact_stats WHERE filterND(date_id)) AS f
          |JOIN dim_stats s
          |ON f.store_id = s.store_id WHERE s.country = 'DE'
        """.stripMargin)
      checkAnswer(df, Seq(Row(3, 99, 1)))
    }
  }


  test("SPARK-24940: coalesce and repartition hint") {
    withTempView("nums1") {
      val numPartitionsSrc = 10
      spark.range(0, 100, 1, numPartitionsSrc).createOrReplaceTempView("nums1")
      assert(spark.table("nums1").rdd.getNumPartitions == numPartitionsSrc)

      withTable("nums") {
        sql("CREATE TABLE nums (id INT) USING parquet")

        Seq(5, 20, 2).foreach { numPartitions =>
          sql(
            s"""
               |INSERT OVERWRITE TABLE nums
               |SELECT /*+ REPARTITION($numPartitions) */ *
               |FROM nums1
             """.stripMargin)
          assert(spark.table("nums").inputFiles.length == numPartitions)

          sql(
            s"""
               |INSERT OVERWRITE TABLE nums
               |SELECT /*+ COALESCE($numPartitions) */ *
               |FROM nums1
             """.stripMargin)
          // Coalesce can not increase the number of partitions
          assert(spark.table("nums").inputFiles.length == Seq(numPartitions, numPartitionsSrc).min)
        }
      }
    }
  }

  test("SPARK-25084: 'distribute by' on multiple columns may lead to codegen issue") {
    withView("spark_25084") {
      val count = 1000
      val df = spark.range(count)
      val columns = (0 until 400).map{ i => s"id as id$i" }
      val distributeExprs = (0 until 100).map(c => s"id$c").mkString(",")
      df.selectExpr(columns : _*).createTempView("spark_25084")
      assert(
        spark.sql(s"select * from spark_25084 distribute by ($distributeExprs)").count === count)
    }
  }

  test("SPARK-25144 'distinct' causes memory leak") {
    val ds = List(Foo(Some("bar"))).toDS
    val result = ds.flatMap(_.bar).distinct
    result.rdd.isEmpty
  }

  test("SPARK-25454: decimal division with negative scale") {
    // TODO: completely fix this issue even when LITERAL_PRECISE_PRECISION is true.
    withSQLConf(SQLConf.LITERAL_PICK_MINIMUM_PRECISION.key -> "false") {
      checkAnswer(sql("select 26393499451 / (1e6 * 1000)"), Row(BigDecimal("26.3934994510000")))
    }
  }

  test("SPARK-25988: self join with aliases on partitioned tables #1") {
    withTempView("tmpView1", "tmpView2") {
      withTable("tab1", "tab2") {
        sql(
          """
            |CREATE TABLE `tab1` (`col1` INT, `TDATE` DATE)
            |USING CSV
            |PARTITIONED BY (TDATE)
          """.stripMargin)
        spark.table("tab1").where("TDATE >= '2017-08-15'").createOrReplaceTempView("tmpView1")
        sql("CREATE TABLE `tab2` (`TDATE` DATE) USING parquet")
        sql(
          """
            |CREATE OR REPLACE TEMPORARY VIEW tmpView2 AS
            |SELECT N.tdate, col1 AS aliasCol1
            |FROM tmpView1 N
            |JOIN tab2 Z
            |ON N.tdate = Z.tdate
          """.stripMargin)
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "0") {
          sql("SELECT * FROM tmpView2 x JOIN tmpView2 y ON x.tdate = y.tdate").collect()
        }
      }
    }
  }

  test("SPARK-25988: self join with aliases on partitioned tables #2") {
    withTempView("tmp") {
      withTable("tab1", "tab2") {
        sql(
          """
            |CREATE TABLE `tab1` (`EX` STRING, `TDATE` DATE)
            |USING parquet
            |PARTITIONED BY (tdate)
          """.stripMargin)
        sql("CREATE TABLE `tab2` (`TDATE` DATE) USING parquet")
        sql(
          """
            |CREATE OR REPLACE TEMPORARY VIEW TMP as
            |SELECT  N.tdate, EX AS new_ex
            |FROM tab1 N
            |JOIN tab2 Z
            |ON N.tdate = Z.tdate
          """.stripMargin)
        sql(
          """
            |SELECT * FROM TMP x JOIN TMP y
            |ON x.tdate = y.tdate
          """.stripMargin).queryExecution.executedPlan
      }
    }
  }

  test("SPARK-26366: verify ReplaceExceptWithFilter") {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.REPLACE_EXCEPT_WITH_FILTER.key -> enabled.toString) {
        val df = spark.createDataFrame(
          sparkContext.parallelize(Seq(Row(0, 3, 5),
            Row(0, 3, null),
            Row(null, 3, 5),
            Row(0, null, 5),
            Row(0, null, null),
            Row(null, null, 5),
            Row(null, 3, null),
            Row(null, null, null))),
          StructType(Seq(StructField("c1", IntegerType),
            StructField("c2", IntegerType),
            StructField("c3", IntegerType))))
        val where = "c2 >= 3 OR c1 >= 0"
        val whereNullSafe =
          """
            |(c2 IS NOT NULL AND c2 >= 3)
            |OR (c1 IS NOT NULL AND c1 >= 0)
          """.stripMargin

        val df_a = df.filter(where)
        val df_b = df.filter(whereNullSafe)
        checkAnswer(df.except(df_a), df.except(df_b))

        val whereWithIn = "c2 >= 3 OR c1 in (2)"
        val whereWithInNullSafe =
          """
            |(c2 IS NOT NULL AND c2 >= 3)
          """.stripMargin
        val dfIn_a = df.filter(whereWithIn)
        val dfIn_b = df.filter(whereWithInNullSafe)
        checkAnswer(df.except(dfIn_a), df.except(dfIn_b))
      }
    }
  }

  test("SPARK-26402: accessing nested fields with different cases in case insensitive mode") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val msg = intercept[AnalysisException] {
        withTable("t") {
          sql("create table t (s struct<i: Int>) using json")
          checkAnswer(sql("select s.I from t group by s.i"), Nil)
        }
      }.message
      assert(msg.contains("No such struct field I in i"))
    }

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      withTable("t") {
        sql("create table t (s struct<i: Int>) using json")
        checkAnswer(sql("select s.I from t group by s.i"), Nil)
      }
    }
  }

  test("SPARK-27699 Validate pushed down filters") {
    def checkPushedFilters(format: String, df: DataFrame, filters: Array[sources.Filter]): Unit = {
      val scan = df.queryExecution.sparkPlan
        .find(_.isInstanceOf[BatchScanExec]).get.asInstanceOf[BatchScanExec]
        .scan
      format match {
        case "orc" =>
          assert(scan.isInstanceOf[OrcScan])
          assert(scan.asInstanceOf[OrcScan].pushedFilters === filters)
        case "parquet" =>
          assert(scan.isInstanceOf[ParquetScan])
          assert(scan.asInstanceOf[ParquetScan].pushedFilters === filters)
        case _ =>
          fail(s"unknown format $format")
      }
    }

    Seq("orc", "parquet").foreach { format =>
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "",
        SQLConf.PARQUET_FILTER_PUSHDOWN_STRING_PREDICATE_ENABLED.key -> "false") {
        withTempPath { dir =>
          spark.range(10).map(i => (i, i.toString)).toDF("id", "s")
            .write
            .format(format)
            .save(dir.getCanonicalPath)
          val df = spark.read.format(format).load(dir.getCanonicalPath)
          checkPushedFilters(
            format,
            df.where(($"id" < 2 and $"s".contains("foo")) or
              ($"id" > 10 and $"s".contains("bar"))),
            Array(sources.Or(sources.LessThan("id", 2), sources.GreaterThan("id", 10))))
          checkPushedFilters(
            format,
            df.where($"s".contains("foo") or
              ($"id" > 10 and $"s".contains("bar"))),
            Array.empty)
          checkPushedFilters(
            format,
            df.where($"id" < 2 and not($"id" > 10 and $"s".contains("bar"))),
            Array(sources.IsNotNull("id"), sources.LessThan("id", 2)))
        }
      }
    }
  }

  test("SPARK-26709: OptimizeMetadataOnlyQuery does not handle empty records correctly") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "parquet") {
      Seq(true, false).foreach { enableOptimizeMetadataOnlyQuery =>
        withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key ->
          enableOptimizeMetadataOnlyQuery.toString) {
          withTable("t") {
            sql("CREATE TABLE t (col1 INT, p1 INT) USING PARQUET PARTITIONED BY (p1)")
            sql("INSERT INTO TABLE t PARTITION (p1 = 5) SELECT ID FROM range(1, 1)")
            if (enableOptimizeMetadataOnlyQuery) {
              // The result is wrong if we enable the configuration.
              checkAnswer(sql("SELECT MAX(p1) FROM t"), Row(5))
            } else {
              checkAnswer(sql("SELECT MAX(p1) FROM t"), Row(null))
            }
            checkAnswer(sql("SELECT MAX(col1) FROM t"), Row(null))
          }

          withTempPath { path =>
            val tabLocation = path.getCanonicalPath
            val partLocation1 = tabLocation + "/p=3"
            val partLocation2 = tabLocation + "/p=1"
            // SPARK-23271 empty RDD when saved should write a metadata only file
            val df = spark.emptyDataFrame.select(lit(1).as("col"))
            df.write.parquet(partLocation1)
            val df2 = spark.range(10).toDF("col")
            df2.write.parquet(partLocation2)
            val readDF = spark.read.parquet(tabLocation)
            if (enableOptimizeMetadataOnlyQuery) {
              // The result is wrong if we enable the configuration.
              checkAnswer(readDF.selectExpr("max(p)"), Row(3))
            } else {
              checkAnswer(readDF.selectExpr("max(p)"), Row(1))
            }
            checkAnswer(readDF.selectExpr("max(col)"), Row(9))
          }
        }
      }
    }
  }

  test("reset command should not fail with cache") {
    withTable("tbl") {
      val provider = spark.sessionState.conf.defaultDataSourceName
      sql(s"CREATE TABLE tbl(i INT, j STRING) USING $provider")
      sql("reset")
      sql("cache table tbl")
      sql("reset")
    }
  }

  test("string date comparison") {
    withTempView("t1") {
      spark.range(1).selectExpr("date '2000-01-01' as d").createOrReplaceTempView("t1")
      val result = Date.valueOf("2000-01-01")
      checkAnswer(sql("select * from t1 where d < '2000'"), Nil)
      checkAnswer(sql("select * from t1 where d < '2001'"), Row(result))
      checkAnswer(sql("select * from t1 where d < '2000-01'"), Nil)
      checkAnswer(sql("select * from t1 where d < '2000-01-01'"), Nil)
      checkAnswer(sql("select * from t1 where d < '2000-1-1'"), Nil)
      checkAnswer(sql("select * from t1 where d <= '2000-1-1'"), Row(result))
      checkAnswer(sql("select * from t1 where d <= '1999-12-30'"), Nil)
      checkAnswer(sql("select * from t1 where d = '2000-1-1'"), Row(result))
      checkAnswer(sql("select * from t1 where d = '2000-01-01'"), Row(result))
      checkAnswer(sql("select * from t1 where d = '2000-1-02'"), Nil)
      checkAnswer(sql("select * from t1 where d > '2000-01-01'"), Nil)
      checkAnswer(sql("select * from t1 where d > '1999'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000-1'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000-1-1'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000-1-01'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000-01-1'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000-01-01'"), Row(result))
      checkAnswer(sql("select * from t1 where d >= '2000-01-02'"), Nil)
      checkAnswer(sql("select * from t1 where '2000' >= d"), Row(result))
      if (!conf.ansiEnabled) {
        checkAnswer(sql("select * from t1 where d > '2000-13'"), Nil)
      }

      withSQLConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING.key -> "true") {
        checkAnswer(sql("select * from t1 where d < '2000'"), Nil)
        checkAnswer(sql("select * from t1 where d < '2001'"), Row(result))
        checkAnswer(sql("select * from t1 where d <= '1999'"), Nil)
        checkAnswer(sql("select * from t1 where d >= '2000'"), Row(result))
        if (!conf.ansiEnabled) {
          checkAnswer(sql("select * from t1 where d < '2000-1-1'"), Row(result))
          checkAnswer(sql("select * from t1 where d > '1999-13'"), Row(result))
          checkAnswer(sql("select to_date('2000-01-01') > '1'"), Row(true))
        }
      }
    }
  }

  test("string timestamp comparison") {
    spark.range(1)
      .selectExpr("timestamp '2000-01-01 01:10:00.000' as d")
      .createOrReplaceTempView("t1")
    val result = Timestamp.valueOf("2000-01-01 01:10:00")
    checkAnswer(sql("select * from t1 where d < '2000'"), Nil)
    checkAnswer(sql("select * from t1 where d < '2001'"), Row(result))
    checkAnswer(sql("select * from t1 where d < '2000-01'"), Nil)
    checkAnswer(sql("select * from t1 where d < '2000-1-1'"), Nil)
    checkAnswer(sql("select * from t1 where d < '2000-01-01 01:10:00.000'"), Nil)
    checkAnswer(sql("select * from t1 where d < '2000-01-01 02:10:00.000'"), Row(result))
    checkAnswer(sql("select * from t1 where d <= '2000-1-1 01:10:00'"), Row(result))
    checkAnswer(sql("select * from t1 where d <= '2000-1-1 01:00:00'"), Nil)
    checkAnswer(sql("select * from t1 where d = '2000-1-1 01:10:00.000'"), Row(result))
    checkAnswer(sql("select * from t1 where d = '2000-01-01 01:10:00.000'"), Row(result))
    checkAnswer(sql("select * from t1 where d = '2000-1-02 01:10:00.000'"), Nil)
    checkAnswer(sql("select * from t1 where d > '2000'"), Row(result))
    checkAnswer(sql("select * from t1 where d > '2000-1'"), Row(result))
    checkAnswer(sql("select * from t1 where d > '2000-1-1'"), Row(result))
    checkAnswer(sql("select * from t1 where d > '2000-1-1 01:00:00.000'"), Row(result))
    checkAnswer(sql("select * from t1 where d > '2001'"), Nil)
    checkAnswer(sql("select * from t1 where d > '2000-01-02'"), Nil)
    checkAnswer(sql("select * from t1 where d >= '2000-1-01'"), Row(result))
    checkAnswer(sql("select * from t1 where d >= '2000-01-1'"), Row(result))
    checkAnswer(sql("select * from t1 where d >= '2000-01-01'"), Row(result))
    checkAnswer(sql("select * from t1 where d >= '2000-01-01 01:10:00.000'"), Row(result))
    checkAnswer(sql("select * from t1 where d >= '2000-01-02 01:10:00.000'"), Nil)
    checkAnswer(sql("select * from t1 where '2000' >= d"), Nil)
    if (!conf.ansiEnabled) {
      checkAnswer(sql("select * from t1 where d > '2000-13'"), Nil)
    }

    withSQLConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING.key -> "true") {
      checkAnswer(sql("select * from t1 where d < '2000'"), Nil)
      checkAnswer(sql("select * from t1 where d < '2001'"), Row(result))
      checkAnswer(sql("select * from t1 where d <= '2000-01-02'"), Row(result))
      checkAnswer(sql("select * from t1 where d <= '1999'"), Nil)
      checkAnswer(sql("select * from t1 where d >= '2000'"), Row(result))
      if (!conf.ansiEnabled) {
        checkAnswer(sql("select * from t1 where d <= '2000-1-1'"), Row(result))
        checkAnswer(sql("select * from t1 where d > '1999-13'"), Row(result))
        checkAnswer(sql("select to_timestamp('2000-01-01 01:10:00') > '1'"), Row(true))
      }
    }
    sql("DROP VIEW t1")
  }

  test("SPARK-28156: self-join should not miss cached view") {
    withTable("table1") {
      withView("table1_vw") {
        withTempView("cachedview") {
          val df = Seq.tabulate(5) { x => (x, x + 1, x + 2, x + 3) }.toDF("a", "b", "c", "d")
          df.write.mode("overwrite").format("orc").saveAsTable("table1")
          sql("drop view if exists table1_vw")
          sql("create view table1_vw as select * from table1")

          val cachedView = sql("select a, b, c, d from table1_vw")

          cachedView.createOrReplaceTempView("cachedview")
          cachedView.persist()

          val queryDf = sql(
            s"""select leftside.a, leftside.b
               |from cachedview leftside
               |join cachedview rightside
               |on leftside.a = rightside.a
           """.stripMargin)

          val inMemoryTableScan = collect(queryDf.queryExecution.executedPlan) {
            case i: InMemoryTableScanExec => i
          }
          assert(inMemoryTableScan.size == 2)
          checkAnswer(queryDf, Row(0, 1) :: Row(1, 2) :: Row(2, 3) :: Row(3, 4) :: Row(4, 5) :: Nil)
        }
      }
    }

  }

  test("SPARK-29000: arithmetic computation overflow when don't allow decimal precision loss ") {
    withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> "false") {
      val df1 = sql("select case when 1=2 then 1 else 100.000000000000000000000000 end * 1")
      checkAnswer(df1, Array(Row(100)))
      val df2 = sql("select case when 1=2 then 1 else 100.000000000000000000000000 end * " +
        "case when 1=2 then 2 else 1 end")
      checkAnswer(df2, Array(Row(100)))
      val df3 = sql("select case when 1=2 then 1 else 1.000000000000000000000001 end / 10")
      checkAnswer(df3, Array(Row(new java.math.BigDecimal("0.100000000000000000000000100"))))
    }
  }

  test("SPARK-29239: Subquery should not cause NPE when eliminating subexpression") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
        SQLConf.SUBQUERY_REUSE_ENABLED.key -> "false",
        SQLConf.CODEGEN_FACTORY_MODE.key -> "CODEGEN_ONLY",
        SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> ConvertToLocalRelation.ruleName) {
      withTempView("t1", "t2") {
        sql("create temporary view t1 as select * from values ('val1a', 10L) as t1(t1a, t1b)")
        sql("create temporary view t2 as select * from values ('val3a', 110L) as t2(t2a, t2b)")
        val df = sql("SELECT min, min from (SELECT (SELECT min(t2b) FROM t2) min " +
          "FROM t1 WHERE t1a = 'val1c')")
        assert(df.collect().size == 0)
      }
    }
  }

  test("SPARK-29213: FilterExec should not throw NPE") {
    // Under ANSI mode, casting string '' as numeric will cause runtime error
    if (!conf.ansiEnabled) {
      withTempView("t1", "t2", "t3") {
        sql("SELECT ''").as[String].map(identity).toDF("x").createOrReplaceTempView("t1")
        sql("SELECT * FROM VALUES 0, CAST(NULL AS BIGINT)")
          .as[java.lang.Long]
          .map(identity)
          .toDF("x")
          .createOrReplaceTempView("t2")
        sql("SELECT ''").as[String].map(identity).toDF("x").createOrReplaceTempView("t3")
        sql(
          """
            |SELECT t1.x
            |FROM t1
            |LEFT JOIN (
            |    SELECT x FROM (
            |        SELECT x FROM t2
            |        UNION ALL
            |        SELECT SUBSTR(x,5) x FROM t3
            |    ) a
            |    WHERE LENGTH(x)>0
            |) t3
            |ON t1.x=t3.x
        """.stripMargin).collect()
      }
    }
  }

  test("SPARK-29682: Conflicting attributes in Expand are resolved") {
    val numsDF = Seq(1, 2, 3).toDF("nums")
    val cubeDF = numsDF.cube("nums").agg(max(lit(0)).as("agcol"))

    checkAnswer(
      cubeDF.join(cubeDF, "nums"),
      Row(1, 0, 0) :: Row(2, 0, 0) :: Row(3, 0, 0) :: Nil)
  }

  test("SPARK-29860: Fix dataType mismatch issue for InSubquery") {
    withTempView("ta", "tb", "tc", "td", "te", "tf") {
      sql("CREATE TEMPORARY VIEW ta AS SELECT * FROM VALUES(CAST(1 AS DECIMAL(8, 0))) AS ta(id)")
      sql("CREATE TEMPORARY VIEW tb AS SELECT * FROM VALUES(CAST(1 AS DECIMAL(7, 2))) AS tb(id)")
      sql("CREATE TEMPORARY VIEW tc AS SELECT * FROM VALUES(CAST(1 AS DOUBLE)) AS tc(id)")
      sql("CREATE TEMPORARY VIEW td AS SELECT * FROM VALUES(CAST(1 AS FLOAT)) AS td(id)")
      sql("CREATE TEMPORARY VIEW te AS SELECT * FROM VALUES(CAST(1 AS BIGINT)) AS te(id)")
      val df1 = sql("SELECT id FROM ta WHERE id IN (SELECT id FROM tb)")
      checkAnswer(df1, Row(new java.math.BigDecimal(1)))
      val df2 = sql("SELECT id FROM ta WHERE id IN (SELECT id FROM tc)")
      checkAnswer(df2, Row(new java.math.BigDecimal(1)))
      val df3 = sql("SELECT id FROM ta WHERE id IN (SELECT id FROM td)")
      checkAnswer(df3, Row(new java.math.BigDecimal(1)))
      val df4 = sql("SELECT id FROM ta WHERE id IN (SELECT id FROM te)")
      checkAnswer(df4, Row(new java.math.BigDecimal(1)))
      if (!conf.ansiEnabled) {
        sql(
          "CREATE TEMPORARY VIEW tf AS SELECT * FROM VALUES(CAST(1 AS DECIMAL(38, 38))) AS tf(id)")
        val df5 = sql("SELECT id FROM ta WHERE id IN (SELECT id FROM tf)")
        checkAnswer(df5, Array.empty[Row])
      }
    }
  }

  test("SPARK-30447: fix constant propagation inside NOT") {
    withTempView("t") {
      Seq[Integer](1, null).toDF("c").createOrReplaceTempView("t")
      val df = sql("SELECT * FROM t WHERE NOT(c = 1 AND c + 1 = 1)")

      checkAnswer(df, Row(1))
    }
  }

  test("SPARK-26218: Fix the corner case when casting float to Integer") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      intercept[ArithmeticException](
        sql("SELECT CAST(CAST(2147483648 as FLOAT) as Integer)").collect()
      )
      intercept[ArithmeticException](
        sql("SELECT CAST(CAST(2147483648 as DOUBLE) as Integer)").collect()
      )
    }
  }

  test("SPARK-30870: Column pruning shouldn't alias a nested column for the whole structure") {
    withTable("t") {
      val df = sql(
        """
          |SELECT value
          |FROM VALUES array(named_struct('field', named_struct('a', 1, 'b', 2))) AS (value)
        """.stripMargin)
      df.write.format("parquet").saveAsTable("t")

      val df2 = spark.table("t")
        .limit(100)
        .select(size(col("value.field")))
      val projects = df2.queryExecution.optimizedPlan.collect {
        case p: Project => p
      }
      assert(projects.length == 1)
      val aliases = NestedColumnAliasingSuite.collectGeneratedAliases(projects(0))
      assert(aliases.length == 0)
    }
  }

  test("SPARK-30955: Exclude Generate output when aliasing in nested column pruning") {
    val df1 = sql(
      """
        |SELECT explodedvalue.*
        |FROM VALUES array(named_struct('nested', named_struct('a', 1, 'b', 2))) AS (value)
        |LATERAL VIEW explode(value) AS explodedvalue
      """.stripMargin)
    checkAnswer(df1, Row(Row(1, 2)) :: Nil)

    val df2 = sql(
      """
        |SELECT explodedvalue.nested.a
        |FROM VALUES array(named_struct('nested', named_struct('a', 1, 'b', 2))) AS (value)
        |LATERAL VIEW explode(value) AS explodedvalue
      """.stripMargin)
    checkAnswer(df2, Row(1) :: Nil)
  }

  test("SPARK-30279 Support 32 or more grouping attributes for GROUPING_ID()") {
    withTempView("t") {
      sql("CREATE TEMPORARY VIEW t AS SELECT * FROM " +
        s"VALUES(${(0 until 65).map { _ => 1 }.mkString(", ")}, 3) AS " +
        s"t(${(0 until 65).map { i => s"k$i" }.mkString(", ")}, v)")

      def testGroupingIDs(numGroupingSet: Int, expectedIds: Seq[Any] = Nil): Unit = {
        val groupingCols = (0 until numGroupingSet).map { i => s"k$i" }
        val df = sql("SELECT GROUPING_ID(), SUM(v) FROM t GROUP BY " +
          s"GROUPING SETS ((${groupingCols.mkString(",")}), (${groupingCols.init.mkString(",")}))")
        checkAnswer(df, expectedIds.map { id => Row(id, 3) })
      }

      withSQLConf(SQLConf.LEGACY_INTEGER_GROUPING_ID.key -> "true") {
        testGroupingIDs(32, Seq(0, 1))
      }

      withSQLConf(SQLConf.LEGACY_INTEGER_GROUPING_ID.key -> "false") {
        testGroupingIDs(64, Seq(0L, 1L))
      }
    }
  }

  test("SPARK-36339: References to grouping attributes should be replaced") {
    withTempView("t") {
      Seq("a", "a", "b").toDF("x").createOrReplaceTempView("t")
      checkAnswer(
        sql(
          """
            |select count(x) c, x from t
            |group by x grouping sets(x)
          """.stripMargin),
        Seq(Row(2, "a"), Row(1, "b")))
    }
  }

  test("SPARK-31166: UNION map<null, null> and other maps should not fail") {
    checkAnswer(
      sql("(SELECT map()) UNION ALL (SELECT map(1, 2))"),
      Seq(Row(Map[Int, Int]()), Row(Map(1 -> 2))))
  }

  test("SPARK-31242: clone SparkSession should respect sessionInitWithConfigDefaults") {
    // Note, only the conf explicitly set in SparkConf(e.g. in SharedSparkSessionBase) would cause
    // problem before the fix.
    withSQLConf(SQLConf.CODEGEN_FALLBACK.key -> "true") {
      val cloned = spark.cloneSession()
      SparkSession.setActiveSession(cloned)
      assert(SQLConf.get.getConf(SQLConf.CODEGEN_FALLBACK) === true)
    }
  }

  test("SPARK-31594: Do not display the seed of rand/randn with no argument in output schema") {
    def checkIfSeedExistsInExplain(df: DataFrame): Unit = {
      val output = new java.io.ByteArrayOutputStream()
      Console.withOut(output) {
        df.explain()
      }
      val projectExplainOutput = output.toString.split("\n").find(_.contains("Project")).get
      assert(projectExplainOutput.matches(""".*randn?\(-?[0-9]+\).*"""))
    }
    val df1 = sql("SELECT rand()")
    assert(df1.schema.head.name === "rand()")
    checkIfSeedExistsInExplain(df1)
    val df2 = sql("SELECT rand(1L)")
    assert(df2.schema.head.name === "rand(1)")
    checkIfSeedExistsInExplain(df2)
    val df3 = sql("SELECT randn()")
    assert(df3.schema.head.name === "randn()")
    checkIfSeedExistsInExplain(df1)
    val df4 = sql("SELECT randn(1L)")
    assert(df4.schema.head.name === "randn(1)")
    checkIfSeedExistsInExplain(df2)
  }

  test("SPARK-31670: Trim unnecessary Struct field alias in Aggregate/GroupingSets") {
    withTempView("t") {
      sql(
        """
          |CREATE TEMPORARY VIEW t(a, b, c) AS
          |SELECT * FROM VALUES
          |('A', 1, NAMED_STRUCT('row_id', 1, 'json_string', '{"i": 1}')),
          |('A', 2, NAMED_STRUCT('row_id', 2, 'json_string', '{"i": 1}')),
          |('A', 2, NAMED_STRUCT('row_id', 2, 'json_string', '{"i": 2}')),
          |('B', 1, NAMED_STRUCT('row_id', 3, 'json_string', '{"i": 1}')),
          |('C', 3, NAMED_STRUCT('row_id', 4, 'json_string', '{"i": 1}'))
        """.stripMargin)

      checkAnswer(
        sql(
          """
            |SELECT a, c.json_string, SUM(b)
            |FROM t
            |GROUP BY a, c.json_string
            |""".stripMargin),
        Row("A", "{\"i\": 1}", 3) :: Row("A", "{\"i\": 2}", 2) ::
          Row("B", "{\"i\": 1}", 1) :: Row("C", "{\"i\": 1}", 3) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, c.json_string, SUM(b)
            |FROM t
            |GROUP BY a, c.json_string
            |WITH CUBE
            |""".stripMargin),
        Row("A", "{\"i\": 1}", 3) :: Row("A", "{\"i\": 2}", 2) :: Row("A", null, 5) ::
          Row("B", "{\"i\": 1}", 1) :: Row("B", null, 1) ::
          Row("C", "{\"i\": 1}", 3) :: Row("C", null, 3) ::
          Row(null, "{\"i\": 1}", 7) :: Row(null, "{\"i\": 2}", 2) :: Row(null, null, 9) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, get_json_object(c.json_string, '$.i'), SUM(b)
            |FROM t
            |GROUP BY a, get_json_object(c.json_string, '$.i')
            |WITH CUBE
            |""".stripMargin),
        Row("A", "1", 3) :: Row("A", "2", 2) :: Row("A", null, 5) ::
          Row("B", "1", 1) :: Row("B", null, 1) ::
          Row("C", "1", 3) :: Row("C", null, 3) ::
          Row(null, "1", 7) :: Row(null, "2", 2) :: Row(null, null, 9) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, c.json_string AS json_string, SUM(b)
            |FROM t
            |GROUP BY a, c.json_string
            |WITH CUBE
            |""".stripMargin),
        Row("A", null, 5) :: Row("A", "{\"i\": 1}", 3) :: Row("A", "{\"i\": 2}", 2) ::
          Row("B", null, 1) :: Row("B", "{\"i\": 1}", 1) ::
          Row("C", null, 3) :: Row("C", "{\"i\": 1}", 3) ::
          Row(null, null, 9) :: Row(null, "{\"i\": 1}", 7) :: Row(null, "{\"i\": 2}", 2) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, c.json_string as js, SUM(b)
            |FROM t
            |GROUP BY a, c.json_string
            |WITH CUBE
            |""".stripMargin),
        Row("A", null, 5) :: Row("A", "{\"i\": 1}", 3) :: Row("A", "{\"i\": 2}", 2) ::
          Row("B", null, 1) :: Row("B", "{\"i\": 1}", 1) ::
          Row("C", null, 3) :: Row("C", "{\"i\": 1}", 3) ::
          Row(null, null, 9) :: Row(null, "{\"i\": 1}", 7) :: Row(null, "{\"i\": 2}", 2) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, c.json_string as js, SUM(b)
            |FROM t
            |GROUP BY a, c.json_string
            |WITH ROLLUP
            |""".stripMargin),
        Row("A", null, 5) :: Row("A", "{\"i\": 1}", 3) :: Row("A", "{\"i\": 2}", 2) ::
          Row("B", null, 1) :: Row("B", "{\"i\": 1}", 1) ::
          Row("C", null, 3) :: Row("C", "{\"i\": 1}", 3) ::
          Row(null, null, 9) :: Nil)

      checkAnswer(
        sql(
          """
            |SELECT a, c.json_string, SUM(b)
            |FROM t
            |GROUP BY a, c.json_string
            |GROUPING sets((a),(a, c.json_string))
            |""".stripMargin),
        Row("A", null, 5) :: Row("A", "{\"i\": 1}", 3) :: Row("A", "{\"i\": 2}", 2) ::
          Row("B", null, 1) :: Row("B", "{\"i\": 1}", 1) ::
          Row("C", null, 3) :: Row("C", "{\"i\": 1}", 3) :: Nil)
    }
  }

  test("SPARK-31761: test byte, short, integer overflow for (Divide) integral type") {
    checkAnswer(sql("Select -2147483648 DIV -1"), Seq(Row(Integer.MIN_VALUE.toLong * -1)))
    checkAnswer(sql("select CAST(-128 as Byte) DIV CAST (-1 as Byte)"),
      Seq(Row(Byte.MinValue.toLong * -1)))
    checkAnswer(sql("select CAST(-32768 as short) DIV CAST (-1 as short)"),
      Seq(Row(Short.MinValue.toLong * -1)))
  }

  test("normalize special floating numbers in subquery") {
    withTempView("v1", "v2", "v3") {
      Seq(-0.0).toDF("d").createTempView("v1")
      Seq(0.0).toDF("d").createTempView("v2")
      spark.range(2).createTempView("v3")

      // non-correlated subquery
      checkAnswer(sql("SELECT (SELECT v1.d FROM v1 JOIN v2 ON v1.d = v2.d)"), Row(-0.0))
      // correlated subquery
      checkAnswer(
        sql(
          """
            |SELECT id FROM v3 WHERE EXISTS
            |  (SELECT v1.d FROM v1 JOIN v2 ON v1.d = v2.d WHERE id > 0)
            |""".stripMargin), Row(1))
    }
  }

  test("SPARK-31875: remove hints from plan when spark.sql.optimizer.disableHints = true") {
    withSQLConf(SQLConf.DISABLE_HINTS.key -> "true") {
      withTempView("t1", "t2") {
        Seq[Integer](1, 2).toDF("c1").createOrReplaceTempView("t1")
        Seq[Integer](1, 2).toDF("c1").createOrReplaceTempView("t2")
        val repartitionHints = Seq(
          "COALESCE(2)",
          "REPARTITION(c1)",
          "REPARTITION(c1, 2)",
          "REPARTITION_BY_RANGE(c1, 2)",
          "REPARTITION_BY_RANGE(c1)"
        )
        val joinHints = Seq(
          "BROADCASTJOIN (t1)",
          "MAPJOIN(t1)",
          "SHUFFLE_MERGE(t1)",
          "MERGEJOIN(t1)",
          "SHUFFLE_REPLICATE_NL(t1)"
        )

        repartitionHints.foreach { hintName =>
          val sqlText = s"SELECT /*+ $hintName */ * FROM t1"
          val sqlTextWithoutHint = "SELECT * FROM t1"
          val expectedPlan = sql(sqlTextWithoutHint)
          val actualPlan = sql(sqlText)
          comparePlans(actualPlan.queryExecution.analyzed, expectedPlan.queryExecution.analyzed)
        }

        joinHints.foreach { hintName =>
          val sqlText = s"SELECT /*+ $hintName */ * FROM t1 INNER JOIN t2 ON t1.c1 = t2.c1"
          val sqlTextWithoutHint = "SELECT * FROM t1 INNER JOIN t2 ON t1.c1 = t2.c1"
          val expectedPlan = sql(sqlTextWithoutHint)
          val actualPlan = sql(sqlText)
          comparePlans(actualPlan.queryExecution.analyzed, expectedPlan.queryExecution.analyzed)
        }
      }
    }
  }

  test("SPARK-32372: ResolveReferences.dedupRight should only rewrite attributes for ancestor " +
    "plans of the conflict plan") {
    withTempView("person_a", "person_b", "person_c") {
      sql("SELECT name, avg(age) as avg_age FROM person GROUP BY name")
        .createOrReplaceTempView("person_a")
      sql("SELECT p1.name, p2.avg_age FROM person p1 JOIN person_a p2 ON p1.name = p2.name")
        .createOrReplaceTempView("person_b")
      sql("SELECT * FROM person_a UNION SELECT * FROM person_b")
        .createOrReplaceTempView("person_c")
      checkAnswer(
        sql("SELECT p1.name, p2.avg_age FROM person_c p1 JOIN person_c p2 ON p1.name = p2.name"),
        Row("jim", 20.0) :: Row("mike", 30.0) :: Nil)
    }
  }

  test("SPARK-32280: Avoid duplicate rewrite attributes when there're multiple JOINs") {
    withTempView("A", "B", "C") {
      sql("SELECT 1 AS id").createOrReplaceTempView("A")
      sql("SELECT id, 'foo' AS kind FROM A").createOrReplaceTempView("B")
      sql("SELECT l.id as id FROM B AS l LEFT SEMI JOIN B AS r ON l.kind = r.kind")
        .createOrReplaceTempView("C")
      checkAnswer(sql("SELECT 0 FROM ( SELECT * FROM B JOIN C USING (id)) " +
        "JOIN ( SELECT * FROM B JOIN C USING (id)) USING (id)"), Row(0))
    }
  }

  test("SPARK-32788: non-partitioned table scan should not have partition filter") {
    withTable("t") {
      spark.range(1).write.saveAsTable("t")
      checkAnswer(sql("SELECT id FROM t WHERE (SELECT true)"), Row(0L))
    }
  }

  test("SPARK-33306: Timezone is needed when cast Date to String") {
    withTempView("t1", "t2") {
      spark.sql("select to_date(concat('2000-01-0', id)) as d from range(1, 2)")
        .createOrReplaceTempView("t1")
      spark.sql("select concat('2000-01-0', id) as d from range(1, 2)")
        .createOrReplaceTempView("t2")
      val result = Date.valueOf("2000-01-01")

      checkAnswer(sql("select t1.d from t1 join t2 on t1.d = t2.d"), Row(result))
      withSQLConf(SQLConf.LEGACY_CAST_DATETIME_TO_STRING.key -> "true") {
        checkAnswer(sql("select t1.d from t1 join t2 on t1.d = t2.d"), Row(result))
      }
    }
  }

  test("SPARK-33338: GROUP BY using literal map should not fail") {
    withTable("t") {
      withTempDir { dir =>
        sql(s"CREATE TABLE t USING ORC LOCATION '${dir.toURI}' AS SELECT map('k1', 'v1') m, 'k1' k")
        Seq(
          "SELECT map('k1', 'v1')[k] FROM t GROUP BY 1",
          "SELECT map('k1', 'v1')[k] FROM t GROUP BY map('k1', 'v1')[k]",
          "SELECT map('k1', 'v1')[k] a FROM t GROUP BY a").foreach { statement =>
          checkAnswer(sql(statement), Row("v1"))
        }
      }
    }
  }

  test("SPARK-33084: Add jar support Ivy URI in SQL") {
    val sc = spark.sparkContext
    val hiveVersion = "2.3.9"
    // transitive=false, only download specified jar
    sql(s"ADD JAR ivy://org.apache.hive.hcatalog:hive-hcatalog-core:$hiveVersion?transitive=false")
    assert(sc.listJars()
      .exists(_.contains(s"org.apache.hive.hcatalog_hive-hcatalog-core-$hiveVersion.jar")))

    // default transitive=true, test download ivy URL jar return multiple jars
    sql("ADD JAR ivy://org.scala-js:scalajs-test-interface_2.12:1.2.0")
    assert(sc.listJars().exists(_.contains("scalajs-library_2.12")))
    assert(sc.listJars().exists(_.contains("scalajs-test-interface_2.12")))

    sql(s"ADD JAR ivy://org.apache.hive:hive-contrib:$hiveVersion" +
      "?exclude=org.pentaho:pentaho-aggdesigner-algorithm&transitive=true")
    assert(sc.listJars().exists(_.contains(s"org.apache.hive_hive-contrib-$hiveVersion.jar")))
    assert(sc.listJars().exists(_.contains(s"org.apache.hive_hive-exec-$hiveVersion.jar")))
    assert(!sc.listJars().exists(_.contains("org.pentaho.pentaho_aggdesigner-algorithm")))
  }

  test("SPARK-33677: LikeSimplification should be skipped if pattern contains any escapeChar") {
    withTempView("df") {
      Seq("m@ca").toDF("s").createOrReplaceTempView("df")

      val e = intercept[AnalysisException] {
        sql("SELECT s LIKE 'm%@ca' ESCAPE '%' FROM df").collect()
      }
      assert(e.message.contains("the pattern 'm%@ca' is invalid, " +
        "the escape character is not allowed to precede '@'"))

      checkAnswer(sql("SELECT s LIKE 'm@@ca' ESCAPE '@' FROM df"), Row(true))
    }
  }

  test("limit partition num to 1 when distributing by foldable expressions") {
    withSQLConf((SQLConf.SHUFFLE_PARTITIONS.key, "5")) {
      Seq(1, "1, 2", null, "version()").foreach { expr =>
        val plan = sql(s"select * from values (1), (2), (3) t(a) distribute by $expr")
          .queryExecution.optimizedPlan
        val res = plan.collect {
          case r: RepartitionByExpression if r.numPartitions == 1 => true
        }
        assert(res.nonEmpty)
      }
    }
  }

  test("Fold RepartitionExpression num partition should check if partition expression is empty") {
    withSQLConf((SQLConf.SHUFFLE_PARTITIONS.key, "5")) {
      val df = spark.range(1).hint("REPARTITION_BY_RANGE")
      val plan = df.queryExecution.optimizedPlan
      val res = plan.collect {
        case r: RepartitionByExpression if r.numPartitions == 5 => true
      }
      assert(res.nonEmpty)
    }
  }

  test("SPARK-34030: Fold RepartitionExpression num partition should at Optimizer") {
    withSQLConf((SQLConf.SHUFFLE_PARTITIONS.key, "2")) {
      Seq(1, "1, 2", null, "version()").foreach { expr =>
        val plan = sql(s"select * from values (1), (2), (3) t(a) distribute by $expr")
          .queryExecution.analyzed
        val res = plan.collect {
          case r: RepartitionByExpression if r.numPartitions == 2 => true
        }
        assert(res.nonEmpty)
      }
    }
  }

  test("SPARK-33591: null as string partition literal value 'null' after setting legacy conf") {
    withSQLConf(SQLConf.LEGACY_PARSE_NULL_PARTITION_SPEC_AS_STRING_LITERAL.key -> "true") {
      val t = "tbl"
      withTable("tbl") {
        sql(s"CREATE TABLE $t (col1 INT, p1 STRING) USING PARQUET PARTITIONED BY (p1)")
        sql(s"INSERT INTO TABLE $t PARTITION (p1 = null) SELECT 0")
        checkAnswer(spark.sql(s"SELECT * FROM $t"), Row(0, "null"))
      }
    }
  }

  test("SPARK-33593: Vector reader got incorrect data with binary partition value") {
    Seq("false", "true").foreach(value => {
      withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> value) {
        withTable("t1") {
          sql(
            """CREATE TABLE t1(name STRING, id BINARY, part BINARY)
              |USING PARQUET PARTITIONED BY (part)""".stripMargin)
          sql("INSERT INTO t1 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
          checkAnswer(sql("SELECT name, cast(id as string), cast(part as string) FROM t1"),
            Row("a", "Spark SQL", "Spark SQL"))
        }
      }

      withSQLConf(SQLConf.ORC_VECTORIZED_READER_ENABLED.key -> value) {
        withTable("t2") {
          sql(
            """CREATE TABLE t2(name STRING, id BINARY, part BINARY)
              |USING ORC PARTITIONED BY (part)""".stripMargin)
          sql("INSERT INTO t2 PARTITION(part = 'Spark SQL') VALUES('a', X'537061726B2053514C')")
          checkAnswer(sql("SELECT name, cast(id as string), cast(part as string) FROM t2"),
            Row("a", "Spark SQL", "Spark SQL"))
        }
      }
    })
  }

  test("SPARK-33084: Add jar support Ivy URI in SQL -- jar contains udf class") {
    val sumFuncClass = "org.apache.spark.examples.sql.Spark33084"
    val functionName = "test_udf"
    withTempDir { dir =>
      System.setProperty("ivy.home", dir.getAbsolutePath)
      val sourceJar = new File(Thread.currentThread().getContextClassLoader
        .getResource("SPARK-33084.jar").getFile)
      val targetCacheJarDir = new File(dir.getAbsolutePath +
        "/local/org.apache.spark/SPARK-33084/1.0/jars/")
      targetCacheJarDir.mkdir()
      // copy jar to local cache
      FileUtils.copyFileToDirectory(sourceJar, targetCacheJarDir)
      withTempView("v1") {
        withUserDefinedFunction(
          s"default.$functionName" -> false,
          functionName -> true) {
          // create temporary function without class
          val e = intercept[AnalysisException] {
            sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$sumFuncClass'")
          }.getMessage
          assert(e.contains("Can not load class 'org.apache.spark.examples.sql.Spark33084"))
          sql("ADD JAR ivy://org.apache.spark:SPARK-33084:1.0")
          sql(s"CREATE TEMPORARY FUNCTION $functionName AS '$sumFuncClass'")
          // create a view using a function in 'default' database
          sql(s"CREATE TEMPORARY VIEW v1 AS SELECT $functionName(col1) FROM VALUES (1), (2), (3)")
          // view v1 should still using function defined in `default` database
          checkAnswer(sql("SELECT * FROM v1"), Seq(Row(2.0)))
        }
      }
    }
  }

  test("SPARK-33964: Combine distinct unions that have noop project between them") {
    val df = sql("""
      |SELECT a, b FROM (
      |  SELECT a, b FROM testData2
      |  UNION
      |  SELECT a, sum(b) FROM testData2 GROUP BY a
      |  UNION
      |  SELECT null AS a, sum(b) FROM testData2
      |)""".stripMargin)

    val unions = df.queryExecution.sparkPlan.collect {
      case u: UnionExec => u
    }

    assert(unions.size == 1)
  }

  test("SPARK-34421: Resolve temporary objects in temporary views with CTEs") {
    val tempFuncName = "temp_func"
    withUserDefinedFunction(tempFuncName -> true) {
      spark.udf.register(tempFuncName, identity[Int](_))

      val tempViewName = "temp_view"
      withTempView(tempViewName) {
        sql(s"CREATE TEMPORARY VIEW $tempViewName AS SELECT 1")

        val testViewName = "test_view"

        withTempView(testViewName) {
          sql(
            s"""
              |CREATE TEMPORARY VIEW $testViewName AS
              |WITH cte AS (
              |  SELECT $tempFuncName(0)
              |)
              |SELECT * FROM cte
              |""".stripMargin)
          checkAnswer(sql(s"SELECT * FROM $testViewName"), Row(0))
        }

        withTempView(testViewName) {
          sql(
            s"""
              |CREATE TEMPORARY VIEW $testViewName AS
              |WITH cte AS (
              |  SELECT * FROM $tempViewName
              |)
              |SELECT * FROM cte
              |""".stripMargin)
          checkAnswer(sql(s"SELECT * FROM $testViewName"), Row(1))
        }
      }
    }
  }

  test("SPARK-34421: Resolve temporary objects in permanent views with CTEs") {
    val tempFuncName = "temp_func"
    withUserDefinedFunction((tempFuncName, true)) {
      spark.udf.register(tempFuncName, identity[Int](_))

      val tempViewName = "temp_view"
      withTempView(tempViewName) {
        sql(s"CREATE TEMPORARY VIEW $tempViewName AS SELECT 1")

        val testViewName = "test_view"

        val e = intercept[AnalysisException] {
          sql(
            s"""
              |CREATE VIEW $testViewName AS
              |WITH cte AS (
              |  SELECT * FROM $tempViewName
              |)
              |SELECT * FROM cte
              |""".stripMargin)
        }
        assert(e.message.contains("Not allowed to create a permanent view " +
          s"`$SESSION_CATALOG_NAME`.`default`.`$testViewName` by referencing a " +
          s"temporary view $tempViewName"))

        val e2 = intercept[AnalysisException] {
          sql(
            s"""
              |CREATE VIEW $testViewName AS
              |WITH cte AS (
              |  SELECT $tempFuncName(0)
              |)
              |SELECT * FROM cte
              |""".stripMargin)
        }
        assert(e2.message.contains("Not allowed to create a permanent view " +
          s"`$SESSION_CATALOG_NAME`.`default`.`$testViewName` by referencing a " +
          s"temporary function `$tempFuncName`"))
      }
    }
  }

  test("SPARK-26138 Pushdown limit through InnerLike when condition is empty") {
    withTable("t1", "t2") {
      spark.range(5).repartition(1).write.saveAsTable("t1")
      spark.range(5).repartition(1).write.saveAsTable("t2")
      val df = spark.sql("SELECT * FROM t1 CROSS JOIN t2 LIMIT 3")
      val pushedLocalLimits = df.queryExecution.optimizedPlan.collect {
        case l @ LocalLimit(_, _: LogicalRelation) => l
      }
      assert(pushedLocalLimits.length === 2)
      checkAnswer(df, Row(0, 0) :: Row(0, 1) :: Row(0, 2) :: Nil)
    }
  }

  test("SPARK-34514: Push down limit through LEFT SEMI and LEFT ANTI join") {
    withTable("left_table", "nonempty_right_table", "empty_right_table") {
      spark.range(5).toDF().repartition(1).write.saveAsTable("left_table")
      spark.range(3).write.saveAsTable("nonempty_right_table")
      spark.range(0).write.saveAsTable("empty_right_table")
      Seq("LEFT SEMI", "LEFT ANTI").foreach { joinType =>
        val joinWithNonEmptyRightDf = spark.sql(
          s"SELECT * FROM left_table $joinType JOIN nonempty_right_table LIMIT 3")
        val joinWithEmptyRightDf = spark.sql(
          s"SELECT * FROM left_table $joinType JOIN empty_right_table LIMIT 3")

        Seq(joinWithNonEmptyRightDf, joinWithEmptyRightDf).foreach { df =>
          val pushedLocalLimits = df.queryExecution.optimizedPlan.collect {
            case l @ LocalLimit(_, _: LogicalRelation) => l
          }
          assert(pushedLocalLimits.length === 1)
        }

        val expectedAnswer = Seq(Row(0), Row(1), Row(2))
        if (joinType == "LEFT SEMI") {
          checkAnswer(joinWithNonEmptyRightDf, expectedAnswer)
          checkAnswer(joinWithEmptyRightDf, Seq.empty)
        } else {
          checkAnswer(joinWithNonEmptyRightDf, Seq.empty)
          checkAnswer(joinWithEmptyRightDf, expectedAnswer)
        }
      }
    }
  }

  test("SPARK-34575 Push down limit through window when partitionSpec is empty") {
    withTable("t1") {
      val numRows = 10
      spark.range(numRows)
        .selectExpr("if (id % 2 = 0, null, id) AS a", s"$numRows - id AS b")
        .write
        .saveAsTable("t1")

      val df1 = spark.sql(
        """
          |SELECT a, b, ROW_NUMBER() OVER(ORDER BY a, b) AS rn
          |FROM t1 LIMIT 3
          |""".stripMargin)
      val pushedLocalLimits1 = df1.queryExecution.optimizedPlan.collect {
        case l @ LocalLimit(_, _: Sort) => l
      }
      assert(pushedLocalLimits1.length === 1)
      checkAnswer(df1, Seq(Row(null, 2, 1), Row(null, 4, 2), Row(null, 6, 3)))

      val df2 = spark.sql(
        """
          |SELECT b, RANK() OVER(ORDER BY a, b) AS rk, DENSE_RANK(b) OVER(ORDER BY a, b) AS s
          |FROM t1 LIMIT 2
          |""".stripMargin)
      val pushedLocalLimits2 = df2.queryExecution.optimizedPlan.collect {
        case l @ LocalLimit(_, _: Sort) => l
      }
      assert(pushedLocalLimits2.length === 1)
      checkAnswer(df2, Seq(Row(2, 1, 1), Row(4, 2, 2)))
    }
  }

  test("SPARK-34796: Avoid code-gen compilation error for LIMIT query") {
    withTable("left_table", "empty_right_table", "output_table") {
      spark.range(5).toDF("k").write.saveAsTable("left_table")
      spark.range(0).toDF("k").write.saveAsTable("empty_right_table")

      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        spark.sql("CREATE TABLE output_table (k INT) USING parquet")
        spark.sql(
          """
            |INSERT INTO TABLE output_table
            |SELECT t1.k FROM left_table t1
            |JOIN empty_right_table t2
            |ON t1.k = t2.k
            |LIMIT 3
          """.stripMargin)
      }
    }
  }

  test("SPARK-33482: Fix FileScan canonicalization") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(5).toDF().write.mode("overwrite").parquet(path.toString)
        withTempView("t") {
          spark.read.parquet(path.toString).createOrReplaceTempView("t")
          val df = sql(
            """
              |SELECT *
              |FROM t AS t1
              |JOIN t AS t2 ON t2.id = t1.id
              |JOIN t AS t3 ON t3.id = t2.id
              |""".stripMargin)
          df.collect()
          val reusedExchanges = collect(df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }
          assert(reusedExchanges.size == 1)
        }
      }
    }
  }

  test("SPARK-40247: Fix BitSet equals") {
    withTable("td") {
      testData
        .withColumn("bucket", $"key" % 3)
        .write
        .mode(SaveMode.Overwrite)
        .bucketBy(2, "bucket")
        .format("parquet")
        .saveAsTable("td")
      val df = sql(
        """
          |SELECT t1.key, t2.key, t3.key
          |FROM td AS t1
          |JOIN td AS t2 ON t2.key = t1.key
          |JOIN td AS t3 ON t3.key = t2.key
          |WHERE t1.bucket = 1 AND t2.bucket = 1 AND t3.bucket = 1
          |""".stripMargin)
      df.collect()
      val reusedExchanges = collect(df.queryExecution.executedPlan) {
        case r: ReusedExchangeExec => r
      }
      assert(reusedExchanges.size == 1)
    }
  }

  test("SPARK-40245: Fix FileScan canonicalization when partition or data filter columns are not " +
    "read") {
    withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key -> "") {
      withTempPath { path =>
        spark.range(5)
          .withColumn("p", $"id" % 2)
          .write
          .mode("overwrite")
          .partitionBy("p")
          .parquet(path.toString)
        withTempView("t") {
          spark.read.parquet(path.toString).createOrReplaceTempView("t")
          val df = sql(
            """
              |SELECT t1.id, t2.id, t3.id
              |FROM t AS t1
              |JOIN t AS t2 ON t2.id = t1.id
              |JOIN t AS t3 ON t3.id = t2.id
              |WHERE t1.p = 1 AND t2.p = 1 AND t3.p = 1
              |""".stripMargin)
          df.collect()
          val reusedExchanges = collect(df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }
          assert(reusedExchanges.size == 1)
        }
      }
    }
  }

  test("SPARK-35331: Fix resolving original expression in RepartitionByExpression after aliased") {
    Seq("CLUSTER", "DISTRIBUTE").foreach { keyword =>
      Seq("a", "substr(a, 0, 3)").foreach { expr =>
        val clause = keyword + " by " + expr
        withClue(clause) {
          checkAnswer(sql(s"select a b from values('123') t(a) $clause"), Row("123"))
        }
      }
    }
    checkAnswer(sql(s"select /*+ REPARTITION(3, a) */ a b from values('123') t(a)"), Row("123"))
  }

  test("SPARK-35737: Parse day-time interval literals to tightest types") {
    import DayTimeIntervalType._
    val dayToSecDF = spark.sql("SELECT INTERVAL '13 02:02:10' DAY TO SECOND")
    assert(dayToSecDF.schema.head.dataType === DayTimeIntervalType(DAY, SECOND))
    val dayToMinuteDF = spark.sql("SELECT INTERVAL '-2 13:00' DAY TO MINUTE")
    assert(dayToMinuteDF.schema.head.dataType === DayTimeIntervalType(DAY, MINUTE))
    val dayToHourDF = spark.sql("SELECT INTERVAL '0 15' DAY TO HOUR")
    assert(dayToHourDF.schema.head.dataType === DayTimeIntervalType(DAY, HOUR))
    val dayDF = spark.sql("SELECT INTERVAL '23' DAY")
    assert(dayDF.schema.head.dataType === DayTimeIntervalType(DAY))
    val hourToSecDF = spark.sql("SELECT INTERVAL '00:21:02.03' HOUR TO SECOND")
    assert(hourToSecDF.schema.head.dataType === DayTimeIntervalType(HOUR, SECOND))
    val hourToMinuteDF = spark.sql("SELECT INTERVAL '01:02' HOUR TO MINUTE")
    assert(hourToMinuteDF.schema.head.dataType === DayTimeIntervalType(HOUR, MINUTE))
    val hourDF1 = spark.sql("SELECT INTERVAL '17' HOUR")
    assert(hourDF1.schema.head.dataType === DayTimeIntervalType(HOUR))
    val minuteToSecDF = spark.sql("SELECT INTERVAL '10:03.775808000' MINUTE TO SECOND")
    assert(minuteToSecDF.schema.head.dataType === DayTimeIntervalType(MINUTE, SECOND))
    val minuteDF1 = spark.sql("SELECT INTERVAL '03' MINUTE")
    assert(minuteDF1.schema.head.dataType === DayTimeIntervalType(MINUTE))
    val secondDF1 = spark.sql("SELECT INTERVAL '11' SECOND")
    assert(secondDF1.schema.head.dataType === DayTimeIntervalType(SECOND))

    // Seconds greater than 1 minute
    val secondDF2 = spark.sql("SELECT INTERVAL '75' SECONDS")
    assert(secondDF2.schema.head.dataType === DayTimeIntervalType(SECOND))

    // Minutes and seconds greater than 1 hour
    val minuteDF2 = spark.sql("SELECT INTERVAL '68' MINUTES")
    assert(minuteDF2.schema.head.dataType === DayTimeIntervalType(MINUTE))
    val secondDF3 = spark.sql("SELECT INTERVAL '11112' SECONDS")
    assert(secondDF3.schema.head.dataType === DayTimeIntervalType(SECOND))

    // Hours, minutes and seconds greater than 1 day
    val hourDF2 = spark.sql("SELECT INTERVAL '27' HOURS")
    assert(hourDF2.schema.head.dataType === DayTimeIntervalType(HOUR))
    val minuteDF3 = spark.sql("SELECT INTERVAL '2883' MINUTES")
    assert(minuteDF3.schema.head.dataType === DayTimeIntervalType(MINUTE))
    val secondDF4 = spark.sql("SELECT INTERVAL '266582' SECONDS")
    assert(secondDF4.schema.head.dataType === DayTimeIntervalType(SECOND))
  }

  test("SPARK-35773: Parse year-month interval literals to tightest types") {
    import YearMonthIntervalType._
    val yearToMonthDF = spark.sql("SELECT INTERVAL '2021-06' YEAR TO MONTH")
    assert(yearToMonthDF.schema.head.dataType === YearMonthIntervalType(YEAR, MONTH))
    val yearDF = spark.sql("SELECT INTERVAL '2022' YEAR")
    assert(yearDF.schema.head.dataType === YearMonthIntervalType(YEAR))
    val monthDF1 = spark.sql("SELECT INTERVAL '08' MONTH")
    assert(monthDF1.schema.head.dataType === YearMonthIntervalType(MONTH))
    // Months greater than 1 year
    val monthDF2 = spark.sql("SELECT INTERVAL '25' MONTHS")
    assert(monthDF2.schema.head.dataType === YearMonthIntervalType(MONTH))
  }

  test("SPARK-35749: Parse multiple unit fields interval literals as day-time interval types") {
    def evalAsSecond(query: String): Long = {
      spark.sql(query).map(_.getAs[Duration](0)).collect.head.getSeconds
    }

    Seq(
      ("SELECT INTERVAL '7' DAY", 604800),
      ("SELECT INTERVAL '5' HOUR", 18000),
      ("SELECT INTERVAL '2' MINUTE", 120),
      ("SELECT INTERVAL '30' SECOND", 30),
      ("SELECT INTERVAL '10' DAY '20' HOUR '30' MINUTE '40' SECOND", 937840),
      // Units end with 's'
      ("SELECT INTERVAL '2' DAYS '18' HOURS '34' MINUTES '53' SECONDS", 239693),
      // A unit occurs more than one time
      ("SELECT INTERVAL '1' DAY '23' HOURS '3' DAYS '70' MINUTES " +
        "'5' SECONDS '24' HOURS '10' MINUTES '80' SECONDS", 519685)
    ).foreach { case (query, expect) =>
      assert(evalAsSecond(query) === expect)
      // Units are lower case
      assert(evalAsSecond(query.toLowerCase(Locale.ROOT)) === expect)
    }
  }

  test("SPARK-35749: Parse multiple unit fields interval literals as year-month interval types") {
    def evalAsYearAndMonth(query: String): (Int, Int) = {
      val result = spark.sql(query).map(_.getAs[Period](0)).collect.head
      (result.getYears, result.getMonths)
    }

    Seq(
      ("SELECT INTERVAL '10' YEAR", (10, 0)),
      ("SELECT INTERVAL '7' MONTH", (0, 7)),
      ("SELECT INTERVAL '8' YEAR '3' MONTH", (8, 3)),
      // Units end with 's'
      ("SELECT INTERVAL '5' YEARS '10' MONTHS", (5, 10)),
      // A unit is appears more than one time
      ("SELECT INTERVAL '3' YEARS '5' MONTHS '1' YEAR '8' MONTHS", (5, 1))
    ).foreach { case (query, expect) =>
      assert(evalAsYearAndMonth(query) === expect)
      // Units are lower case
      assert(evalAsYearAndMonth(query.toLowerCase(Locale.ROOT)) === expect)
    }
  }

  test("SPARK-35937: Extract date field from timestamp should work in ANSI mode") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      Seq("to_timestamp", "to_timestamp_ntz").foreach { func =>
        checkAnswer(sql(s"select extract(year from $func('2021-01-02 03:04:05'))"), Row(2021))
        checkAnswer(sql(s"select extract(month from $func('2021-01-02 03:04:05'))"), Row(1))
        checkAnswer(sql(s"select extract(day from $func('2021-01-02 03:04:05'))"), Row(2))
      }
    }
  }

  test("SPARK-35545: split SubqueryExpression's children field into outer attributes and " +
    "join conditions") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      checkAnswer(sql(
        s"""with
           |start as (
           |  select c1, c2 from t A where not exists (
           |    select * from t B where A.c1 = B.c1 - 2
           |  )
           |),
           |
           |end as (
           |  select c1, c2 from t A where not exists (
           |    select * from t B where A.c1 < B.c1
           |  )
           |)
           |
           |select * from start S join end E on S.c1 = E.c1
           |""".stripMargin),
        Row(1, 2, 1, 2) :: Nil)
    }
  }

  test("SPARK-36093: RemoveRedundantAliases should not change expression's name") {
    withTable("t1", "t2") {
      withView("t1_v") {
        sql("CREATE TABLE t1(cal_dt DATE) USING PARQUET")
        sql(
          """
            |INSERT INTO t1 VALUES
            |(date'2021-06-27'),
            |(date'2021-06-28'),
            |(date'2021-06-29'),
            |(date'2021-06-30')""".stripMargin)
        sql("CREATE VIEW t1_v AS SELECT * FROM t1")
        sql(
          """
            |CREATE TABLE t2(FLAG INT, CAL_DT DATE)
            |USING PARQUET
            |PARTITIONED BY (CAL_DT)""".stripMargin)
        val insert = sql(
          """
            |INSERT INTO t2 SELECT 2 AS FLAG,CAL_DT FROM t1_v
            |WHERE CAL_DT BETWEEN '2021-06-29' AND '2021-06-30'""".stripMargin)
        insert.queryExecution.executedPlan.collectFirst {
          case CommandResultExec(_, DataWritingCommandExec(
            i: InsertIntoHadoopFsRelationCommand, _), _) => i
        }.get.partitionColumns.map(_.name).foreach(name => assert(name == "CAL_DT"))
        checkAnswer(sql("SELECT FLAG, CAST(CAL_DT as STRING) FROM t2 "),
            Row(2, "2021-06-29") :: Row(2, "2021-06-30") :: Nil)
        checkAnswer(sql("SHOW PARTITIONS t2"),
            Row("CAL_DT=2021-06-29") :: Row("CAL_DT=2021-06-30") :: Nil)
      }
    }
  }

  test("SPARK-36371: Support raw string literal") {
    checkAnswer(sql("""SELECT r'a\tb\nc'"""), Row("""a\tb\nc"""))
    checkAnswer(sql("""SELECT R'a\tb\nc'"""), Row("""a\tb\nc"""))
    checkAnswer(sql("""SELECT r"a\tb\nc""""), Row("""a\tb\nc"""))
    checkAnswer(sql("""SELECT R"a\tb\nc""""), Row("""a\tb\nc"""))
    checkAnswer(sql("""SELECT from_json(r'{"a": "\\"}', 'a string')"""), Row(Row("\\")))
    checkAnswer(sql("""SELECT from_json(R'{"a": "\\"}', 'a string')"""), Row(Row("\\")))
  }

  test("TABLE SAMPLE") {
    withTable("test") {
      sql("CREATE TABLE test(c int) USING PARQUET")
      for (i <- 0 to 20) {
        sql(s"INSERT INTO test VALUES ($i)")
      }
      val df1 = sql("SELECT * FROM test TABLESAMPLE (20 PERCENT) REPEATABLE (12345)")
      val df2 = sql("SELECT * FROM test TABLESAMPLE (20 PERCENT) REPEATABLE (12345)")
      checkAnswer(df1, df2)

      val df3 = sql("SELECT * FROM test TABLESAMPLE (BUCKET 4 OUT OF 10) REPEATABLE (6789)")
      val df4 = sql("SELECT * FROM test TABLESAMPLE (BUCKET 4 OUT OF 10) REPEATABLE (6789)")
      checkAnswer(df3, df4)
    }
  }

  test("SPARK-27442: Spark support read/write parquet file with invalid char in field name") {
    withTempDir { dir =>
      Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10), (2, 4, 6, 8, 10, 12, 14, 16, 18, 20))
        .toDF("max(t)", "max(t", "=", "\n", ";", "a b", "{", ".", "a.b", "a")
        .repartition(1)
        .write.mode(SaveMode.Overwrite).parquet(dir.getAbsolutePath)
      val df = spark.read.parquet(dir.getAbsolutePath)
      checkAnswer(df,
        Row(1, 2, 3, 4, 5, 6, 7, 8, 9, 10) ::
          Row(2, 4, 6, 8, 10, 12, 14, 16, 18, 20) :: Nil)
      assert(df.schema.names.sameElements(
        Array("max(t)", "max(t", "=", "\n", ";", "a b", "{", ".", "a.b", "a")))
      checkAnswer(df.select("`max(t)`", "`a b`", "`{`", "`.`", "`a.b`"),
        Row(1, 6, 7, 8, 9) :: Row(2, 12, 14, 16, 18) :: Nil)
      checkAnswer(df.where("`a.b` > 10"),
        Row(2, 4, 6, 8, 10, 12, 14, 16, 18, 20) :: Nil)
    }
  }

  test("SPARK-37965: Spark support read/write orc file with invalid char in field name") {
    withTempDir { dir =>
      Seq((1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11), (2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22))
        .toDF("max(t)", "max(t", "=", "\n", ";", "a b", "{", ".", "a.b", "a", ",")
        .repartition(1)
        .write.mode(SaveMode.Overwrite).orc(dir.getAbsolutePath)
      val df = spark.read.orc(dir.getAbsolutePath)
      checkAnswer(df,
        Row(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11) ::
          Row(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22) :: Nil)
      assert(df.schema.names.sameElements(
        Array("max(t)", "max(t", "=", "\n", ";", "a b", "{", ".", "a.b", "a", ",")))
      checkAnswer(df.select("`max(t)`", "`a b`", "`{`", "`.`", "`a.b`"),
        Row(1, 6, 7, 8, 9) :: Row(2, 12, 14, 16, 18) :: Nil)
      checkAnswer(df.where("`a.b` > 10"),
        Row(2, 4, 6, 8, 10, 12, 14, 16, 18, 20, 22) :: Nil)
    }
  }

  test("SPARK-38173: Quoted column cannot be recognized correctly " +
    "when quotedRegexColumnNames is true") {
    withSQLConf(SQLConf.SUPPORT_QUOTED_REGEX_COLUMN_NAME.key -> "true") {
      checkAnswer(
        sql(
          """
            |SELECT `(C3)?+.+`,T.`C1` * `C2` AS CC
            |FROM (SELECT 3 AS C1,2 AS C2,1 AS C3) T
            |""".stripMargin),
        Row(3, 2, 6) :: Nil)
    }
  }

  test("SPARK-38548: try_sum should return null if overflow happens before merging") {
    val longDf = Seq(Long.MaxValue, Long.MaxValue, 2).toDF("v")
    val yearMonthDf = Seq(Int.MaxValue, Int.MaxValue, 2)
      .map(Period.ofMonths)
      .toDF("v")
    val dayTimeDf = Seq(106751991L, 106751991L, 2L)
      .map(Duration.ofDays)
      .toDF("v")
    Seq(longDf, yearMonthDf, dayTimeDf).foreach { df =>
      checkAnswer(df.repartitionByRange(2, col("v")).selectExpr("try_sum(v)"), Row(null))
    }
  }

  test("SPARK-39166: Query context of binary arithmetic should be serialized to executors" +
    " when WSCG is off") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("t") {
        sql("create table t(i int, j int) using parquet")
        sql("insert into t values(2147483647, 10)")
        Seq(
          "select i + j from t",
          "select -i - j from t",
          "select i * j from t",
          "select i / (j - 10) from t").foreach { query =>
          val msg = intercept[SparkException] {
            sql(query).collect()
          }.getMessage
          assert(msg.contains(query))
        }
      }
    }
  }

  test("SPARK-39175: Query context of Cast should be serialized to executors" +
    " when WSCG is off") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("t") {
        sql("create table t(s string) using parquet")
        sql("insert into t values('a')")
        Seq(
          "select cast(s as int) from t",
          "select cast(s as long) from t",
          "select cast(s as double) from t",
          "select cast(s as decimal(10, 2)) from t",
          "select cast(s as date) from t",
          "select cast(s as timestamp) from t",
          "select cast(s as boolean) from t").foreach { query =>
          val msg = intercept[SparkException] {
            sql(query).collect()
          }.getMessage
          assert(msg.contains(query))
        }
      }
    }
  }

  test("SPARK-39190,SPARK-39208,SPARK-39210: Query context of decimal overflow error should " +
    "be serialized to executors when WSCG is off") {
    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
      SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("t") {
        sql("create table t(d decimal(38, 0)) using parquet")
        sql("insert into t values (6e37BD),(6e37BD)")
        Seq(
          "select d / 0.1 from t",
          "select sum(d) from t",
          "select avg(d) from t").foreach { query =>
          val msg = intercept[SparkException] {
            sql(query).collect()
          }.getMessage
          assert(msg.contains(query))
        }
      }
    }
  }

  test("SPARK-38589: try_avg should return null if overflow happens before merging") {
    val yearMonthDf = Seq(Int.MaxValue, Int.MaxValue, 2)
      .map(Period.ofMonths)
      .toDF("v")
    val dayTimeDf = Seq(106751991L, 106751991L, 2L)
      .map(Duration.ofDays)
      .toDF("v")
    Seq(yearMonthDf, dayTimeDf).foreach { df =>
      checkAnswer(df.repartitionByRange(2, col("v")).selectExpr("try_avg(v)"), Row(null))
    }
  }

  test("SPARK-39012: SparkSQL cast partition value does not support all data types") {
    withTempDir { dir =>
      val binary1 = Hex.hex(UTF8String.fromString("Spark").getBytes).getBytes
      val binary2 = Hex.hex(UTF8String.fromString("SQL").getBytes).getBytes
      val data = Seq[(Int, Boolean, Array[Byte])](
        (1, false, binary1),
        (2, true, binary2)
      )
      data.toDF("a", "b", "c")
        .write
        .mode("overwrite")
        .partitionBy("b", "c")
        .parquet(dir.getCanonicalPath)
      val res = spark.read
        .schema("a INT, b BOOLEAN, c BINARY")
        .parquet(dir.getCanonicalPath)
      checkAnswer(res,
        Seq(
          Row(1, false, mutable.WrappedArray.make(binary1)),
          Row(2, true, mutable.WrappedArray.make(binary2))
        ))
    }
  }

  test("SPARK-39216: Don't collapse projects in CombineUnions if it hasCorrelatedSubquery") {
    checkAnswer(
      sql(
        """
          |SELECT (SELECT IF(x, 1, 0)) AS a
          |FROM (SELECT true) t(x)
          |UNION
          |SELECT 1 AS a
        """.stripMargin),
      Seq(Row(1)))

    checkAnswer(
      sql(
        """
          |SELECT x + 1
          |FROM   (SELECT id
          |               + (SELECT Max(id)
          |                  FROM   range(2)) AS x
          |        FROM   range(1)) t
          |UNION
          |SELECT 1 AS a
        """.stripMargin),
      Seq(Row(2), Row(1)))
  }

  test("SPARK-39548: CreateView will make queries go into inline CTE code path thus" +
    "trigger a mis-clarified `window definition not found` issue") {
    sql(
      """
        |create or replace temporary view test_temp_view as
        |with step_1 as (
        |select * , min(a) over w2 as min_a_over_w2 from
        |(select 1 as a, 2 as b, 3 as c) window w2 as (partition by b order by c)) , step_2 as
        |(
        |select *, max(e) over w1 as max_a_over_w1
        |from (select 1 as e, 2 as f, 3 as g)
        |join step_1 on true
        |window w1 as (partition by f order by g)
        |)
        |select *
        |from step_2
        |""".stripMargin)

    checkAnswer(
      sql("select * from test_temp_view"),
      Row(1, 2, 3, 1, 2, 3, 1, 1))
  }

  test("SPARK-40389: Don't eliminate a cast which can cause overflow") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      withTable("dt") {
        sql("create table dt using parquet as select 9000000000BD as d")
        val msg = intercept[SparkException] {
          sql("select cast(cast(d as int) as long) from dt").collect()
        }.getCause.getMessage
        assert(msg.contains("[CAST_OVERFLOW]"))
      }
    }
  }
}

case class Foo(bar: Option[String])
