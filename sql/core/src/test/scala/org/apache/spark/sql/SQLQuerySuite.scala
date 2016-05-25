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

import java.math.MathContext
import java.sql.Timestamp

import org.apache.spark.AccumulatorSuite
import org.apache.spark.sql.catalyst.analysis.UnresolvedException
import org.apache.spark.sql.catalyst.expressions.SortOrder
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.catalyst.util.StringUtils
import org.apache.spark.sql.execution.aggregate
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, CartesianProductExec, SortMergeJoinExec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSQLContext, TestSQLContext}
import org.apache.spark.sql.test.SQLTestData._
import org.apache.spark.sql.types._

class SQLQuerySuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  setupTestData()

  test("having clause") {
    Seq(("one", 1), ("two", 2), ("three", 3), ("one", 5)).toDF("k", "v")
      .createOrReplaceTempView("hav")
    checkAnswer(
      sql("SELECT k, sum(v) FROM hav GROUP BY k HAVING sum(v) > 2"),
      Row("one", 6) :: Row("three", 3) :: Nil)
  }

  test("SPARK-8010: promote numeric to string") {
    val df = Seq((1, 1)).toDF("key", "value")
    df.createOrReplaceTempView("src")
    val queryCaseWhen = sql("select case when true then 1.0 else '1' end from src ")
    val queryCoalesce = sql("select coalesce(null, 1, '1') from src ")

    checkAnswer(queryCaseWhen, Row("1.0") :: Nil)
    checkAnswer(queryCoalesce, Row("1") :: Nil)
  }

  test("show functions") {
    def getFunctions(pattern: String): Seq[Row] = {
      StringUtils.filterPattern(spark.sessionState.functionRegistry.listFunction(), pattern)
        .map(Row(_))
    }
    checkAnswer(sql("SHOW functions"), getFunctions("*"))
    Seq("^c*", "*e$", "log*", "*date*").foreach { pattern =>
      // For the pattern part, only '*' and '|' are allowed as wildcards.
      // For '*', we need to replace it to '.*'.
      checkAnswer(sql(s"SHOW FUNCTIONS '$pattern'"), getFunctions(pattern))
    }
  }

  test("describe functions") {
    checkKeywordsExist(sql("describe function extended upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns str with all characters changed to uppercase",
      "Extended Usage:",
      "> SELECT upper('SparkSql');",
      "'SPARKSQL'")

    checkKeywordsExist(sql("describe functioN Upper"),
      "Function: upper",
      "Class: org.apache.spark.sql.catalyst.expressions.Upper",
      "Usage: upper(str) - Returns str with all characters changed to uppercase")

    checkKeywordsNotExist(sql("describe functioN Upper"), "Extended Usage")

    checkKeywordsExist(sql("describe functioN abcadf"), "Function: abcadf not found.")
  }

  test("SPARK-14415: All functions should have own descriptions") {
    for (f <- spark.sessionState.functionRegistry.listFunction()) {
      if (!Seq("cube", "grouping", "grouping_id", "rollup", "window").contains(f)) {
        checkKeywordsNotExist(sql(s"describe function `$f`"), "N/A.")
      }
    }
  }

  test("SPARK-6743: no columns from cache") {
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

  test("self join with aliases") {
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
    spark.read.json(sparkContext.parallelize(
      """{"nested": {"attribute": 1}, "value": 2}""" :: Nil))
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

  test("SPARK-6201 IN type conversion") {
    spark.read.json(
      sparkContext.parallelize(
        Seq("{\"a\": \"1\"}}", "{\"a\": \"2\"}}", "{\"a\": \"3\"}}")))
      .createOrReplaceTempView("d")

    checkAnswer(
      sql("select * from d where d.a in (1,2)"),
      Seq(Row("1"), Row("2")))
  }

  test("SPARK-11226 Skip empty line in json file") {
    spark.read.json(
      sparkContext.parallelize(
        Seq("{\"a\": \"1\"}}", "{\"a\": \"2\"}}", "{\"a\": \"3\"}}", "")))
      .createOrReplaceTempView("d")

    checkAnswer(
      sql("select count(1) from d"),
      Seq(Row(3)))
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
      .collect { case _: aggregate.TungstenAggregate => true }
      .nonEmpty
    if (!hasGeneratedAgg) {
      fail(
        s"""
           |Codegen is enabled, but query $sqlText does not have TungstenAggregate in the plan.
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
      // Aggregate with Code generation handling all null values
      testCodeGen(
        "SELECT  sum('a'), avg('a'), count(null) FROM testData",
        Row(null, null, 0) :: Nil)
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
    (0 to 3).map(i => Tuple1(new Timestamp(i))).toDF("time").createOrReplaceTempView("timestamps")

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='1969-12-31 16:00:00.0'"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time=CAST('1969-12-31 16:00:00.001' AS TIMESTAMP)"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='1969-12-31 16:00:00.001'"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE '1969-12-31 16:00:00.001'=time"),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")))

    checkAnswer(sql(
      """SELECT time FROM timestamps WHERE time<'1969-12-31 16:00:00.003'
          AND time>'1969-12-31 16:00:00.001'"""),
      Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.002")))

    checkAnswer(sql(
      """
        |SELECT time FROM timestamps
        |WHERE time IN ('1969-12-31 16:00:00.001','1969-12-31 16:00:00.002')
      """.stripMargin),
      Seq(Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.001")),
        Row(java.sql.Timestamp.valueOf("1969-12-31 16:00:00.002"))))

    checkAnswer(sql(
      "SELECT time FROM timestamps WHERE time='123'"),
      Nil)
  }

  test("index into array") {
    checkAnswer(
      sql("SELECT data, data[0], data[0] + data[1], data[0 + 1] FROM arrayData"),
      arrayData.map(d => Row(d.data, d.data(0), d.data(0) + d.data(1), d.data(1))).collect())
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

  test("index into array of arrays") {
    checkAnswer(
      sql(
        "SELECT nestedData, nestedData[0][0], nestedData[0][0] + nestedData[0][1] FROM arrayData"),
      arrayData.map(d =>
        Row(d.nestedData,
         d.nestedData(0)(0),
         d.nestedData(0)(0) + d.nestedData(0)(1))).collect().toSeq)
  }

  test("agg") {
    checkAnswer(
      sql("SELECT a, SUM(b) FROM testData2 GROUP BY a"),
      Seq(Row(1, 3), Row(2, 3), Row(3, 3)))
  }

  test("Group By Ordinal - basic") {
    checkAnswer(
      sql("SELECT a, sum(b) FROM testData2 GROUP BY 1"),
      sql("SELECT a, sum(b) FROM testData2 GROUP BY a"))

    // duplicate group-by columns
    checkAnswer(
      sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a, 1"),
      sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a"))

    checkAnswer(
      sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY 1, 2"),
      sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a"))
  }

  test("Group By Ordinal - non aggregate expressions") {
    checkAnswer(
      sql("SELECT a, b + 2, count(2) FROM testData2 GROUP BY a, 2"),
      sql("SELECT a, b + 2, count(2) FROM testData2 GROUP BY a, b + 2"))

    checkAnswer(
      sql("SELECT a, b + 2 as c, count(2) FROM testData2 GROUP BY a, 2"),
      sql("SELECT a, b + 2, count(2) FROM testData2 GROUP BY a, b + 2"))
  }

  test("Group By Ordinal - non-foldable constant expression") {
    checkAnswer(
      sql("SELECT a, b, sum(b) FROM testData2 GROUP BY a, b, 1 + 0"),
      sql("SELECT a, b, sum(b) FROM testData2 GROUP BY a, b"))

    checkAnswer(
      sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a, 1 + 2"),
      sql("SELECT a, 1, sum(b) FROM testData2 GROUP BY a"))
  }

  test("Group By Ordinal - alias") {
    checkAnswer(
      sql("SELECT a, (b + 2) as c, count(2) FROM testData2 GROUP BY a, 2"),
      sql("SELECT a, b + 2, count(2) FROM testData2 GROUP BY a, b + 2"))

    checkAnswer(
      sql("SELECT a as b, b as a, sum(b) FROM testData2 GROUP BY 1, 2"),
      sql("SELECT a, b, sum(b) FROM testData2 GROUP BY a, b"))
  }

  test("Group By Ordinal - constants") {
    checkAnswer(
      sql("SELECT 1, 2, sum(b) FROM testData2 GROUP BY 1, 2"),
      sql("SELECT 1, 2, sum(b) FROM testData2"))
  }

  test("Group By Ordinal - negative cases") {
    intercept[UnresolvedException[Aggregate]] {
      sql("SELECT a, b FROM testData2 GROUP BY -1")
    }

    intercept[UnresolvedException[Aggregate]] {
      sql("SELECT a, b FROM testData2 GROUP BY 3")
    }

    var e = intercept[UnresolvedException[Aggregate]](
      sql("SELECT SUM(a) FROM testData2 GROUP BY 1"))
    assert(e.getMessage contains
      "Invalid call to Group by position: the '1'th column in the select contains " +
        "an aggregate function")

    e = intercept[UnresolvedException[Aggregate]](
      sql("SELECT SUM(a) + 1 FROM testData2 GROUP BY 1"))
    assert(e.getMessage contains
      "Invalid call to Group by position: the '1'th column in the select contains " +
        "an aggregate function")

    var ae = intercept[AnalysisException](
      sql("SELECT a, rand(0), sum(b) FROM testData2 GROUP BY a, 2"))
    assert(ae.getMessage contains
      "nondeterministic expression rand(0) should not appear in grouping expression")

    ae = intercept[AnalysisException](
      sql("SELECT * FROM testData2 GROUP BY a, b, 1"))
    assert(ae.getMessage contains
      "Group by position: star is not allowed to use in the select list " +
        "when using ordinals in group by")
  }

  test("Group By Ordinal: spark.sql.groupByOrdinal=false") {
    withSQLConf(SQLConf.GROUP_BY_ORDINAL.key -> "false") {
      // If spark.sql.groupByOrdinal=false, ignore the position number.
      intercept[AnalysisException] {
        sql("SELECT a, sum(b) FROM testData2 GROUP BY 1")
      }
      // '*' is not allowed to use in the select list when users specify ordinals in group by
      checkAnswer(
        sql("SELECT * FROM testData2 GROUP BY a, b, 1"),
        sql("SELECT * FROM testData2 GROUP BY a, b"))
    }
  }

  test("aggregates with nulls") {
    checkAnswer(
      sql("SELECT SKEWNESS(a), KURTOSIS(a), MIN(a), MAX(a)," +
        "AVG(a), VARIANCE(a), STDDEV(a), SUM(a), COUNT(a) FROM nullInts"),
      Row(0, -1.5, 1, 3, 2, 1.0, 1, 6, 3)
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

  test("limit") {
    checkAnswer(
      sql("SELECT * FROM testData LIMIT 10"),
      testData.take(10).toSeq)

    checkAnswer(
      sql("SELECT * FROM arrayData LIMIT 1"),
      arrayData.collect().take(1).map(Row.fromTuple).toSeq)

    checkAnswer(
      sql("SELECT * FROM mapData LIMIT 1"),
      mapData.collect().take(1).map(Row.fromTuple).toSeq)
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
      Row(java.sql.Date.valueOf("2015-01-28"))
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
    withTempTable("t") {
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
      testData.rdd.flatMap(
        row => Seq.fill(16)(Row.merge(row, row))).collect().toSeq)
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
    checkAnswer(
      sql("SELECT n FROM lowerCaseData UNION SELECT L FROM upperCaseData"),
      ("1" :: "2" :: "3" :: "4" :: "A" :: "B" :: "C" :: "D" :: "E" :: "F" :: Nil).map(Row(_)))
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

  test("SET commands semantics using sql()") {
    spark.sqlContext.conf.clear()
    val testKey = "test.key.0"
    val testVal = "test.val.0"
    val nonexistentKey = "nonexistent"

    // "set" itself returns all config variables currently specified in SQLConf.
    assert(sql("SET").collect().size === TestSQLContext.overrideConfs.size)
    sql("SET").collect().foreach { row =>
      val key = row.getString(0)
      val value = row.getString(1)
      assert(
        TestSQLContext.overrideConfs.contains(key),
        s"$key should exist in SQLConf.")
      assert(
        TestSQLContext.overrideConfs(key) === value,
        s"The value of $key should be ${TestSQLContext.overrideConfs(key)} instead of $value.")
    }
    val overrideConfs = sql("SET").collect()

    // "set key=val"
    sql(s"SET $testKey=$testVal")
    checkAnswer(
      sql("SET"),
      overrideConfs ++ Seq(Row(testKey, testVal))
    )

    sql(s"SET ${testKey + testKey}=${testVal + testVal}")
    checkAnswer(
      sql("set"),
      overrideConfs ++ Seq(Row(testKey, testVal), Row(testKey + testKey, testVal + testVal))
    )

    // "set key"
    checkAnswer(
      sql(s"SET $testKey"),
      Row(testKey, testVal)
    )
    checkAnswer(
      sql(s"SET $nonexistentKey"),
      Row(nonexistentKey, "<undefined>")
    )
    spark.sqlContext.conf.clear()
  }

  test("SET commands with illegal or inappropriate argument") {
    spark.sqlContext.conf.clear()
    // Set negative mapred.reduce.tasks for automatically determining
    // the number of reducers is not supported
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-1"))
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-01"))
    intercept[IllegalArgumentException](sql(s"SET mapred.reduce.tasks=-2"))
    spark.sqlContext.conf.clear()
  }

  test("apply schema") {
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
      Row(Row(values(0).toInt, values(2).toBoolean), scala.collection.mutable.Map(values(1) -> v4))
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

  test("throw errors for non-aggregate attributes with aggregation") {
    def checkAggregation(query: String, isInvalidQuery: Boolean = true) {
      if (isInvalidQuery) {
        val e = intercept[AnalysisException](sql(query).queryExecution.analyzed)
        assert(e.getMessage contains "group by")
      } else {
        // Should not throw
        sql(query).queryExecution.analyzed
      }
    }

    checkAggregation("SELECT key, COUNT(*) FROM testData")
    checkAggregation("SELECT COUNT(key), COUNT(*) FROM testData", isInvalidQuery = false)

    checkAggregation("SELECT value, COUNT(*) FROM testData GROUP BY key")
    checkAggregation("SELECT COUNT(value), SUM(key) FROM testData GROUP BY key", false)

    checkAggregation("SELECT key + 2, COUNT(*) FROM testData GROUP BY key + 1")
    checkAggregation("SELECT key + 1 + 1, COUNT(*) FROM testData GROUP BY key + 1", false)
  }

  test("Test to check we can use Long.MinValue") {
    checkAnswer(
      sql(s"SELECT ${Long.MinValue} FROM testData ORDER BY key LIMIT 1"), Row(Long.MinValue)
    )

    checkAnswer(
      sql(s"SELECT key FROM testData WHERE key > ${Long.MinValue}"),
      (1 to 100).map(Row(_)).toSeq
    )
  }

  test("Floating point number format") {
    checkAnswer(
      sql("SELECT 0.3"), Row(BigDecimal(0.3))
    )

    checkAnswer(
      sql("SELECT -0.8"), Row(BigDecimal(-0.8))
    )

    checkAnswer(
      sql("SELECT .5"), Row(BigDecimal(0.5))
    )

    checkAnswer(
      sql("SELECT -.18"), Row(BigDecimal(-0.18))
    )
  }

  test("Auto cast integer type") {
    checkAnswer(
      sql(s"SELECT ${Int.MaxValue + 1L}"), Row(Int.MaxValue + 1L)
    )

    checkAnswer(
      sql(s"SELECT ${Int.MinValue - 1L}"), Row(Int.MinValue - 1L)
    )

    checkAnswer(
      sql("SELECT 9223372036854775808"), Row(new java.math.BigDecimal("9223372036854775808"))
    )

    checkAnswer(
      sql("SELECT -9223372036854775809"), Row(new java.math.BigDecimal("-9223372036854775809"))
    )
  }

  test("Test to check we can apply sign to expression") {

    checkAnswer(
      sql("SELECT -100"), Row(-100)
    )

    checkAnswer(
      sql("SELECT +230"), Row(230)
    )

    checkAnswer(
      sql("SELECT -5.2"), Row(BigDecimal(-5.2))
    )

    checkAnswer(
      sql("SELECT +6.8e0"), Row(6.8d)
    )

    checkAnswer(
      sql("SELECT -key FROM testData WHERE key = 2"), Row(-2)
    )

    checkAnswer(
      sql("SELECT +key FROM testData WHERE key = 3"), Row(3)
    )

    checkAnswer(
      sql("SELECT -(key + 1) FROM testData WHERE key = 1"), Row(-2)
    )

    checkAnswer(
      sql("SELECT - key + 1 FROM testData WHERE key = 10"), Row(-9)
    )

    checkAnswer(
      sql("SELECT +(key + 5) FROM testData WHERE key = 5"), Row(10)
    )

    checkAnswer(
      sql("SELECT -MAX(key) FROM testData"), Row(-100)
    )

    checkAnswer(
      sql("SELECT +MAX(key) FROM testData"), Row(100)
    )

    checkAnswer(
      sql("SELECT - (-10)"), Row(10)
    )

    checkAnswer(
      sql("SELECT + (-key) FROM testData WHERE key = 32"), Row(-32)
    )

    checkAnswer(
      sql("SELECT - (+Max(key)) FROM testData"), Row(-100)
    )

    checkAnswer(
      sql("SELECT - - 3"), Row(3)
    )

    checkAnswer(
      sql("SELECT - + 20"), Row(-20)
    )

    checkAnswer(
      sql("SELEcT - + 45"), Row(-45)
    )

    checkAnswer(
      sql("SELECT + + 100"), Row(100)
    )

    checkAnswer(
      sql("SELECT - - Max(key) FROM testData"), Row(100)
    )

    checkAnswer(
      sql("SELECT + - key FROM testData WHERE key = 33"), Row(-33)
    )
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
    val data = sparkContext.parallelize(
      Seq("""{"key?number1": "value1", "key.number2": "value2"}"""))
    spark.read.json(data).createOrReplaceTempView("records")
    sql("SELECT `key?number1`, `key.number2` FROM records")
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
    spark.read.json(sparkContext.makeRDD("""{"a": {"b": [{"c": 1}]}}""" :: Nil))
      .createOrReplaceTempView("data")
    checkAnswer(sql("SELECT a.b[0].c FROM data GROUP BY a.b[0].c"), Row(1))
    spark.catalog.dropTempView("data")

    spark.read.json(
      sparkContext.makeRDD("""{"a": {"b": 1}}""" :: Nil)).createOrReplaceTempView("data")
    checkAnswer(sql("SELECT a.b + 1 FROM data GROUP BY a.b + 1"), Row(2))
    spark.catalog.dropTempView("data")
  }

  test("SPARK-4432 Fix attribute reference resolution error when using ORDER BY") {
    checkAnswer(
      sql("SELECT a + b FROM testData2 ORDER BY a"),
      Seq(2, 3, 3, 4, 4, 5).map(Row(_))
    )
  }

  test("oder by asc by default when not specify ascending and descending") {
    checkAnswer(
      sql("SELECT a, b FROM testData2 ORDER BY a desc, b"),
      Seq(Row(3, 1), Row(3, 2), Row(2, 1), Row(2, 2), Row(1, 1), Row(1, 2))
    )
  }

  test("Supporting relational operator '<=>' in Spark SQL") {
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

  test("Multi-column COUNT(DISTINCT ...)") {
    val data = TestData(1, "val_1") :: TestData(2, "val_2") :: Nil
    val rdd = sparkContext.parallelize((0 to 1).map(i => data(i)))
    rdd.toDF().createOrReplaceTempView("distinctData")
    checkAnswer(sql("SELECT COUNT(DISTINCT key,value) FROM distinctData"), Row(2))
  }

  test("SPARK-4699 case sensitivity SQL query") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val data = TestData(1, "val_1") :: TestData(2, "val_2") :: Nil
      val rdd = sparkContext.parallelize((0 to 1).map(i => data(i)))
      rdd.toDF().createOrReplaceTempView("testTable1")
      checkAnswer(sql("SELECT VALUE FROM TESTTABLE1 where KEY = 1"), Row("val_1"))
    }
  }

  test("SPARK-6145: ORDER BY test for nested fields") {
    spark.read.json(sparkContext.makeRDD(
        """{"a": {"b": 1, "a": {"a": 1}}, "c": [{"d": 1}]}""" :: Nil))
      .createOrReplaceTempView("nestedOrder")

    checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY a.b"), Row(1))
    checkAnswer(sql("SELECT a.b FROM nestedOrder ORDER BY a.b"), Row(1))
    checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY a.a.a"), Row(1))
    checkAnswer(sql("SELECT a.a.a FROM nestedOrder ORDER BY a.a.a"), Row(1))
    checkAnswer(sql("SELECT 1 FROM nestedOrder ORDER BY c[0].d"), Row(1))
    checkAnswer(sql("SELECT c[0].d FROM nestedOrder ORDER BY c[0].d"), Row(1))
  }

  test("SPARK-6145: special cases") {
    spark.read
      .json(sparkContext.makeRDD("""{"a": {"b": [1]}, "b": [{"a": 1}], "_c0": {"a": 1}}""" :: Nil))
      .createOrReplaceTempView("t")

    checkAnswer(sql("SELECT a.b[0] FROM t ORDER BY _c0.a"), Row(1))
    checkAnswer(sql("SELECT b[0].a FROM t ORDER BY _c0.a"), Row(1))
  }

  test("SPARK-6898: complete support for special chars in column names") {
    spark.read.json(sparkContext.makeRDD(
      """{"a": {"c.b": 1}, "b.$q": [{"a@!.q": 1}], "q.w": {"w.i&": [1]}}""" :: Nil))
      .createOrReplaceTempView("t")

    checkAnswer(sql("SELECT a.`c.b`, `b.$q`[0].`a@!.q`, `q.w`.`w.i&`[0] FROM t"), Row(1, 1, 1))
  }

  test("SPARK-6583 order by aggregated function") {
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

  test("SPARK-7952: fix the equality check between boolean and numeric types") {
    withTempTable("t") {
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

  test("SPARK-7067: order by queries for complex ExtractValue chain") {
    withTempTable("t") {
      spark.read.json(sparkContext.makeRDD(
        """{"a": {"b": [{"c": 1}]}, "b": [{"d": 1}]}""" :: Nil)).createOrReplaceTempView("t")
      checkAnswer(sql("SELECT a.b FROM t ORDER BY b[0].d"), Row(Seq(Row(1))))
    }
  }

  test("SPARK-8782: ORDER BY NULL") {
    withTempTable("t") {
      Seq((1, 2), (1, 2)).toDF("a", "b").createOrReplaceTempView("t")
      checkAnswer(sql("SELECT * FROM t ORDER BY NULL"), Seq(Row(1, 2), Row(1, 2)))
    }
  }

  test("SPARK-8837: use keyword in column name") {
    withTempTable("t") {
      val df = Seq(1 -> "a").toDF("count", "sort")
      checkAnswer(df.filter("count > 0"), Row(1, "a"))
      df.createOrReplaceTempView("t")
      checkAnswer(sql("select count, sort from t"), Row(1, "a"))
    }
  }

  test("SPARK-8753: add interval type") {
    import org.apache.spark.unsafe.types.CalendarInterval

    val df = sql("select interval 3 years -3 month 7 week 123 microseconds")
    checkAnswer(df, Row(new CalendarInterval(12 * 3 - 3, 7L * 1000 * 1000 * 3600 * 24 * 7 + 123 )))
    withTempPath(f => {
      // Currently we don't yet support saving out values of interval data type.
      val e = intercept[AnalysisException] {
        df.write.json(f.getCanonicalPath)
      }
      e.message.contains("Cannot save interval data type into external storage")
    })

    val e1 = intercept[AnalysisException] {
      sql("select interval")
    }
    assert(e1.message.contains("at least one time unit should be given for interval literal"))
    // Currently we don't yet support nanosecond
    val e2 = intercept[AnalysisException] {
      sql("select interval 23 nanosecond")
    }
    assert(e2.message.contains("No interval can be constructed"))
  }

  test("SPARK-8945: add and subtract expressions for interval type") {
    import org.apache.spark.unsafe.types.CalendarInterval
    import org.apache.spark.unsafe.types.CalendarInterval.MICROS_PER_WEEK

    val df = sql("select interval 3 years -3 month 7 week 123 microseconds as i")
    checkAnswer(df, Row(new CalendarInterval(12 * 3 - 3, 7L * MICROS_PER_WEEK + 123)))

    checkAnswer(df.select(df("i") + new CalendarInterval(2, 123)),
      Row(new CalendarInterval(12 * 3 - 3 + 2, 7L * MICROS_PER_WEEK + 123 + 123)))

    checkAnswer(df.select(df("i") - new CalendarInterval(2, 123)),
      Row(new CalendarInterval(12 * 3 - 3 - 2, 7L * MICROS_PER_WEEK + 123 - 123)))

    // unary minus
    checkAnswer(df.select(-df("i")),
      Row(new CalendarInterval(-(12 * 3 - 3), -(7L * MICROS_PER_WEEK + 123))))
  }

  test("aggregation with codegen updates peak execution memory") {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sparkContext, "aggregation with codegen") {
      testCodeGen(
        "SELECT key, count(value) FROM testData GROUP BY key",
        (1 to 100).map(i => Row(i, 1)))
    }
  }

  test("decimal precision with multiply/division") {
    checkAnswer(sql("select 10.3 * 3.0"), Row(BigDecimal("30.90")))
    checkAnswer(sql("select 10.3000 * 3.0"), Row(BigDecimal("30.90000")))
    checkAnswer(sql("select 10.30000 * 30.0"), Row(BigDecimal("309.000000")))
    checkAnswer(sql("select 10.300000000000000000 * 3.000000000000000000"),
      Row(BigDecimal("30.900000000000000000000000000000000000", new MathContext(38))))
    checkAnswer(sql("select 10.300000000000000000 * 3.0000000000000000000"),
      Row(null))

    checkAnswer(sql("select 10.3 / 3.0"), Row(BigDecimal("3.433333")))
    checkAnswer(sql("select 10.3000 / 3.0"), Row(BigDecimal("3.4333333")))
    checkAnswer(sql("select 10.30000 / 30.0"), Row(BigDecimal("0.343333333")))
    checkAnswer(sql("select 10.300000000000000000 / 3.00000000000000000"),
      Row(BigDecimal("3.433333333333333333333333333", new MathContext(38))))
    checkAnswer(sql("select 10.3000000000000000000 / 3.00000000000000000"),
      Row(BigDecimal("3.4333333333333333333333333333", new MathContext(38))))
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
      Seq(Row(BigDecimal(2.12321))))
    checkAnswer(
      df.selectExpr("b * a - b"),
      Seq(Row(BigDecimal(0.12321))))
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
    withTempTable("1one") {
      sparkContext.parallelize(1 to 10).map(i => (i, i.toString))
        .toDF("num", "str")
        .createOrReplaceTempView("1one")
      checkAnswer(sql("select count(num) from 1one"), Row(10))
    }
  }

  test("specifying database name for a temporary table is not allowed") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
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
          |CREATE TEMPORARY TABLE db.t
          |USING parquet
          |OPTIONS (
          |  path '$path'
          |)
        """.stripMargin)
      }.getMessage

      // If you use backticks to quote the name then it's OK.
      spark.sql(
        s"""
          |CREATE TEMPORARY TABLE `db.t`
          |USING parquet
          |OPTIONS (
          |  path '$path'
          |)
        """.stripMargin)
      checkAnswer(spark.table("`db.t`"), df)
    }
  }

  test("SPARK-10130 type coercion for IF should have children resolved first") {
    withTempTable("src") {
      Seq((1, 1), (-1, 1)).toDF("key", "value").createOrReplaceTempView("src")
      checkAnswer(
        sql("SELECT IF(a > 0, a, 0) FROM (SELECT key a FROM src) temp"), Seq(Row(1), Row(0)))
    }
  }

  test("SPARK-10389: order by non-attribute grouping expression on Aggregate") {
    withTempTable("src") {
      Seq((1, 1), (-1, 1)).toDF("key", "value").createOrReplaceTempView("src")
      checkAnswer(sql("SELECT MAX(value) FROM src GROUP BY key + 1 ORDER BY key + 1"),
        Seq(Row(1), Row(1)))
      checkAnswer(sql("SELECT MAX(value) FROM src GROUP BY key + 1 ORDER BY (key + 1) * 2"),
        Seq(Row(1), Row(1)))
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

    val e1 = intercept[AnalysisException] {
      sql("select * from in_valid_table")
    }
    assert(e1.message.contains("Table or view not found"))

    val e2 = intercept[AnalysisException] {
      sql("select * from no_db.no_table").show()
    }
    assert(e2.message.contains("Table or view not found"))

    val e3 = intercept[AnalysisException] {
      sql("select * from json.invalid_file")
    }
    assert(e3.message.contains("Path does not exist"))
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

  test("SPARK-11032: resolve having correctly") {
    withTempTable("src") {
      Seq(1 -> "a").toDF("i", "j").createOrReplaceTempView("src")
      checkAnswer(
        sql("SELECT MIN(t.i) FROM (SELECT * FROM src WHERE i > 0) t HAVING(COUNT(1) > 0)"),
        Row(1))
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
      sql("select struct(a, b) as r1, struct(b, a) as r2 from testData2").select($"r1.*", $"r2.*"),
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
        Row(Row(2, 2), Row(2, 2)) :: Row(Row(3, 1), Row(1, 3)) :: Row(Row(3, 2), Row(2, 3)) :: Nil)
    checkAnswer(nestedStructData.select($"record.r1"),
      Row(Row(1, 1)) :: Row(Row(1, 2)) :: Row(Row(2, 1)) :: Row(Row(2, 2)) ::
        Row(Row(3, 1)) :: Row(Row(3, 2)) :: Nil)
    checkAnswer(
      nestedStructData.select($"record.r1.*"),
      Row(1, 1) :: Row(1, 2) :: Row(2, 1) :: Row(2, 2) :: Row(3, 1) :: Row(3, 2) :: Nil)

    // Try with a temporary view
    withTempTable("nestedStructTable") {
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
    val specialCharacterPath = sql(
      """
        | SELECT struct(`col$.a_`, `a.b.c.`) as `r&&b.c` FROM
        |   (SELECT struct(a, b) as `col$.a_`, struct(b, a) as `a.b.c.` FROM testData2) tmp
      """.stripMargin)
    withTempTable("specialCharacterTable") {
      specialCharacterPath.createOrReplaceTempView("specialCharacterTable")
      checkAnswer(
        specialCharacterPath.select($"`r&&b.c`.*"),
        nestedStructData.select($"record.*"))
      checkAnswer(
        sql("SELECT `r&&b.c`.`col$.a_` FROM specialCharacterTable"),
        nestedStructData.select($"record.r1"))
      checkAnswer(
        sql("SELECT `r&&b.c`.`a.b.c.` FROM specialCharacterTable"),
        nestedStructData.select($"record.r2"))
      checkAnswer(
        sql("SELECT `r&&b.c`.`col$.a_`.* FROM specialCharacterTable"),
        nestedStructData.select($"record.r1.*"))
    }

    // Try star expanding a scalar. This should fail.
    assert(intercept[AnalysisException](sql("select a.* from testData2")).getMessage.contains(
      "Can only star expand struct data types."))
  }

  test("Struct Star Expansion - Name conflict") {
    // Create a data set that contains a naming conflict
    val nameConflict = sql("SELECT struct(a, b) as nameConflict, a as a FROM testData2")
    withTempTable("nameConflict") {
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
    withSQLConf("spark.sql.retainGroupColumns" -> "false") {
      checkAnswer(
        testData2.groupBy($"a", $"b").agg($"*"),
        sql("SELECT * FROM testData2 group by a, b"))
    }
  }

  test("Common subexpression elimination") {
    // TODO: support subexpression elimination in whole stage codegen
    withSQLConf("spark.sql.codegen.wholeStage" -> "false") {
      // select from a table to prevent constant folding.
      val df = sql("SELECT a, b from testData2 limit 1")
      checkAnswer(df, Row(1, 1))

      checkAnswer(df.selectExpr("a + 1", "a + 1"), Row(2, 2))
      checkAnswer(df.selectExpr("a + 1", "a + 1 + 1"), Row(2, 3))

      // This does not work because the expressions get grouped like (a + a) + 1
      checkAnswer(df.selectExpr("a + 1", "a + a + 1"), Row(2, 3))
      checkAnswer(df.selectExpr("a + 1", "a + (a + 1)"), Row(2, 3))

      // Identity udf that tracks the number of times it is called.
      val countAcc = sparkContext.accumulator(0, "CallCount")
      spark.udf.register("testUdf", (x: Int) => {
        countAcc.++=(1)
        x
      })

      // Evaluates df, verifying it is equal to the expectedResult and the accumulator's value
      // is correct.
      def verifyCallCount(df: DataFrame, expectedResult: Row, expectedCount: Int): Unit = {
        countAcc.setValue(0)
        checkAnswer(df, expectedResult)
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
        countAcc.++=(1)
        x
      })
      verifyCallCount(
        df.groupBy().agg(sum(testUdf($"b") + testUdf($"b") + testUdf($"b"))), Row(3.0), 1)

      verifyCallCount(
        df.selectExpr("testUdf(a + 1) + testUdf(1 + a)", "testUdf(a + 1)"), Row(4, 2), 1)

      // Try disabling it via configuration.
      spark.conf.set("spark.sql.subexpressionElimination.enabled", "false")
      verifyCallCount(df.selectExpr("testUdf(a)", "testUdf(a)"), Row(1, 1), 2)
      spark.conf.set("spark.sql.subexpressionElimination.enabled", "true")
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

  test("rollup") {
    checkAnswer(
      sql("select course, year, sum(earnings) from courseSales group by rollup(course, year)" +
        " order by course, year"),
      Row(null, null, 113000.0) ::
        Row("Java", null, 50000.0) ::
        Row("Java", 2012, 20000.0) ::
        Row("Java", 2013, 30000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row("dotNET", 2012, 15000.0) ::
        Row("dotNET", 2013, 48000.0) :: Nil
    )
  }

  test("grouping sets when aggregate functions containing groupBy columns") {
    checkAnswer(
      sql("select course, sum(earnings) as sum from courseSales group by course, earnings " +
        "grouping sets((), (course), (course, earnings)) " +
        "order by course, sum"),
      Row(null, 113000.0) ::
        Row("Java", 20000.0) ::
        Row("Java", 30000.0) ::
        Row("Java", 50000.0) ::
        Row("dotNET", 5000.0) ::
        Row("dotNET", 10000.0) ::
        Row("dotNET", 48000.0) ::
        Row("dotNET", 63000.0) :: Nil
    )

    checkAnswer(
      sql("select course, sum(earnings) as sum, grouping_id(course, earnings) from courseSales " +
        "group by course, earnings grouping sets((), (course), (course, earnings)) " +
        "order by course, sum"),
      Row(null, 113000.0, 3) ::
        Row("Java", 20000.0, 0) ::
        Row("Java", 30000.0, 0) ::
        Row("Java", 50000.0, 1) ::
        Row("dotNET", 5000.0, 0) ::
        Row("dotNET", 10000.0, 0) ::
        Row("dotNET", 48000.0, 0) ::
        Row("dotNET", 63000.0, 1) :: Nil
    )
  }

  test("cube") {
    checkAnswer(
      sql("select course, year, sum(earnings) from courseSales group by cube(course, year)"),
      Row("Java", 2012, 20000.0) ::
        Row("Java", 2013, 30000.0) ::
        Row("Java", null, 50000.0) ::
        Row("dotNET", 2012, 15000.0) ::
        Row("dotNET", 2013, 48000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row(null, 2012, 35000.0) ::
        Row(null, 2013, 78000.0) ::
        Row(null, null, 113000.0) :: Nil
    )
  }

  test("grouping sets") {
    checkAnswer(
      sql("select course, year, sum(earnings) from courseSales group by course, year " +
        "grouping sets(course, year)"),
      Row("Java", null, 50000.0) ::
        Row("dotNET", null, 63000.0) ::
        Row(null, 2012, 35000.0) ::
        Row(null, 2013, 78000.0) :: Nil
    )

    checkAnswer(
      sql("select course, year, sum(earnings) from courseSales group by course, year " +
        "grouping sets(course)"),
      Row("Java", null, 50000.0) ::
        Row("dotNET", null, 63000.0) :: Nil
    )

    checkAnswer(
      sql("select course, year, sum(earnings) from courseSales group by course, year " +
        "grouping sets(year)"),
      Row(null, 2012, 35000.0) ::
        Row(null, 2013, 78000.0) :: Nil
    )
  }

  test("grouping and grouping_id") {
    checkAnswer(
      sql("select course, year, grouping(course), grouping(year), grouping_id(course, year)" +
        " from courseSales group by cube(course, year)"),
      Row("Java", 2012, 0, 0, 0) ::
        Row("Java", 2013, 0, 0, 0) ::
        Row("Java", null, 0, 1, 1) ::
        Row("dotNET", 2012, 0, 0, 0) ::
        Row("dotNET", 2013, 0, 0, 0) ::
        Row("dotNET", null, 0, 1, 1) ::
        Row(null, 2012, 1, 0, 2) ::
        Row(null, 2013, 1, 0, 2) ::
        Row(null, null, 1, 1, 3) :: Nil
    )

    var error = intercept[AnalysisException] {
      sql("select course, year, grouping(course) from courseSales group by course, year")
    }
    assert(error.getMessage contains "grouping() can only be used with GroupingSets/Cube/Rollup")
    error = intercept[AnalysisException] {
      sql("select course, year, grouping_id(course, year) from courseSales group by course, year")
    }
    assert(error.getMessage contains "grouping_id() can only be used with GroupingSets/Cube/Rollup")
    error = intercept[AnalysisException] {
      sql("select course, year, grouping__id from courseSales group by cube(course, year)")
    }
    assert(error.getMessage contains "grouping__id is deprecated; use grouping_id() instead")
  }

  test("grouping and grouping_id in having") {
    checkAnswer(
      sql("select course, year from courseSales group by cube(course, year)" +
        " having grouping(year) = 1 and grouping_id(course, year) > 0"),
        Row("Java", null) ::
        Row("dotNET", null) ::
        Row(null, null) :: Nil
    )

    var error = intercept[AnalysisException] {
      sql("select course, year from courseSales group by course, year" +
        " having grouping(course) > 0")
    }
    assert(error.getMessage contains
      "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    error = intercept[AnalysisException] {
      sql("select course, year from courseSales group by course, year" +
        " having grouping_id(course, year) > 0")
    }
    assert(error.getMessage contains
      "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    error = intercept[AnalysisException] {
      sql("select course, year from courseSales group by cube(course, year)" +
        " having grouping__id > 0")
    }
    assert(error.getMessage contains "grouping__id is deprecated; use grouping_id() instead")
  }

  test("grouping and grouping_id in sort") {
    checkAnswer(
      sql("select course, year, grouping(course), grouping(year) from courseSales" +
        " group by cube(course, year) order by grouping_id(course, year), course, year"),
      Row("Java", 2012, 0, 0) ::
        Row("Java", 2013, 0, 0) ::
        Row("dotNET", 2012, 0, 0) ::
        Row("dotNET", 2013, 0, 0) ::
        Row("Java", null, 0, 1) ::
        Row("dotNET", null, 0, 1) ::
        Row(null, 2012, 1, 0) ::
        Row(null, 2013, 1, 0) ::
        Row(null, null, 1, 1) :: Nil
    )

    checkAnswer(
      sql("select course, year, grouping_id(course, year) from courseSales" +
        " group by cube(course, year) order by grouping(course), grouping(year), course, year"),
      Row("Java", 2012, 0) ::
        Row("Java", 2013, 0) ::
        Row("dotNET", 2012, 0) ::
        Row("dotNET", 2013, 0) ::
        Row("Java", null, 1) ::
        Row("dotNET", null, 1) ::
        Row(null, 2012, 2) ::
        Row(null, 2013, 2) ::
        Row(null, null, 3) :: Nil
    )

    var error = intercept[AnalysisException] {
      sql("select course, year from courseSales group by course, year" +
        " order by grouping(course)")
    }
    assert(error.getMessage contains
      "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    error = intercept[AnalysisException] {
      sql("select course, year from courseSales group by course, year" +
        " order by grouping_id(course, year)")
    }
    assert(error.getMessage contains
      "grouping()/grouping_id() can only be used with GroupingSets/Cube/Rollup")
    error = intercept[AnalysisException] {
      sql("select course, year from courseSales group by cube(course, year)" +
        " order by grouping__id")
    }
    assert(error.getMessage contains "grouping__id is deprecated; use grouping_id() instead")
  }

  test("filter on a grouping column that is not presented in SELECT") {
    checkAnswer(
      sql("select count(1) from (select 1 as a) t group by a having a > 0"),
      Row(1) :: Nil)
  }

  test("SPARK-13056: Null in map value causes NPE") {
    val df = Seq(1 -> Map("abc" -> "somestring", "cba" -> null)).toDF("key", "value")
    withTempTable("maptest") {
      df.createOrReplaceTempView("maptest")
      // local optimization will by pass codegen code, so we should keep the filter `key=1`
      checkAnswer(sql("SELECT value['abc'] FROM maptest where key = 1"), Row("somestring"))
      checkAnswer(sql("SELECT value['cba'] FROM maptest where key = 1"), Row(null))
    }
  }

  test("hash function") {
    val df = Seq(1 -> "a", 2 -> "b").toDF("i", "j")
    withTempTable("tbl") {
      df.createOrReplaceTempView("tbl")
      checkAnswer(
        df.select(hash($"i", $"j")),
        sql("SELECT hash(i, j) from tbl")
      )
    }
  }

  test("order by ordinal number") {
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY 1 DESC"),
      sql("SELECT * FROM testData2 ORDER BY a DESC"))
    // If the position is not an integer, ignore it.
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY 1 + 0 DESC, b ASC"),
      sql("SELECT * FROM testData2 ORDER BY b ASC"))
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY 1 DESC, b ASC"),
      sql("SELECT * FROM testData2 ORDER BY a DESC, b ASC"))
    checkAnswer(
      sql("SELECT * FROM testData2 SORT BY 1 DESC, 2"),
      sql("SELECT * FROM testData2 SORT BY a DESC, b ASC"))
    checkAnswer(
      sql("SELECT * FROM testData2 ORDER BY 1 ASC, b ASC"),
      Seq(Row(1, 1), Row(1, 2), Row(2, 1), Row(2, 2), Row(3, 1), Row(3, 2)))
  }

  test("order by ordinal number - negative cases") {
    intercept[UnresolvedException[SortOrder]] {
      sql("SELECT * FROM testData2 ORDER BY 0")
    }
    intercept[UnresolvedException[SortOrder]] {
      sql("SELECT * FROM testData2 ORDER BY -1 DESC, b ASC")
    }
    intercept[UnresolvedException[SortOrder]] {
      sql("SELECT * FROM testData2 ORDER BY 3 DESC, b ASC")
    }
  }

  test("order by ordinal number with conf spark.sql.orderByOrdinal=false") {
    withSQLConf(SQLConf.ORDER_BY_ORDINAL.key -> "false") {
      // If spark.sql.orderByOrdinal=false, ignore the position number.
      checkAnswer(
        sql("SELECT * FROM testData2 ORDER BY 1 DESC, b ASC"),
        sql("SELECT * FROM testData2 ORDER BY b ASC"))
    }
  }

  test("natural join") {
    val df1 = Seq(("one", 1), ("two", 2), ("three", 3)).toDF("k", "v1")
    val df2 = Seq(("one", 1), ("two", 22), ("one", 5)).toDF("k", "v2")
    withTempTable("nt1", "nt2") {
      df1.createOrReplaceTempView("nt1")
      df2.createOrReplaceTempView("nt2")
      checkAnswer(
        sql("SELECT * FROM nt1 natural join nt2 where k = \"one\""),
        Row("one", 1, 1) :: Row("one", 1, 5) :: Nil)

      checkAnswer(
        sql("SELECT * FROM nt1 natural left join nt2 order by v1, v2"),
        Row("one", 1, 1) :: Row("one", 1, 5) :: Row("two", 2, 22) :: Row("three", 3, null) :: Nil)

      checkAnswer(
        sql("SELECT * FROM nt1 natural right join nt2 order by v1, v2"),
        Row("one", 1, 1) :: Row("one", 1, 5) :: Row("two", 2, 22) :: Nil)

      checkAnswer(
        sql("SELECT count(*) FROM nt1 natural full outer join nt2"),
        Row(4) :: Nil)
    }
  }

  test("join with using clause") {
    val df1 = Seq(("r1c1", "r1c2", "t1r1c3"),
      ("r2c1", "r2c2", "t1r2c3"), ("r3c1x", "r3c2", "t1r3c3")).toDF("c1", "c2", "c3")
    val df2 = Seq(("r1c1", "r1c2", "t2r1c3"),
      ("r2c1", "r2c2", "t2r2c3"), ("r3c1y", "r3c2", "t2r3c3")).toDF("c1", "c2", "c3")
    val df3 = Seq((null, "r1c2", "t3r1c3"),
      ("r2c1", "r2c2", "t3r2c3"), ("r3c1y", "r3c2", "t3r3c3")).toDF("c1", "c2", "c3")
    withTempTable("t1", "t2", "t3") {
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

  test("SPARK-14986: Outer lateral view with empty generate expression") {
    checkAnswer(
      sql("select nil from (select 1 as x ) x lateral view outer explode(array()) n as nil"),
      Row(null) :: Nil
    )
  }

  test("data source table created in InMemoryCatalog should be able to read/write") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(i INT, j STRING) USING parquet")
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
}
