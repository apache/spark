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

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.{count, sum}
import org.apache.spark.sql.internal.{SessionState, SQLConf}
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession, TestSQLSessionStateBuilder}

class DeduplicateRelationsRuleSkipTest extends QueryTest with SharedSparkSession {
  import testImplicits._
  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    new CustomRuleTestSession(sparkConf)
  }

  test("basic skip  DedupRels rule") {
    val td = testData2
    val x = withExpectedSkipFlag( true, td.as("x"))
    val y = withExpectedSkipFlag( true, td.as("y"))
    withExpectedSkipFlag(false, x.join(y, $"x.a" === $"y.a", "inner").queryExecution.analyzed)

    val tab1 = testData2.as("testData2")
    val tab2 = testData3.as("testData3")
    withExpectedSkipFlag(true, tab1.join(tab2, usingColumns = Seq("a"), joinType = "fullouter")
      .queryExecution.analyzed)
  }

  test("joins for which DeduplicateRelations rule is not needed") {
    // join - join using
    var df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    var df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")
    withExpectedSkipFlag(true, df.join(df2, "int").queryExecution.analyzed)
    // join using multiple columns
    df = Seq(1, 2, 3).map(i => (i, i + 1, i.toString)).toDF("int", "int2", "str")
    df2 = Seq(1, 2, 3).map(i => (i, i + 1, (i + 1).toString)).toDF("int", "int2", "str")
    withExpectedSkipFlag(true, df.join(df2, Seq("int", "int2")).queryExecution.analyzed)
    // join using multiple columns array
    withExpectedSkipFlag(true, df.join(df2, Array("int", "int2")).queryExecution.analyzed)
    // join with select
    df = Seq((1, 2, "1"), (3, 4, "3")).toDF("int", "int2", "str_sort").as("df1")
    df2 = Seq((1, 3, "1"), (5, 6, "5")).toDF("int", "int2", "str").as("df2")
    withExpectedSkipFlag(true,
      withExpectedSkipFlag(true, {
        val temp = withExpectedSkipFlag(true, {
          val temp = df.join(df2, $"df1.int" === $"df2.int", "outer")
          temp.queryExecution.analyzed
          temp
        }).select($"df1.int", $"df2.int2")
        temp.queryExecution.analyzed
        temp
      }).orderBy($"str_sort".asc, $"str".asc).queryExecution.analyzed)
    // cross join
    var df1 = Seq((1, "1"), (3, "3")).toDF("int", "str")
    df2 = Seq((2, "2"), (4, "4")).toDF("int", "str")
    withExpectedSkipFlag(true, df1.crossJoin(df2).queryExecution.analyzed)

    df1 = Seq((1, "1"), (2, "2")).toDF("key1", "value1")
    df2 = df1.filter($"value1" === "2").select($"key1".as("key2"), $"value1".as("value2"))
    val joinDf = withExpectedSkipFlag(false, df1.join(df2, $"key1" === $"key2").queryExecution.
      analyzed)
    // If this self joined df is joined with a new different df, then skip flag should be true
    var df3 = Seq((1, "1"), (2, "2")).toDF("key3", "value3")
    withExpectedSkipFlag(true, df3.join(joinDf, joinDf("key2") === df3("key3"))
      .queryExecution.analyzed)

  }

  test("joins for which DeduplicateRelations rule is needed") {
    withSQLConf(SQLConf.CROSS_JOINS_ENABLED.key -> "false") {
      val df = spark.range(2)
      withExpectedSkipFlag(false, df.join(df, df("id") <=> df("id")).queryExecution.analyzed)
    }
    val df1 = testData.select(testData("key")).as("df1")
    val df2 = testData.select(testData("key")).as("df2")
    withExpectedSkipFlag(false, df1.join(df2, $"df1.key" === $"df2.key").queryExecution.analyzed)

    var df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    withExpectedSkipFlag(true,
      withExpectedSkipFlag(false, {
        val temp = withExpectedSkipFlag(true, {
            val temp = df.as("x")
            temp.queryExecution.analyzed
            temp
          }).join(
            withExpectedSkipFlag(true, {
              val temp = df.as("y")
              temp.queryExecution.analyzed
              temp
            }), $"x.str" === $"y.str")
        temp.queryExecution.analyzed
        temp
      }).groupBy("x.str").count().queryExecution.analyzed)

    df = Seq((1, "1"), (2, "2")).toDF("key", "value")
    withExpectedSkipFlag(false, df.join(
      withExpectedSkipFlag(true, {
        val temp = df.filter($"value" === "2")
        temp.queryExecution.analyzed
        temp
      }), df("key") === df("key")).queryExecution.analyzed)

    val left = withExpectedSkipFlag(true, {
      val temp = df.groupBy("key").agg(count("*"))
      temp.queryExecution.analyzed
      temp
    })
    val right = withExpectedSkipFlag(true, {
      val temp = df.groupBy("key").agg(sum("key"))
      temp.queryExecution.analyzed
      temp
    })
    withExpectedSkipFlag(false, left.join(right, left("key") === right("key"))
      .queryExecution.analyzed)

    val dfX = spark.range(3)
    val dfY = dfX.filter($"id" > 0)

    withSQLConf(
      SQLConf.FAIL_AMBIGUOUS_SELF_JOIN_ENABLED.key -> "false",
      SQLConf.CROSS_JOINS_ENABLED.key -> "true") {
      withExpectedSkipFlag(false, dfX.join(dfY, dfX("id") > dfY("id")).queryExecution.analyzed)

      // Alias the dataframe and use qualified column names can fix ambiguous self-join.
      val aliasedDfX = dfX.alias("left")
      val aliasedDfY = dfY.as("right")
      withExpectedSkipFlag(false, aliasedDfX.join(aliasedDfY, $"left.id" > $"right.id")
        .queryExecution.analyzed)
    }
  }

  test("unions for which DeduplicateRelations rule is not needed") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")
    withExpectedSkipFlag(true, df.union(df2).queryExecution.analyzed)
    withExpectedSkipFlag(true, df.unionAll(df2).queryExecution.analyzed)
  }

  test("unions for which DeduplicateRelations rule is needed") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = df.select($"int".as("int1"), $"str".as("str1"))
    withExpectedSkipFlag(false, df.union(df2).queryExecution.analyzed)
    withExpectedSkipFlag(false, df.unionAll(df2).queryExecution.analyzed)
    val df3 = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int3", "str3")
    val u = df.union(df2)
    // since u & df3 have no common, the rule should be skipped on further unions
    withExpectedSkipFlag(true, df3.unionAll(u).queryExecution.analyzed)
    // but u & df2 should require skip flag
    withExpectedSkipFlag(false, u.unionAll(df2).queryExecution.analyzed)
  }

  test("intersection for which DeduplicateRelations rule is not needed") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = Seq(1, 2, 3).map(i => (i, (i + 1).toString)).toDF("int", "str")
    withExpectedSkipFlag(true, df.intersect(df2).queryExecution.analyzed)
    withExpectedSkipFlag(true, df.intersectAll(df2).queryExecution.analyzed)
  }

  test("intersection for which DeduplicateRelations rule is needed") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val df2 = df.select($"int".as("int1"), $"str".as("str1"))
    withExpectedSkipFlag(false, df.intersect(df2).queryExecution.analyzed)
    withExpectedSkipFlag(false, df.intersectAll(df2).queryExecution.analyzed)
    val df3 = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int3", "str3")
    val u = df.intersectAll(df2)
    // since u & df3 have no common, the rule should be skipped on further intersections
    withExpectedSkipFlag(true, df3.intersectAll(u).queryExecution.analyzed)
    // but u & df2 should require skip flag
    withExpectedSkipFlag(false, u.intersectAll(df2).queryExecution.analyzed)
  }

  test("filter for which DeduplicateRelations rule is not needed") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    withExpectedSkipFlag(true, df.filter($"int" > 5 ).queryExecution.analyzed)
  }

  test("filter for which DeduplicateRelations rule is needed") {
    withTempView("v1") {
      val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
      Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int1", "str1").createOrReplaceTempView("v1")
      withExpectedSkipFlag(false, df.filter("int In (select int1 from v1)").queryExecution.analyzed)

      val df3 = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int3", "str3")
      val u = df.filter("int In (select int1 from v1)")
      // since u & df3 have no common, the rule should be skipped on further usage
      withExpectedSkipFlag(true, df3.intersectAll(u).queryExecution.analyzed)
      // but u & df should require skip flag
      withExpectedSkipFlag(false, u.intersectAll(df).queryExecution.analyzed)
    }
  }

  test("projection for which DeduplicateRelations rule is not needed") {
    val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    withExpectedSkipFlag(true, df.select(($"int" + 5).as("in1")).queryExecution.analyzed)
  }

  test("projection for which DeduplicateRelations rule is needed") {
    withTempView("v1") {
      val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
      Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int1", "str1").createOrReplaceTempView("v1")
      withExpectedSkipFlag(false, df.selectExpr("int", "(select max(int1) from v1) as maxii").
        queryExecution.analyzed)

      val df3 = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int3", "str3")
      val u = df.selectExpr("int", "(select max(int1) from v1) as maxii")
      // since u & df3 have no common, the rule should be skipped on further usage
      withExpectedSkipFlag(true, df3.intersectAll(u).queryExecution.analyzed)
      // but u & df should require skip flag
      withExpectedSkipFlag(false, u.intersectAll(df).queryExecution.analyzed)
    }
  }

  test("SparkSession.sql(text) api uses DedupRule") {
    withTempView("v1") {
      val df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
      Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int1", "str1").createOrReplaceTempView("v1")
      val df1 = withExpectedSkipFlag(false, {
          val temp = spark.sql("select int1, str1 from v1")
          temp.queryExecution.analyzed
          temp
      })
      val u1 = withExpectedSkipFlag(true, {
        val temp = df1.union(df)
        temp.queryExecution.analyzed
        temp
      })
      withExpectedSkipFlag(false, u1.intersectAll(df).queryExecution.analyzed)
    }
  }

  // This test can be enabled to measure perf  by skipping the rule when not necessary
  ignore("perf difference by skipping dedup relations") {
    var df = Seq(1, 2, 3).map(i => (i, i.toString)).toDF("int", "str")
    val t1 = System.currentTimeMillis
    for (i <- 0 until 3000) {
      df = df.select(($"int" + 1).as("int"))
    }
    val t2 = System.currentTimeMillis
    // scalastyle:off println
    println("time taken = " + (t2 - t1))
    // scalastyle:ofn println
  }

  private def withExpectedSkipFlag[T](skipDedupRuleflag: Boolean, func : => T): T = {
    DedupFlagVerifierRule.expectedSkipFlag.set(Option(skipDedupRuleflag))
    try {
      func
    } finally {
      DedupFlagVerifierRule.expectedSkipFlag.set(None)
    }
  }
}

class CustomRuleTestSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  @transient
  override lazy val sessionState: SessionState = {
    new CustomRuleTestSQLSessionStateBuilder(this, None).build()
  }
}

class CustomRuleTestSQLSessionStateBuilder(session: SparkSession, state: Option[SessionState])
  extends TestSQLSessionStateBuilder(session, state) {
  override def newBuilder: NewBuilder = new CustomRuleTestSQLSessionStateBuilder(_, _)

  override protected def customResolutionRules: Seq[Rule[LogicalPlan]] = {
    super.customResolutionRules :+ DedupFlagVerifierRule
  }
}

object DedupFlagVerifierRule extends Rule[LogicalPlan] {

  val expectedSkipFlag: ThreadLocal[Option[Boolean]] = new ThreadLocal[Option[Boolean]]() {
    override protected def  initialValue(): Option[Boolean] = {
       None
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    expectedSkipFlag.get().foreach(expected =>
    assert(expected == AnalysisContext.get.skipDedupRelations,
      s"expected flag = $expected, actual flag = ${AnalysisContext.get.skipDedupRelations}"))
    plan
  }
}
