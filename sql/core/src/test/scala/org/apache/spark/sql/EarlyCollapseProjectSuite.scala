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

import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.analysis.EarlyCollapseProject
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession


class EarlyCollapseProjectSuite extends QueryTest
  with SharedSparkSession with AdaptiveSparkPlanHelper {
  import testImplicits._
  val useCaching: Boolean = false

  test("withColumns: check no new project addition for simple columns addition") {
    val baseDfCreator = () => spark.range(20).select($"id" as "a", $"id" as "b")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumns(Seq("newCol1", "newCol2"), Seq(col("a") + 1, col("b") + 2)),
      (1, 2), (1, 1))
  }

  test("withColumns: check no new project addition if redefined alias is not used in" +
    " new columns") {
    val baseDfCreator = () => spark.range(20).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "a", $"b")

    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumns(Seq("newCol1"), Seq(col("b") + 2)), (1, 2), (1, 1))
  }

  test("withColumns: no new project addition if redefined alias is used in new columns - 1") {
    val baseDfCreator = () => spark.range(20).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "a", $"b")

    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumns(Seq("newCol1"), Seq(col("a") + 2)), (1, 2), (1, 1))
  }

  test("withColumns: no new project addition if redefined alias is used in new columns - 2") {
    val baseDfCreator = () => spark.range(20).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumns(Seq("newCol1"), Seq(col("c") + 2 + col("a") * col("e"))), (1, 2), (1, 1))
  }

  test("withColumnRenamed: remap of column should not result in new project if the source" +
    " of remap is not used in other cols") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumnRenamed("a", "c"), (1, 1), (0, 0))
  }

  test("withColumnRenamed: remap of column should not result in new project if the source" +
    " of remap is an attribute used in other cols") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumnRenamed("a", "d"), (1, 1), (0, 0))
  }


  test("withColumnRenamed: remap of column should not result in new project if the remap" +
    " is on an alias") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d" )
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.withColumnRenamed("d", "x"),
      (1, 1), (0, 0))
  }

  test("withColumnRenamed: remap of column should not  result in new project if the remap" +
    " source an alias and that attribute is also projected as another attribute") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d").
      select($"c", $"a", $"b", $"d", $"d" as "k")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.withColumnRenamed("d", "x"),
      (1, 1), (0, 0))
  }


  test("withColumnRenamed: test multi column remap") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumnsRenamed(Map("d" -> "x", "c" -> "k", "a" -> "u")), (1, 1), (0, 0))
  }

  test("withColumns: test multi column addition") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumns(
        Seq("newCol1", "newCol2", "newCol3", "newCol4"),
        Seq(col("a") + 2, col("b") + 7, col("a") + col("b"), col("a") + col("d"))), (1, 2), (1, 1))
  }

  test("mix of column addition, rename and dropping") {
    val baseDfCreator = () => spark.range(100).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.select($"a" + $"d" as "newCol1", $"b" * $"a" as "newCol2",
        $"a" as "renameCola", $"c" * $"d" as "c", $"a"), (1, 2), (1, 1))
  }


  test("mix of column addition, rename and dropping - 1") {
    val baseDfCreator = () => spark.range(100).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.select($"c" * $"d" as "c", $"a" + $"d" as "newCol1", $"b" * $"a" as "newCol2",
        $"a" as "renameCola", $"a"), (1, 2), (1, 1))
  }

  test("mix of column addition, rename and dropping - 2") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" + 5 as "b").
      select($"a" + $"b" as "c", $"a", $"b").select($"c", $"a", $"b", $"c" * $"a" * $"b" as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.select($"d", $"b" as "renameB", $"a" as "renameA", $"a" as "renameColA"),
      (1, 2), (1, 1))
  }


  test("mix of column addition, rename and dropping - 3") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" + 5 as "b").
      select($"a" + $"b" as "c", $"a", $"b").select($"c", $"a", $"b", $"c" * $"a" * $"b" as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.select($"d" * $"a" as "d", $"b" as "renameB", $"a" * $"d" as "renameA",
      $"a" as "renameColA"), (1, 2), (1, 1))
  }


  test("mix of column addition, rename and dropping - 4") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" + 5 as "b").
      select($"a" + $"b" as "c", $"a", $"b").select($"c", $"a", $"b", $"c" * $"a" * $"b" as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.select($"c"),
      (1, 2), (0, 1))
  }


  test("mix of column addition, rename and dropping - 5") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" + 5 as "b").
      select($"a" + $"b" as "c", $"a", $"b").select($"c", $"a", $"b", $"c" * $"a" * $"b" as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.select($"d" * 7 as "a"),
      (1, 2), (0, 1))
  }


  test("mix of column addition, rename and dropping - 6") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" + 5 as "b").
      select($"a" + $"b" as "c", $"a", $"b").select($"c", $"a", $"b", $"c" * $"a" * $"b" as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.select($"d" * 7 as "a", $"d" * 7 as "b", $"b" + $"a" as "e"), (1, 2), (0, 1))
  }

  test("mix of column addition, rename and dropping - 7") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" + 5 as "b").
      select($"a" + $"b" as "c", $"a", $"b").select( lit(9) as "e", $"c", lit(11) as "a", $"b",
      $"c" * $"a" * $"b" as "d")
    checkProjectCollapseCacheUseAndInvalidation(
      baseDfCreator,
      df => df.select($"a" as "a1", lit(7)  as "d1", $"b" as "b1", $"c" * $"a" as "c",
        lit(13) as "f"), (1, 2), (0, 1))
  }

  test("new columns added do not result in new project -1") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.withColumns(
        Seq("newCol1", "newCol2", "newCol3", "newCol4"),
        Seq(col("a") + 2, col("b") + 7, col("a") + col("b"), col("a") + col("d"))),
      (1, 2), (1, 1))
  }

  test("new columns added do not result in new project -2") {
    val baseDfCreator = () => spark.range(20).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.withColumns(Seq("newCol1"), Seq(col("c") + 2 + col("a") * col("e"))),
      (1, 2), (1, 1))
  }

  test("new columns added do not result in new project, with positions changed") {
    val baseDfCreator = () => spark.range(20).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e")
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator,
      df => df.select( $"e", $"a", $"c" + 2 + $"a" * $"e" as "newCol", $"c", $"d", $"b"),
      (1, 2), (1, 1))
  }


  test("renamed columns do not result in new project") {
    val baseDfCreator = () => spark.range(10).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").select($"c", $"a", $"b", $"c" + 7 as "d")

    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.withColumnsRenamed(
      Map("c" -> "c1", "a" -> "a1", "b" -> "b1", "d" -> "d1")), (1, 1), (0, 0))
  }

  test("early collapse of filter chain with project - 1") {
    val baseDfCreator = () => spark.range(100).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b")

    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.filter($"a" > 4).
      filter($"c" * $"b" < 60).
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e"),
      (1, 2), (0, 1))
  }

  test("early collapse of filter chain with project - 2") {
    val baseDfCreator = () => spark.range(100).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"a", $"b").filter($"a" > 4).filter($"c" * $"b" < 60)

    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.filter($"b" < 100).
      select($"c" + $"a" as "c", $"a" + 3 as "a", $"b", $"c" + 7 as "d", $"a" - $"b" as "e"),
      (1, 2), (0, 1))
  }

  test("resurrection of intermediate dropped cols when used in filter") {
    val baseDfCreator = () => spark.range(100).select($"id" as "a", $"id" as "b").
      select($"a" + 1 as "c", $"b").select($"c", $"b", $"c" + 7 as "d")
    // A dropped column would result in a new project being added on top of filter
    // so we have to take into account of that extra project added while checking
    // assertion of init node size and optimized df nodes size
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.withColumnsRenamed(
      Map("c" -> "c1", "b" -> "b1", "d" -> "d1")).filter($"a" > 5), (1, 2), (0, 1))
  }

  test("resurrection of right renamed intermediate dropped cols when used in filter") {
    val baseDfCreator = () => spark.range(100).select($"id" + 7 as "a", $"id" as "b").
      select($"a" + 1 as "c", $"b", $"a" * $"b" as "a").select($"c", $"b", $"c" + 7 as "d")
    // A dropped column would result in a new project being added on top of filter
    // so we have to take into account of that extra project added while checking
    // assertion of init node size and optimized df nodes size
    checkProjectCollapseCacheUseAndInvalidation(baseDfCreator, df => df.withColumnsRenamed(
      Map("c" -> "c1", "b" -> "b1", "d" -> "d1")).select($"c1", $"d1").filter($"a" > 25),
      (1, 2), (0, 1))
  }

  protected def checkProjectCollapseCacheUseAndInvalidation(
      baseDfCreator: () => DataFrame,
      testExec: DataFrame => DataFrame,
      baseAndDerivedIMRsOnCache: (Int, Int),
      baseAndDerivedIMRsOnCBaseInvalidation: (Int, Int)): Unit = {
    val baseDf = baseDfCreator()
    val baseDfRows = baseDf.collect()
    val testDfRows = testExec(baseDf).collect()
    if (useCaching) {
      baseDf.cache()
    }
    val initNodes = collectNodes(baseDf)
    val (newDfOpt, newDfUnopt) = getComparableDataFrames(baseDf, testExec)
    val optDfNodes = collectNodes(newDfOpt)
    val nonOptDfNodes = collectNodes(newDfUnopt)
    val foundFilterNodes = optDfNodes.exists(_.isInstanceOf[Filter])
    if (!foundFilterNodes) {
      assert(initNodes.size === optDfNodes.size)
    }
    assert(nonOptDfNodes.size > optDfNodes.size)
    checkAnswer(newDfOpt, newDfUnopt)
    if (useCaching) {
      assert(newDfOpt.queryExecution.optimizedPlan.collectLeaves().head.
        isInstanceOf[InMemoryRelation])
    }
    // now check if the results of optimized dataframe and completely unoptimized dataframe are same
    val fullyUnopt = withSQLConf(
      SQLConf.EXCLUDE_POST_ANALYSIS_RULES.key -> EarlyCollapseProject.ruleName) {
       testExec(baseDfCreator())
    }
    assert(collectNodes(fullyUnopt).size >= nonOptDfNodes.size)
    checkAnswer(newDfOpt, fullyUnopt)

    if (useCaching) {
      // first unpersist both dataframes
      baseDf.unpersist(true)
      newDfOpt.unpersist(true)
      baseDf.cache()
      newDfOpt.cache()
      assertCacheDependency(baseDfCreator(), baseAndDerivedIMRsOnCache._1)
      assertCacheDependency(testExec(baseDfCreator()), baseAndDerivedIMRsOnCache._2)
      checkAnswer(baseDfCreator(), baseDfRows)
      checkAnswer(testExec(baseDfCreator()), testDfRows)
      baseDf.unpersist(true)
      newDfOpt.unpersist(true)
      baseDfCreator().cache()
      testExec(baseDfCreator()).cache()
      baseDfCreator().unpersist(true)
      assertCacheDependency(baseDfCreator(), baseAndDerivedIMRsOnCBaseInvalidation._1)
      assertCacheDependency(testExec(baseDfCreator()), baseAndDerivedIMRsOnCBaseInvalidation._2)
      checkAnswer(baseDfCreator(), baseDfRows)
      checkAnswer(testExec(baseDfCreator()), testDfRows)
      // recache base df so that if existing tests want to continue should work fine
      newDfOpt.unpersist(true)
      baseDfCreator().cache()
    }
  }

  private def getComparableDataFrames(
      baseDf: DataFrame,
      transformation: DataFrame => DataFrame): (DataFrame, DataFrame) = {
    // first obtain optimized transformation which avoids adding new project
    val newDfOpt = transformation(baseDf)
    // then obtain optimized transformation which adds new project

    val newDfUnopt = withSQLConf(
      SQLConf.EXCLUDE_POST_ANALYSIS_RULES.key -> EarlyCollapseProject.ruleName) {
      transformation(baseDf)
    }
    (newDfOpt, newDfUnopt)
  }

  private def collectNodes(df: DataFrame): Seq[LogicalPlan] = df.logicalPlan.collect {
    case l => l
  }

  def assertCacheDependency(df: DataFrame, numOfCachesExpected: Int): Unit = {

    val cachedPlans = df.queryExecution.withCachedData.collect {
      case i: InMemoryRelation => i.cacheBuilder.cachedPlan
    }
    val totalIMRs = cachedPlans.size + cachedPlans.map(ime => recurse(ime)).sum
    assert(totalIMRs == numOfCachesExpected)
  }

  private def recurse(sparkPlan: SparkPlan): Int = {
    val imrs = sparkPlan.collect {
      case i: InMemoryTableScanExec => i
    }
    imrs.size + imrs.map(ime => recurse(ime.relation.cacheBuilder.cachedPlan)).sum
  }

}

