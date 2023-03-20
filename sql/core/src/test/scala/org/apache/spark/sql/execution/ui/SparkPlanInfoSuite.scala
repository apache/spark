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

package org.apache.spark.sql.execution.ui

import scala.collection.mutable

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanInfoSuite extends SharedSparkSession {

  import testImplicits._

  val imrExtractionPattern =
    """InMemoryTableScan\(Repeat Identifier: [0-9]+\)""".r

  def validateSparkPlanInfo(sparkPlanInfo: SparkPlanInfo,
  repeatMapWithChildrenCount: mutable.HashMap[String, Int] = new mutable.HashMap[String, Int]()):
  Unit = {
    sparkPlanInfo.nodeName match {
      case s if sparkPlanInfo.simpleString.startsWith("InMemoryTableScan(Repeat Identifier") =>
        val currentChildrenCount = repeatMapWithChildrenCount.getOrElse(s, 0)
        repeatMapWithChildrenCount.put(sparkPlanInfo.simpleString,
          currentChildrenCount + sparkPlanInfo.children.length)
      case "InMemoryTableScan" => assert(sparkPlanInfo.children.length == 1)
      case _ => sparkPlanInfo.children.foreach(validateSparkPlanInfo(_, repeatMapWithChildrenCount))
    }
    repeatMapWithChildrenCount.foreach {
      case (_, childrenCount) => assert(childrenCount == 1 || childrenCount == 0)
    }
  }

  def extractIMRWithRepeatIdentifier(sparkPlanInfo: SparkPlanInfo): Seq[String] =
    sparkPlanInfo.nodeName match {
      case s if imrExtractionPattern.unapplySeq(s).isDefined =>
        s :: Nil
      case _ => sparkPlanInfo.children.flatMap(extractIMRWithRepeatIdentifier(_))
    }

  test("SparkPlanInfo creation from SparkPlan with InMemoryTableScan node") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache().repartition(10)

    val planInfoResult = SparkPlanInfo.fromSparkPlan(dfWithCache.queryExecution.executedPlan)

    validateSparkPlanInfo(planInfoResult)
  }

  test("SparkPlanInfo containing wrapper plan for InMemoryRelation") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache()

    val planInfoResult = SparkPlanInfo.fromSparkPlan(dfWithCache.queryExecution.executedPlan)
    validateSparkPlanInfo(planInfoResult)
  }

  test("Repeat IMR is skipped") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache()
    val joinedDf = dfWithCache.join(dfWithCache)

    val planInfoResult = SparkPlanInfo.fromSparkPlan(joinedDf.queryExecution.executedPlan)
    validateSparkPlanInfo(planInfoResult)
    // ensure that same IMRs are not generating different Ids
    // id should be incremented by 1 only
    val imrs = extractIMRWithRepeatIdentifier(planInfoResult)
    assert(imrs.toSet.size == 1)
  }

  test("distinct IMR are not skipped ") {
    val dfWithCache1 = Seq(
      (1, 1),
      (2, 2)
    ).toDF("a", "b").filter("a > 1").cache()

    val dfWithCache2 = Seq(
      (1, 1),
      (2, 2),
      (3, 2)
    ).toDF("x", "y").filter("y > 1").cache()

    val joinedDf = dfWithCache1.join(dfWithCache2)
    val planInfoResult = SparkPlanInfo.fromSparkPlan(joinedDf.queryExecution.executedPlan)
    validateSparkPlanInfo(planInfoResult)
    // ensure that same IMRs are  generating different Ids
    // id should be incremented by 2 only
    val imrs = extractIMRWithRepeatIdentifier(planInfoResult)
    assert(imrs.toSet.size == 2)
  }
}
