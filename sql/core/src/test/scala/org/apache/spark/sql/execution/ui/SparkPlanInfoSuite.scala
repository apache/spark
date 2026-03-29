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

import org.apache.spark.sql.execution.{FormattedMode, SparkPlanInfo}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanInfoSuite extends SharedSparkSession {

  import testImplicits._

  def validateSparkPlanInfo(sparkPlanInfo: SparkPlanInfo): Unit = {
    sparkPlanInfo.nodeName match {
      case "InMemoryTableScan" => assert(sparkPlanInfo.children.length == 1)
      case _ => sparkPlanInfo.children.foreach(validateSparkPlanInfo)
    }
  }

  test("SparkPlanInfo creation from SparkPlan with InMemoryTableScan node") {
    val dfWithCache = Seq(
      (1, 1),
      (2, 2)
    ).toDF().filter("_1 > 1").cache().repartition(10)

    val planInfoResult = SparkPlanInfo.fromSparkPlan(dfWithCache.queryExecution.executedPlan)

    validateSparkPlanInfo(planInfoResult)
  }

  test("SparkPlanInfo and plan graph include subtree under EmptyRelation") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
      testData
      val df = spark.sql("SELECT key FROM testData WHERE key = 0 ORDER BY key, value")
      df.collect()
      val planInfo = SparkPlanInfo.fromSparkPlan(df.queryExecution.executedPlan)

      def findNamed(info: SparkPlanInfo, name: String): Option[SparkPlanInfo] = {
        if (info.nodeName == name) Some(info)
        else info.children.view.flatMap(findNamed(_, name)).headOption
      }
      val emptyInfo = findNamed(planInfo, "EmptyRelation")
      assert(emptyInfo.isDefined, s"expected EmptyRelation in plan info: $planInfo")
      assert(
        emptyInfo.get.children.nonEmpty,
        "EmptyRelation SparkPlanInfo should carry the preserved logical plan as children")

      val graph = SparkPlanGraph(planInfo)
      val emptyNode = graph.allNodes.find(_.name == "EmptyRelation")
      assert(
        emptyNode.isDefined,
        s"expected EmptyRelation graph node: ${graph.allNodes.map(_.name)}")
      val childIds = graph.edges.collect { case e if e.toId == emptyNode.get.id => e.fromId }
      assert(
        childIds.nonEmpty,
        "plan graph should have edges from nested nodes to EmptyRelation")

      val dot = graph.makeDotFile(Map.empty)
      assert(
        childIds.forall(cid => dot.contains(s"$cid->${emptyNode.get.id}")),
        s"DOT should connect preserved-plan nodes to EmptyRelation: $dot")
    }
  }

  test("formatted explain matches SQL UI plan text: no (unknown) under EmptyRelationExec") {
    withSQLConf(
      SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
      SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1",
      SQLConf.UI_EXPLAIN_MODE.key -> "formatted") {
      testData
      val df = spark.sql("SELECT key FROM testData WHERE key = 0 ORDER BY key, value")
      df.collect()
      val explained = df.queryExecution.explainString(FormattedMode)
      assert(
        !explained.contains("unknown"),
        s"Logical subtree under EmptyRelationExec must not use missing operator ids:\n$explained")
      assert(
        explained.contains("EmptyRelation"),
        s"expected EmptyRelation in formatted physical plan:\n$explained")
    }
  }
}
