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

package org.apache.spark.sql.execution

import org.scalatest.Matchers._

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, ExprId}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.columnar._
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.test.SharedSparkSession

class SparkPlanInfoSuite extends SparkFunSuite with SharedSparkSession {

  test ("Generating SparkPlanInfo from plan gives expected SparkPlanInfo") {
    val plan = NamedDummyLeafNode("testPlan")
    val planInfo = SparkPlanInfo.fromSparkPlan(plan)

    planInfo.nodeName shouldBe "testPlan"
  }

  test ("Generating SparkPlanInfo from plan with SparkPlan child gives expected SparkPlanInfo") {
    val childPlan = NamedDummyLeafNode("testChild")
    val parentPlan = NamedDummyUnaryNode("testParent", childPlan)
    val planInfo = SparkPlanInfo.fromSparkPlan(parentPlan)

    planInfo.allNodeNames should contain inOrderOnly ("testParent", "testChild")
  }

  test("Generating SparkPlanInfo from InMemoryTableScan gives InMemoryRelation child," +
    " cachedPlan grandchild") {
    val cachedPlan = NamedDummyLeafNode("testChild")
    val inMemoryTableScan = createInMemoryTableScan(cachedPlan)
    val inMemoryTableScanInfo = SparkPlanInfo.fromSparkPlan(inMemoryTableScan)

    inMemoryTableScanInfo.allNodeNames should contain inOrderOnly
      ("InMemoryTableScan", "InMemoryRelation", "testChild")
  }

  test("Generating SparkPlanInfo from nested InMemoryTableScan gives nested correct children") {
    val cachedPlan = NamedDummyLeafNode("testChild")
    val inMemoryTableScanChild = createInMemoryTableScan(cachedPlan)
    val inMemoryTableScanParent = createInMemoryTableScan(inMemoryTableScanChild)
    val inMemoryTableScanInfo = SparkPlanInfo.fromSparkPlan(inMemoryTableScanParent)

    inMemoryTableScanInfo.allNodeNames should contain theSameElementsInOrderAs Seq(
      "InMemoryTableScan", "InMemoryRelation", "InMemoryTableScan", "InMemoryRelation", "testChild")
  }

  test("Generating SparkPlanInfo from InMemoryTableScan with subqueries gives subqueries and" +
    " cachedPlan children") {
    val subqueryExpression = ScalarSubquery(new SubqueryExec("testSubquery",
      NamedDummyLeafNode("testSubqueryChild")), ExprId(0))
    val cachedPlan = NamedDummyLeafNode("testChild")
    val inMemoryTableScan = createInMemoryTableScan(cachedPlan, Seq(subqueryExpression))
    val inMemoryTableScanInfo = SparkPlanInfo.fromSparkPlan(inMemoryTableScan)

    inMemoryTableScanInfo.nodeName shouldBe "InMemoryTableScan"
    inMemoryTableScanInfo.children.map(_.nodeName) should contain only
      ("InMemoryRelation", "Subquery")
  }

  test("Generating SparkPlanInfo from child InMemoryRelation correctly propagates metadata and" +
    " metrics for InMemoryRelation") {
    val scan = createInMemoryTableScan(NamedDummyLeafNode("testCachedPlan"))
    val relation = scan.relation

    val scanInfo = SparkPlanInfo.fromSparkPlan(scan)
    val relationInfo = scanInfo.children.head

    relation.metrics.foreach { case (key, metric) =>
      relationInfo.metrics should contain
        new SQLMetricInfo(metric.name.getOrElse(key), metric.id, metric.metricType)
    }
    relationInfo.metadata shouldBe relation.metadata
  }

  private implicit class SparkPlanInfoAdditions(info: SparkPlanInfo) {
    def allNodeNames: Seq[String] = allNodes(info).map(_.nodeName)

    private def allNodes(info: SparkPlanInfo): Seq[SparkPlanInfo] =
      info +: info.children.flatMap(allNodes)
  }

  private def createInMemoryTableScan(cachedPlan: SparkPlan,
                                      predicates: Seq[Expression] = Nil): InMemoryTableScanExec = {
    val cachedRDDBuilder = CachedRDDBuilder(useCompression = false, 0, null, cachedPlan, None)
    val inMemoryRelation = new InMemoryRelation(Nil, cachedRDDBuilder, Nil)
    InMemoryTableScanExec(Nil, predicates, inMemoryRelation)
  }
}

private case class NamedDummyLeafNode(override val nodeName: String) extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new NotImplementedError
  override def output: Seq[Attribute] = Seq.empty
}

private case class NamedDummyUnaryNode(override val nodeName: String, child: SparkPlan)
  extends UnaryExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new NotImplementedError
  override def output: Seq[Attribute] = Seq.empty
}
