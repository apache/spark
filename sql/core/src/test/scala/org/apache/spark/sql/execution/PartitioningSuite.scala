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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, Attribute, AttributeReference, Expression, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning, PartitioningCollection, RangePartitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, ObjectHashAggregateExec, SortAggregateExec}
import org.apache.spark.sql.types.IntegerType

class PartitioningSuite extends SparkFunSuite {

  private val attr1 = AttributeReference("attr1", IntegerType)()
  private val attr2 = AttributeReference("attr2", IntegerType)()
  private val aliasedAttr1 = Alias(attr1, "alias_attr1")()
  private val aliasedAttr2 = Alias(attr2, "alias_attr2")()
  private val aliasedAttr1Twice = Alias(Alias(attr1, "alias_attr1")(), "alias_attr1_2")()

  private val planHashPartitioned1Attr = PartitionedSparkPlan(
    output = Seq(attr1), outputPartitioning = HashPartitioning(Seq(attr1), 10))
  private val planHashPartitioned2Attr = PartitionedSparkPlan(
    output = Seq(attr1, attr2), outputPartitioning = HashPartitioning(Seq(attr1, attr2), 10))
  private val planRangePartitioned1Attr = PartitionedSparkPlan(
    output = Seq(attr1), outputPartitioning = simpleRangePartitioning(Seq(attr1), 10))
  private val planRangePartitioned2Attr = PartitionedSparkPlan(
    output = Seq(attr1, attr2),
    outputPartitioning = simpleRangePartitioning(Seq(attr1, attr2), 10))

  def testPartitioning(
      outputExpressions: Seq[NamedExpression],
      inputPlan: SparkPlan,
      expectedPartitioning: Partitioning): Unit = {
    testProjectPartitioning(outputExpressions, inputPlan, expectedPartitioning)
    testAggregatePartitioning(outputExpressions, inputPlan, expectedPartitioning)
  }

  def testProjectPartitioning(
      projectList: Seq[NamedExpression],
      inputPlan: SparkPlan,
      expectedPartitioning: Partitioning): Unit = {
    assert(ProjectExec(projectList, inputPlan).outputPartitioning == expectedPartitioning)
  }

  def testAggregatePartitioning(
      groupingExprs: Seq[NamedExpression],
      inputPlan: SparkPlan,
      expectedPartitioning: Partitioning): Unit = {
    val hashAgg = HashAggregateExec(requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExprs,
      aggregateExpressions = Seq.empty,
      aggregateAttributes = Seq.empty,
      initialInputBufferOffset = 0,
      resultExpressions = groupingExprs,
      child = inputPlan)
    val sortAgg = SortAggregateExec(requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExprs,
      aggregateExpressions = Seq.empty,
      aggregateAttributes = Seq.empty,
      initialInputBufferOffset = 0,
      resultExpressions = groupingExprs,
      child = inputPlan)
    val objAgg = ObjectHashAggregateExec(requiredChildDistributionExpressions = None,
      groupingExpressions = groupingExprs,
      aggregateExpressions = Seq.empty,
      aggregateAttributes = Seq.empty,
      initialInputBufferOffset = 0,
      resultExpressions = groupingExprs,
      child = inputPlan)
    assert(hashAgg.outputPartitioning == expectedPartitioning)
    assert(sortAgg.outputPartitioning == expectedPartitioning)
    assert(objAgg.outputPartitioning == expectedPartitioning)
  }

  def simpleRangePartitioning(exprs: Seq[Expression], numPartitions: Int): RangePartitioning = {
    RangePartitioning(exprs.map(e => SortOrder(e, Ascending)), numPartitions)
  }

  test("HashPartitioning with simple attribute rename") {
    testPartitioning(
      Seq(aliasedAttr1),
      planHashPartitioned1Attr,
      HashPartitioning(Seq(aliasedAttr1.toAttribute), 10))
    testPartitioning(
      Seq(aliasedAttr1Twice),
      planHashPartitioned1Attr,
      HashPartitioning(Seq(aliasedAttr1Twice.toAttribute), 10))

    testPartitioning(
      Seq(aliasedAttr1, attr2),
      planHashPartitioned2Attr,
      HashPartitioning(Seq(aliasedAttr1.toAttribute, attr2), 10))
    testPartitioning(
      Seq(aliasedAttr1Twice, attr2),
      planHashPartitioned2Attr,
      HashPartitioning(Seq(aliasedAttr1Twice.toAttribute, attr2), 10))

    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr2),
      planHashPartitioned2Attr,
      HashPartitioning(Seq(aliasedAttr1.toAttribute, aliasedAttr2.toAttribute), 10))
    testPartitioning(
      Seq(aliasedAttr1Twice, aliasedAttr2),
      planHashPartitioned2Attr,
      HashPartitioning(Seq(aliasedAttr1Twice.toAttribute, aliasedAttr2.toAttribute), 10))
  }

  test("HashPartitioning with double attribute rename") {
    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr1Twice),
      planHashPartitioned1Attr,
      PartitioningCollection(Seq(
        HashPartitioning(Seq(aliasedAttr1.toAttribute), 10),
        HashPartitioning(Seq(aliasedAttr1Twice.toAttribute), 10))))
    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr1Twice, attr2),
      planHashPartitioned2Attr,
      PartitioningCollection(Seq(
        HashPartitioning(Seq(aliasedAttr1.toAttribute, attr2), 10),
        HashPartitioning(Seq(aliasedAttr1Twice.toAttribute, attr2), 10))))
    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr1Twice, attr2, aliasedAttr2),
      planHashPartitioned2Attr,
      PartitioningCollection(Seq(
        HashPartitioning(Seq(aliasedAttr1.toAttribute, attr2), 10),
        HashPartitioning(Seq(aliasedAttr1.toAttribute, aliasedAttr2.toAttribute), 10),
        HashPartitioning(Seq(aliasedAttr1Twice.toAttribute, attr2), 10),
        HashPartitioning(Seq(aliasedAttr1Twice.toAttribute, aliasedAttr2.toAttribute), 10))))
  }

  test("HashPartitioning without attribute in output") {
    testPartitioning(
      Seq(attr2),
      planHashPartitioned1Attr,
      UnknownPartitioning(10))
    testPartitioning(
      Seq(attr1),
      planHashPartitioned2Attr,
      UnknownPartitioning(10))
  }

  test("HashPartitioning without renaming") {
    testPartitioning(
      Seq(attr1),
      planHashPartitioned1Attr,
      HashPartitioning(Seq(attr1), 10))
    testPartitioning(
      Seq(attr1, attr2),
      planHashPartitioned2Attr,
      HashPartitioning(Seq(attr1, attr2), 10))
  }

  test("RangePartitioning with simple attribute rename") {
    testPartitioning(
      Seq(aliasedAttr1),
      planRangePartitioned1Attr,
      simpleRangePartitioning(Seq(aliasedAttr1.toAttribute), 10))
    testPartitioning(
      Seq(aliasedAttr1Twice),
      planRangePartitioned1Attr,
      simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute), 10))

    testPartitioning(
      Seq(aliasedAttr1, attr2),
      planRangePartitioned2Attr,
      simpleRangePartitioning(Seq(aliasedAttr1.toAttribute, attr2), 10))
    testPartitioning(
      Seq(aliasedAttr1Twice, attr2),
      planRangePartitioned2Attr,
      simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute, attr2), 10))

    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr2),
      planRangePartitioned2Attr,
      simpleRangePartitioning(Seq(aliasedAttr1.toAttribute, aliasedAttr2.toAttribute), 10))
    testPartitioning(
      Seq(aliasedAttr1Twice, aliasedAttr2),
      planRangePartitioned2Attr,
      simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute, aliasedAttr2.toAttribute), 10))
  }

  test("RangePartitioning with double attribute rename") {
    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr1Twice),
      planRangePartitioned1Attr,
      PartitioningCollection(Seq(
        simpleRangePartitioning(Seq(aliasedAttr1.toAttribute), 10),
        simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute), 10))))
    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr1Twice, attr2),
      planRangePartitioned2Attr,
      PartitioningCollection(Seq(
        simpleRangePartitioning(Seq(aliasedAttr1.toAttribute, attr2), 10),
        simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute, attr2), 10))))
    testPartitioning(
      Seq(aliasedAttr1, aliasedAttr1Twice, attr2, aliasedAttr2),
      planRangePartitioned2Attr,
      PartitioningCollection(Seq(
        simpleRangePartitioning(Seq(aliasedAttr1.toAttribute, attr2), 10),
        simpleRangePartitioning(Seq(aliasedAttr1.toAttribute, aliasedAttr2.toAttribute), 10),
        simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute, attr2), 10),
        simpleRangePartitioning(Seq(aliasedAttr1Twice.toAttribute, aliasedAttr2.toAttribute), 10))))
  }

  test("RangePartitioning without attribute in output") {
    testPartitioning(
      Seq(attr2),
      planRangePartitioned2Attr,
      UnknownPartitioning(10))
    testPartitioning(
      Seq(attr1),
      planRangePartitioned2Attr,
      simpleRangePartitioning(Seq(attr1), 10))
  }

  test("RangePartitioning without renaming") {
    testPartitioning(
      Seq(attr1),
      planRangePartitioned1Attr,
      simpleRangePartitioning(Seq(attr1), 10))
    testPartitioning(
      Seq(attr1, attr2),
      planRangePartitioned2Attr,
      simpleRangePartitioning(Seq(attr1, attr2), 10))
  }
}

private case class PartitionedSparkPlan(
    override val output: Seq[Attribute] = Seq.empty,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val children: Seq[SparkPlan] = Nil) extends SparkPlan {
  override protected def doExecute() = throw new UnsupportedOperationException
}
