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
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.{InternalRow, SimpleCatalystConf}
import org.apache.spark.sql.catalyst.analysis.{Analyzer, EmptyFunctionRegistry}
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, If, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.{CollectSet, Count, ImperativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Expand, LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

class RewriteDistinctAggregatesSuite extends PlanTest {
  val conf = SimpleCatalystConf(caseSensitiveAnalysis = false, groupByOrdinal = false)
  val catalog = new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, conf)
  val analyzer = new Analyzer(catalog, conf)

  val nullInt = Literal(null, IntegerType)
  val nullString = Literal(null, StringType)
  val testRelation = LocalRelation('a.string, 'b.string, 'c.string, 'd.string, 'e.int)

  private def checkRewrite(rewrite: LogicalPlan): Unit = rewrite match {
    case Aggregate(_, _, Aggregate(_, _, _: Expand)) =>
    case _ => fail(s"Plan is not rewritten:\n$rewrite")
  }

  test("single distinct group") {
    val input = testRelation
      .groupBy('a)(countDistinct('e))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("single distinct group with partial aggregates") {
    val input = testRelation
      .groupBy('a, 'd)(
        countDistinct('e, 'c).as('agg1),
        max('b).as('agg2))
      .analyze
    val rewrite = RewriteDistinctAggregates(input)
    comparePlans(input, rewrite)
  }

  test("single distinct group with non-partial aggregates") {
    val input = testRelation
      .groupBy('a, 'd)(
        countDistinct('e, 'c).as('agg1),
        DummpAgg('b).toAggregateExpression().as('agg2))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups") {
    val input = testRelation
      .groupBy('a)(countDistinct('b, 'c), countDistinct('d))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with partial aggregates") {
    val input = testRelation
      .groupBy('a)(countDistinct('b, 'c), countDistinct('d), sum('e))
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }

  test("multiple distinct groups with non-partial aggregates") {
    val input = testRelation
      .groupBy('a)(
        countDistinct('b, 'c),
        countDistinct('d),
        CollectSet('b).toAggregateExpression())
      .analyze
    checkRewrite(RewriteDistinctAggregates(input))
  }
}

// A dummy aggregate function for testing
case class DummpAgg(
    child: Expression,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Int] {

  def this(child: Expression) = this(child, 0, 0)

  override def children: Seq[Expression] = child :: Nil

  override def dataType: DataType = IntegerType

  override def nullable: Boolean = false

  override val supportsPartial: Boolean = false

  override def createAggregationBuffer(): Int = 0

  override def update(buffer: Int, input: InternalRow): Int = 0

  override def merge(buffer: Int, input: Int): Int = 0

  override def eval(buffer: Int): Any = 0

  override def serialize(buffer: Int): Array[Byte] = null

  override def deserialize(storageFormat: Array[Byte]): Int = 0

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override val prettyName: String = "dummy_agg"
}
