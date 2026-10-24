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

package org.apache.spark.sql.execution.datasources.v2

import java.util.Collections

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.{Alias, Ascending, AttributeReference, EqualTo, Literal, Rand, SortOrder}
import org.apache.spark.sql.catalyst.plans.{Cross, Inner, LeftOuter, PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, GlobalLimit, Join, JoinHint, Limit, LocalLimit, LogicalPlan, Offset, Project, Sort, Union}
import org.apache.spark.sql.connector.catalog.{Column, Table, TableCapability}
import org.apache.spark.sql.connector.expressions.{SortOrder => V2SortOrder}
import org.apache.spark.sql.connector.read.{Scan, ScanBuilder, SupportsPushDownLimit, SupportsPushDownOffset, SupportsPushDownTopN}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class V2ScanRelationPushDownSuite extends PlanTest {

  gridTest("standalone local limits are not pushed")(Seq(false, true)) { withProject =>
    val builder = new TestScanBuilder
    val holder = scanBuilderHolder(builder)
    val plan = LocalLimit(Literal(5), projectIfNeeded(holder, withProject))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
    assert(builder.calls.isEmpty)
    assert(holder.pushedLimit.isEmpty)
  }

  gridTest("push local limits below a globally limited union")(
      for {
        withProject <- Seq(false, true)
        (acceptLimit, partiallyPushed) <- Seq((true, false), (true, true), (false, false))
      } yield (withProject, acceptLimit, partiallyPushed)) {
    case (withProject, acceptLimit, partiallyPushed) =>
      val builders = Seq.fill(2)(new TestScanBuilder(acceptLimit, partiallyPushed))
      val holders = builders.map(scanBuilderHolder)
      val children = holders.map { holder =>
        LocalLimit(Literal(5), projectIfNeeded(holder, withProject))
      }
      val plan = Limit(Literal(5), Union(children))

      comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
      assert(builders.forall(_.calls.toSeq == Seq("limit:5")))
      assert(holders.forall(_.pushedLimit == Option.when(acceptLimit)(5)))
      assert(children.forall(_.maxRowsPerPartition.contains(5)))
  }

  gridTest("push local limits below a globally limited conditionless inner join")(
      Seq(Inner, Cross)) { joinType =>
    val leftBuilder = new TestScanBuilder
    val rightBuilder = new TestScanBuilder
    val left = LocalLimit(Literal(5), scanBuilderHolder(leftBuilder))
    val right = LocalLimit(Literal(5), scanBuilderHolder(rightBuilder))
    val plan = Limit(Literal(5), Join(left, right, joinType, None, JoinHint.NONE))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
    assert(leftBuilder.calls.toSeq == Seq("limit:5"))
    assert(rightBuilder.calls.toSeq == Seq("limit:5"))
  }

  gridTest("offset above a limited union does not repeat branch limit attempts")(
      Seq(true, false)) { acceptLimit =>
    val builders = Seq.fill(2)(new TestScanBuilder(acceptLimit, partiallyPushed = true))
    val children = builders.map { builder => LocalLimit(Literal(5), scanBuilderHolder(builder)) }
    val plan = Offset(Literal(1), Limit(Literal(5), Union(children)))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
    assert(builders.forall(_.calls.toSeq == Seq("limit:5")))
  }

  test("branch local limits must match the enclosing global limit") {
    val builder = new TestScanBuilder
    val holder = scanBuilderHolder(builder)
    val plan = Limit(Literal(5), Union(Seq(
      LocalLimit(Literal(4), holder), LocalLimit(Literal(6), holder))))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
    assert(builder.calls.isEmpty)
  }

  gridTest("branch local limits do not cross filters, sorts or nondeterministic projections")(
      Seq("filter", "sort", "nondeterministic project")) { barrier =>
    val builder = new TestScanBuilder
    val holder = scanBuilderHolder(builder)
    val child = barrier match {
      case "filter" => Filter(EqualTo(holder.output.head, Literal(1)), holder)
      case "sort" => Sort(Seq(SortOrder(holder.output.head, Ascending)), global = true, holder)
      case "nondeterministic project" => Project(Seq(Alias(Rand(0), "random")()), holder)
    }
    val plan = Limit(Literal(5), Union(Seq(
      LocalLimit(Literal(5), child), LocalLimit(Literal(5), child))))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
    assert(builder.calls.isEmpty)
  }

  test("branch local limits do not push below joins with conditions or outer joins") {
    Seq(Inner, LeftOuter).foreach { joinType =>
      val leftBuilder = new TestScanBuilder
      val rightBuilder = new TestScanBuilder
      val leftHolder = scanBuilderHolder(leftBuilder)
      val rightHolder = scanBuilderHolder(rightBuilder)
      val condition = if (joinType == Inner) {
        Some(EqualTo(leftHolder.output.head, rightHolder.output.head))
      } else {
        None
      }
      val plan = Limit(Literal(5), Join(
        LocalLimit(Literal(5), leftHolder), LocalLimit(Literal(5), rightHolder),
        joinType, condition, JoinHint.NONE))

      comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
      assert(leftBuilder.calls.isEmpty)
      assert(rightBuilder.calls.isEmpty)
    }
  }

  test("branch local limits do not push a limit after a pushed offset") {
    val builder = new TestScanBuilder
    val holder = scanBuilderHolder(builder)
    holder.pushedOffset = Some(1)
    val plan = Limit(Literal(5), Union(Seq(
      LocalLimit(Literal(5), holder), LocalLimit(Literal(5), holder))))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), plan)
    assert(builder.calls.isEmpty)
  }

  gridTest("combined limit pushdown retains its existing completion behavior")(
      Seq((true, false), (true, true), (false, false))) {
    case (acceptLimit, partiallyPushed) =>
      val builder = new TestScanBuilder(acceptLimit, partiallyPushed)
      val holder = scanBuilderHolder(builder)
      val plan = Limit(Literal(5), holder)
      val expected = if (acceptLimit && !partiallyPushed) holder else plan

      comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), expected)
      assert(builder.calls.toSeq == Seq("limit:5"))
  }

  gridTest("combined top N pushdown retains its existing completion behavior")(
      Seq((true, false), (true, true), (false, false))) {
    case (acceptTopN, partiallyPushed) =>
      val builder = new TestScanBuilder(
        partiallyPushed = partiallyPushed, acceptTopN = acceptTopN)
      val holder = scanBuilderHolder(builder)
      val sort = Sort(Seq(SortOrder(holder.output.head, Ascending)), global = true, holder)
      val plan = Limit(Literal(5), sort)
      val expected = if (acceptTopN && !partiallyPushed) holder else plan

      comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), expected)
      assert(builder.calls.toSeq == Seq("topN:5"))
  }

  test("a rejected combined limit followed by a pushed offset is not retried") {
    val builder = new TestScanBuilder(acceptLimit = false, acceptOffset = true)
    val holder = scanBuilderHolder(builder)
    val plan = GlobalLimit(Literal(2), Offset(Literal(3), LocalLimit(Literal(5), holder)))

    comparePlans(V2ScanRelationPushDown.pushDownLimitAndOffset(plan), Limit(Literal(2), holder))
    assert(builder.calls.toSeq == Seq("limit:5", "offset:3"))
    assert(holder.pushedOffset.contains(3))
  }

  private def projectIfNeeded(holder: ScanBuilderHolder, withProject: Boolean): LogicalPlan = {
    if (withProject) {
      Project(Seq(Alias(holder.output.head, "projected")()), holder)
    } else {
      holder
    }
  }

  private def scanBuilderHolder(builder: TestScanBuilder): ScanBuilderHolder = {
    val table = new Table {
      override def name(): String = "test_table"
      override def columns(): Array[Column] = Array(Column.create("id", IntegerType))
      override def capabilities(): java.util.Set[TableCapability] =
        Collections.singleton(TableCapability.BATCH_READ)
    }
    val output = Seq(AttributeReference("id", IntegerType)())
    val relation = DataSourceV2Relation(
      table, output, None, None, CaseInsensitiveStringMap.empty())
    ScanBuilderHolder(output, relation, builder)
  }

  private class TestScanBuilder(
      acceptLimit: Boolean = true,
      partiallyPushed: Boolean = false,
      acceptOffset: Boolean = false,
      acceptTopN: Boolean = true)
    extends ScanBuilder with SupportsPushDownLimit with SupportsPushDownOffset
      with SupportsPushDownTopN {

    val calls: ArrayBuffer[String] = ArrayBuffer.empty

    override def pushLimit(limit: Int): Boolean = {
      calls += s"limit:$limit"
      acceptLimit
    }

    override def pushOffset(offset: Int): Boolean = {
      calls += s"offset:$offset"
      acceptOffset
    }

    override def pushTopN(orders: Array[V2SortOrder], limit: Int): Boolean = {
      calls += s"topN:$limit"
      acceptTopN
    }

    override def isPartiallyPushed(): Boolean = partiallyPushed

    override def build(): Scan = throw new UnsupportedOperationException("unexpected scan build")
  }
}
