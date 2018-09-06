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

package org.apache.spark.sql.catalyst.analysis

import java.util.Locale

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, MonotonicallyIncreasingID, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{FlatMapGroupsWithState, _}
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder}
import org.apache.spark.unsafe.types.CalendarInterval

/** A dummy command for testing unsupported operations. */
case class DummyCommand() extends Command

class UnsupportedOperationsSuite extends SparkFunSuite {

  val attribute = AttributeReference("a", IntegerType, nullable = true)()
  val watermarkMetadata = new MetadataBuilder()
    .withMetadata(attribute.metadata)
    .putLong(EventTimeWatermark.delayKey, 1000L)
    .build()
  val attributeWithWatermark = attribute.withMetadata(watermarkMetadata)
  val batchRelation = LocalRelation(attribute)
  val streamRelation = new TestStreamingRelation(attribute)

  /*
    =======================================================================================
                                     BATCH QUERIES
    =======================================================================================
   */

  assertSupportedInBatchPlan("local relation", batchRelation)

  assertNotSupportedInBatchPlan(
    "streaming source",
    streamRelation,
    Seq("with streaming source", "start"))

  assertNotSupportedInBatchPlan(
    "select on streaming source",
    streamRelation.select($"count(*)"),
    Seq("with streaming source", "start"))


  /*
    =======================================================================================
                                     STREAMING QUERIES
    =======================================================================================
   */

  // Batch plan in streaming query
  testError(
    "streaming plan - no streaming source",
    Seq("without streaming source", "start")) {
    UnsupportedOperationChecker.checkForStreaming(batchRelation.select($"count(*)"), Append)
  }

  // Commands
  assertNotSupportedInStreamingPlan(
    "commmands",
    DummyCommand(),
    outputMode = Append,
    expectedMsgs = "commands" :: Nil)

  // Aggregation: Multiple streaming aggregations not supported
  def aggExprs(name: String): Seq[NamedExpression] = Seq(Count("*").as(name))

  assertSupportedInStreamingPlan(
    "aggregate - multiple batch aggregations",
    Aggregate(Nil, aggExprs("c"), Aggregate(Nil, aggExprs("d"), batchRelation)),
    Append)

  assertSupportedInStreamingPlan(
    "aggregate - multiple aggregations but only one streaming aggregation",
    Aggregate(Nil, aggExprs("c"), batchRelation).join(
      Aggregate(Nil, aggExprs("d"), streamRelation), joinType = Inner),
    Update)

  assertNotSupportedInStreamingPlan(
    "aggregate - multiple streaming aggregations",
    Aggregate(Nil, aggExprs("c"), Aggregate(Nil, aggExprs("d"), streamRelation)),
    outputMode = Update,
    expectedMsgs = Seq("multiple streaming aggregations"))

  assertSupportedInStreamingPlan(
    "aggregate - streaming aggregations in update mode",
    Aggregate(Nil, aggExprs("d"), streamRelation),
    outputMode = Update)

  assertSupportedInStreamingPlan(
    "aggregate - streaming aggregations in complete mode",
    Aggregate(Nil, aggExprs("d"), streamRelation),
    outputMode = Complete)

  assertSupportedInStreamingPlan(
    "aggregate - streaming aggregations with watermark in append mode",
    Aggregate(Seq(attributeWithWatermark), aggExprs("d"), streamRelation),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "aggregate - streaming aggregations without watermark in append mode",
    Aggregate(Nil, aggExprs("d"), streamRelation),
    outputMode = Append,
    expectedMsgs = Seq("streaming aggregations", "without watermark"))

  // Aggregation: Distinct aggregates not supported on streaming relation
  val distinctAggExprs = Seq(Count("*").toAggregateExpression(isDistinct = true).as("c"))
  assertSupportedInStreamingPlan(
    "distinct aggregate - aggregate on batch relation",
    Aggregate(Nil, distinctAggExprs, batchRelation),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "distinct aggregate - aggregate on streaming relation",
    Aggregate(Nil, distinctAggExprs, streamRelation),
    outputMode = Complete,
    expectedMsgs = Seq("distinct aggregation"))

  val att = new AttributeReference(name = "a", dataType = LongType)()
  // FlatMapGroupsWithState: Both function modes equivalent and supported in batch.
  for (funcMode <- Seq(Append, Update)) {
    assertSupportedInBatchPlan(
      s"flatMapGroupsWithState - flatMapGroupsWithState($funcMode) on batch relation",
      FlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false, null,
        batchRelation))

    assertSupportedInBatchPlan(
      s"flatMapGroupsWithState - multiple flatMapGroupsWithState($funcMode)s on batch relation",
      FlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false, null,
        FlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false,
          null, batchRelation)))
  }

  // FlatMapGroupsWithState(Update) in streaming without aggregation
  assertSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Update) " +
      "on streaming relation without aggregation in update mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Update)

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Update) " +
      "on streaming relation without aggregation in append mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Append,
    expectedMsgs = Seq("flatMapGroupsWithState in update mode", "Append"))

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Update) " +
      "on streaming relation without aggregation in complete mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Complete,
    // Disallowed by the aggregation check but let's still keep this test in case it's broken in
    // future.
    expectedMsgs = Seq("Complete"))

  // FlatMapGroupsWithState(Update) in streaming with aggregation
  for (outputMode <- Seq(Append, Update, Complete)) {
    assertNotSupportedInStreamingPlan(
      "flatMapGroupsWithState - flatMapGroupsWithState(Update) on streaming relation " +
        s"with aggregation in $outputMode mode",
      FlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode,
      expectedMsgs = Seq("flatMapGroupsWithState in update mode", "with aggregation"))
  }

  // FlatMapGroupsWithState(Append) in streaming without aggregation
  assertSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
      "on streaming relation without aggregation in append mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
      "on streaming relation without aggregation in update mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Update,
    expectedMsgs = Seq("flatMapGroupsWithState in append mode", "update"))

  // FlatMapGroupsWithState(Append) in streaming with aggregation
  for (outputMode <- Seq(Append, Update, Complete)) {
    assertSupportedInStreamingPlan(
      "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
        s"on streaming relation before aggregation in $outputMode mode",
      Aggregate(
        Seq(attributeWithWatermark),
        aggExprs("c"),
        FlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
          streamRelation)),
      outputMode = outputMode)
  }

  for (outputMode <- Seq(Append, Update)) {
    assertNotSupportedInStreamingPlan(
      "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
        s"on streaming relation after aggregation in $outputMode mode",
      FlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode,
      expectedMsgs = Seq("flatMapGroupsWithState", "after aggregation"))
  }

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - " +
      "flatMapGroupsWithState(Update) on streaming relation in complete mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Complete,
    // Disallowed by the aggregation check but let's still keep this test in case it's broken in
    // future.
    expectedMsgs = Seq("Complete"))

  // FlatMapGroupsWithState inside batch relation should always be allowed
  for (funcMode <- Seq(Append, Update)) {
    for (outputMode <- Seq(Append, Update)) { // Complete is not supported without aggregation
      assertSupportedInStreamingPlan(
        s"flatMapGroupsWithState - flatMapGroupsWithState($funcMode) on batch relation inside " +
          s"streaming relation in $outputMode output mode",
        FlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false,
          null, batchRelation),
        outputMode = outputMode
      )
    }
  }

  // multiple FlatMapGroupsWithStates
  assertSupportedInStreamingPlan(
    "flatMapGroupsWithState - multiple flatMapGroupsWithStates on streaming relation and all are " +
      "in append mode",
    FlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
      isMapGroupsWithState = false, null,
      FlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null, streamRelation)),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState -  multiple flatMapGroupsWithStates on s streaming relation but some" +
      " are not in append mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      FlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
        streamRelation)),
    outputMode = Append,
    expectedMsgs = Seq("multiple flatMapGroupsWithState", "append"))

  // mapGroupsWithState
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState " +
      "on streaming relation without aggregation in append mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      streamRelation),
    outputMode = Append,
    // Disallowed by the aggregation check but let's still keep this test in case it's broken in
    // future.
    expectedMsgs = Seq("mapGroupsWithState", "append"))

  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState " +
      "on streaming relation without aggregation in complete mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      streamRelation),
    outputMode = Complete,
    // Disallowed by the aggregation check but let's still keep this test in case it's broken in
    // future.
    expectedMsgs = Seq("Complete"))

  for (outputMode <- Seq(Append, Update, Complete)) {
    assertNotSupportedInStreamingPlan(
      "mapGroupsWithState - mapGroupsWithState on streaming relation " +
        s"with aggregation in $outputMode mode",
      FlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Update,
        isMapGroupsWithState = true, null,
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode,
      expectedMsgs = Seq("mapGroupsWithState", "with aggregation"))
  }

  // multiple mapGroupsWithStates
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - multiple mapGroupsWithStates on streaming relation and all are " +
      "in append mode",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      FlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
        streamRelation)),
    outputMode = Append,
    expectedMsgs = Seq("multiple mapGroupsWithStates"))

  // mixing mapGroupsWithStates and flatMapGroupsWithStates
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - " +
      "mixing mapGroupsWithStates and flatMapGroupsWithStates on streaming relation",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      FlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
        streamRelation)
      ),
    outputMode = Append,
    expectedMsgs = Seq("Mixing mapGroupsWithStates and flatMapGroupsWithStates"))

  // mapGroupsWithState with event time timeout + watermark
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState with event time timeout without watermark",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true,
      EventTimeTimeout, streamRelation),
    outputMode = Update,
    expectedMsgs = Seq("watermark"))

  assertSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState with event time timeout with watermark",
    FlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true,
      EventTimeTimeout, new TestStreamingRelation(attributeWithWatermark)),
    outputMode = Update)

  // Deduplicate
  assertSupportedInStreamingPlan(
    "Deduplicate - Deduplicate on streaming relation before aggregation",
    Aggregate(
      Seq(attributeWithWatermark),
      aggExprs("c"),
      Deduplicate(Seq(att), streamRelation)),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "Deduplicate - Deduplicate on streaming relation after aggregation",
    Deduplicate(Seq(att), Aggregate(Nil, aggExprs("c"), streamRelation)),
    outputMode = Complete,
    expectedMsgs = Seq("dropDuplicates"))

  assertSupportedInStreamingPlan(
    "Deduplicate - Deduplicate on batch relation inside a streaming query",
    Deduplicate(Seq(att), batchRelation),
    outputMode = Append
  )

  // Inner joins: Multiple stream-stream joins supported only in append mode
  testBinaryOperationInStreamingPlan(
    "single inner join in append mode",
    _.join(_, joinType = Inner),
    outputMode = Append,
    streamStreamSupported = true)

  testBinaryOperationInStreamingPlan(
    "multiple inner joins in append mode",
    (x: LogicalPlan, y: LogicalPlan) => {
      x.join(y, joinType = Inner).join(streamRelation, joinType = Inner)
    },
    outputMode = Append,
    streamStreamSupported = true)

  testBinaryOperationInStreamingPlan(
    "inner join in update mode",
    _.join(_, joinType = Inner),
    outputMode = Update,
    streamStreamSupported = false,
    expectedMsg = "inner join")

  // Full outer joins: only batch-batch is allowed
  testBinaryOperationInStreamingPlan(
    "full outer join",
    _.join(_, joinType = FullOuter),
    streamStreamSupported = false,
    batchStreamSupported = false,
    streamBatchSupported = false)

  // Left outer joins: *-stream not allowed
  testBinaryOperationInStreamingPlan(
    "left outer join",
    _.join(_, joinType = LeftOuter),
    batchStreamSupported = false,
    streamStreamSupported = false,
    expectedMsg = "outer join")

  // Left outer joins: stream-stream allowed with join on watermark attribute
  // Note that the attribute need not be watermarked on both sides.
  assertSupportedInStreamingPlan(
    s"left outer join with stream-stream relations and join on attribute with left watermark",
    streamRelation.join(streamRelation, joinType = LeftOuter,
      condition = Some(attributeWithWatermark === attribute)),
    OutputMode.Append())
  assertSupportedInStreamingPlan(
    s"left outer join with stream-stream relations and join on attribute with right watermark",
    streamRelation.join(streamRelation, joinType = LeftOuter,
      condition = Some(attribute === attributeWithWatermark)),
    OutputMode.Append())
  assertNotSupportedInStreamingPlan(
    s"left outer join with stream-stream relations and join on non-watermarked attribute",
    streamRelation.join(streamRelation, joinType = LeftOuter,
      condition = Some(attribute === attribute)),
    OutputMode.Append(),
    Seq("watermark in the join keys"))

  // Left outer joins: stream-stream allowed with range condition yielding state value watermark
  assertSupportedInStreamingPlan(
    s"left outer join with stream-stream relations and state value watermark", {
      val leftRelation = streamRelation
      val rightTimeWithWatermark =
        AttributeReference("b", IntegerType)().withMetadata(watermarkMetadata)
      val rightRelation = new TestStreamingRelation(rightTimeWithWatermark)
      leftRelation.join(
        rightRelation,
        joinType = LeftOuter,
        condition = Some(attribute > rightTimeWithWatermark + 10))
    },
    OutputMode.Append())

  // Left outer joins: stream-stream not allowed with insufficient range condition
  assertNotSupportedInStreamingPlan(
    s"left outer join with stream-stream relations and state value watermark", {
      val leftRelation = streamRelation
      val rightTimeWithWatermark =
        AttributeReference("b", IntegerType)().withMetadata(watermarkMetadata)
      val rightRelation = new TestStreamingRelation(rightTimeWithWatermark)
      leftRelation.join(
        rightRelation,
        joinType = LeftOuter,
        condition = Some(attribute < rightTimeWithWatermark + 10))
    },
    OutputMode.Append(),
    Seq("appropriate range condition"))

  // Left semi joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "left semi join",
    _.join(_, joinType = LeftSemi),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left semi/anti joins")

  // Left anti joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "left anti join",
    _.join(_, joinType = LeftAnti),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left semi/anti joins")

  // Right outer joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "right outer join",
    _.join(_, joinType = RightOuter),
    streamBatchSupported = false,
    streamStreamSupported = false,
    expectedMsg = "outer join")

  // Right outer joins: stream-stream allowed with join on watermark attribute
  // Note that the attribute need not be watermarked on both sides.
  assertSupportedInStreamingPlan(
    s"right outer join with stream-stream relations and join on attribute with left watermark",
    streamRelation.join(streamRelation, joinType = RightOuter,
      condition = Some(attributeWithWatermark === attribute)),
    OutputMode.Append())
  assertSupportedInStreamingPlan(
    s"right outer join with stream-stream relations and join on attribute with right watermark",
    streamRelation.join(streamRelation, joinType = RightOuter,
      condition = Some(attribute === attributeWithWatermark)),
    OutputMode.Append())
  assertNotSupportedInStreamingPlan(
    s"right outer join with stream-stream relations and join on non-watermarked attribute",
    streamRelation.join(streamRelation, joinType = RightOuter,
      condition = Some(attribute === attribute)),
    OutputMode.Append(),
    Seq("watermark in the join keys"))

  // Right outer joins: stream-stream allowed with range condition yielding state value watermark
  assertSupportedInStreamingPlan(
    s"right outer join with stream-stream relations and state value watermark", {
      val leftTimeWithWatermark =
        AttributeReference("b", IntegerType)().withMetadata(watermarkMetadata)
      val leftRelation = new TestStreamingRelation(leftTimeWithWatermark)
      val rightRelation = streamRelation
      leftRelation.join(
        rightRelation,
        joinType = RightOuter,
        condition = Some(leftTimeWithWatermark + 10 < attribute))
    },
    OutputMode.Append())

  // Right outer joins: stream-stream not allowed with insufficient range condition
  assertNotSupportedInStreamingPlan(
    s"right outer join with stream-stream relations and state value watermark", {
      val leftTimeWithWatermark =
        AttributeReference("b", IntegerType)().withMetadata(watermarkMetadata)
      val leftRelation = new TestStreamingRelation(leftTimeWithWatermark)
      val rightRelation = streamRelation
      leftRelation.join(
        rightRelation,
        joinType = RightOuter,
        condition = Some(leftTimeWithWatermark + 10 > attribute))
    },
    OutputMode.Append(),
    Seq("appropriate range condition"))

  // Cogroup: only batch-batch is allowed
  testBinaryOperationInStreamingPlan(
    "cogroup",
    genCogroup,
    streamStreamSupported = false,
    batchStreamSupported = false,
    streamBatchSupported = false)

  def genCogroup(left: LogicalPlan, right: LogicalPlan): LogicalPlan = {
    def func(k: Int, left: Iterator[Int], right: Iterator[Int]): Iterator[Int] = {
      Iterator.empty
    }
    implicit val intEncoder = ExpressionEncoder[Int]

    left.cogroup[Int, Int, Int, Int](
      right,
      func,
      AppendColumns[Int, Int]((x: Int) => x, left).newColumns,
      AppendColumns[Int, Int]((x: Int) => x, right).newColumns,
      left.output,
      right.output)
  }

  // Union: Mixing between stream and batch not supported
  testBinaryOperationInStreamingPlan(
    "union",
    _.union(_),
    streamBatchSupported = false,
    batchStreamSupported = false)

  // Except: *-stream not supported
  testBinaryOperationInStreamingPlan(
    "except",
    _.except(_, isAll = false),
    streamStreamSupported = false,
    batchStreamSupported = false)

  // Intersect: stream-stream not supported
  testBinaryOperationInStreamingPlan(
    "intersect",
    _.intersect(_, isAll = false),
    streamStreamSupported = false)

  // Sort: supported only on batch subplans and after aggregation on streaming plan + complete mode
  testUnaryOperatorInStreamingPlan("sort", Sort(Nil, true, _))
  assertSupportedInStreamingPlan(
    "sort - sort after aggregation in Complete output mode",
    streamRelation.groupBy()(Count("*")).sortBy(),
    Complete)
  assertNotSupportedInStreamingPlan(
    "sort - sort before aggregation in Complete output mode",
    streamRelation.sortBy().groupBy()(Count("*")),
    Complete,
    Seq("sort", "aggregat", "complete"))
  assertNotSupportedInStreamingPlan(
    "sort - sort over aggregated data in Update output mode",
    streamRelation.groupBy()(Count("*")).sortBy(),
    Update,
    Seq("sort", "aggregat", "complete")) // sort on aggregations is supported on Complete mode only


  // Other unary operations
  testUnaryOperatorInStreamingPlan(
    "sample", Sample(0.1, 1, true, 1L, _), expectedMsg = "sampling")
  testUnaryOperatorInStreamingPlan(
    "window", Window(Nil, Nil, Nil, _), expectedMsg = "non-time-based windows")

  // Output modes with aggregation and non-aggregation plans
  testOutputMode(Append, shouldSupportAggregation = false, shouldSupportNonAggregation = true)
  testOutputMode(Update, shouldSupportAggregation = true, shouldSupportNonAggregation = true)
  testOutputMode(Complete, shouldSupportAggregation = true, shouldSupportNonAggregation = false)

  // Unsupported expressions in streaming plan
  assertNotSupportedInStreamingPlan(
    "MonotonicallyIncreasingID",
    streamRelation.select(MonotonicallyIncreasingID()),
    outputMode = Append,
    expectedMsgs = Seq("monotonically_increasing_id"))

  assertSupportedForContinuousProcessing(
    "TypedFilter", TypedFilter(
      null,
      null,
      null,
      null,
      new TestStreamingRelationV2(attribute)), OutputMode.Append())

  /*
    =======================================================================================
                                     TESTING FUNCTIONS
    =======================================================================================
   */

  /**
   * Test that an unary operator correctly fails support check when it has a streaming child plan,
   * but not when it has batch child plan. There can be batch sub-plans inside a streaming plan,
   * so it is valid for the operator to have a batch child plan.
   *
   * This test wraps the logical plan in a fake operator that makes the whole plan look like
   * a streaming plan even if the child plan is a batch plan. This is to test that the operator
   * supports having a batch child plan, forming a batch subplan inside a streaming plan.
   */
  def testUnaryOperatorInStreamingPlan(
    operationName: String,
    logicalPlanGenerator: LogicalPlan => LogicalPlan,
    outputMode: OutputMode = Append,
    expectedMsg: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    assertNotSupportedInStreamingPlan(
      s"$operationName with stream relation",
      wrapInStreaming(logicalPlanGenerator(streamRelation)),
      outputMode,
      expectedMsgs)

    assertSupportedInStreamingPlan(
      s"$operationName with batch relation",
      wrapInStreaming(logicalPlanGenerator(batchRelation)),
      outputMode)
  }


  /**
   * Test that a binary operator correctly fails support check when it has combinations of
   * streaming and batch child plans. There can be batch sub-plans inside a streaming plan,
   * so it is valid for the operator to have a batch child plan.
   */
  def testBinaryOperationInStreamingPlan(
      operationName: String,
      planGenerator: (LogicalPlan, LogicalPlan) => LogicalPlan,
      outputMode: OutputMode = Append,
      streamStreamSupported: Boolean = true,
      streamBatchSupported: Boolean = true,
      batchStreamSupported: Boolean = true,
      expectedMsg: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    if (streamStreamSupported) {
      assertSupportedInStreamingPlan(
        s"$operationName with stream-stream relations",
        planGenerator(streamRelation, streamRelation),
        outputMode)
    } else {
      assertNotSupportedInStreamingPlan(
        s"$operationName with stream-stream relations",
        planGenerator(streamRelation, streamRelation),
        outputMode,
        expectedMsgs)
    }

    if (streamBatchSupported) {
      assertSupportedInStreamingPlan(
        s"$operationName with stream-batch relations",
        planGenerator(streamRelation, batchRelation),
        outputMode)
    } else {
      assertNotSupportedInStreamingPlan(
        s"$operationName with stream-batch relations",
        planGenerator(streamRelation, batchRelation),
        outputMode,
        expectedMsgs)
    }

    if (batchStreamSupported) {
      assertSupportedInStreamingPlan(
        s"$operationName with batch-stream relations",
        planGenerator(batchRelation, streamRelation),
        outputMode)
    } else {
      assertNotSupportedInStreamingPlan(
        s"$operationName with batch-stream relations",
        planGenerator(batchRelation, streamRelation),
        outputMode,
        expectedMsgs)
    }

    assertSupportedInStreamingPlan(
      s"$operationName with batch-batch relations",
      planGenerator(batchRelation, batchRelation),
      outputMode)
  }

  /** Test output mode with and without aggregation in the streaming plan */
  def testOutputMode(
      outputMode: OutputMode,
      shouldSupportAggregation: Boolean,
      shouldSupportNonAggregation: Boolean): Unit = {

    // aggregation
    if (shouldSupportAggregation) {
      assertSupportedInStreamingPlan(
        s"$outputMode output mode - aggregation",
        streamRelation.groupBy("a")("count(*)"),
        outputMode = outputMode)
    } else {
      assertNotSupportedInStreamingPlan(
        s"$outputMode output mode - aggregation",
        streamRelation.groupBy("a")("count(*)"),
        outputMode = outputMode,
        Seq("aggregation", s"$outputMode output mode"))
    }

    // non aggregation
    if (shouldSupportNonAggregation) {
      assertSupportedInStreamingPlan(
        s"$outputMode output mode - no aggregation",
        streamRelation.where($"a" > 1),
        outputMode = outputMode)
    } else {
      assertNotSupportedInStreamingPlan(
        s"$outputMode output mode - no aggregation",
        streamRelation.where($"a" > 1),
        outputMode = outputMode,
        Seq("aggregation", s"$outputMode output mode"))
    }
  }

  /**
   * Assert that the logical plan is supported as subplan insider a streaming plan.
   *
   * To test this correctly, the given logical plan is wrapped in a fake operator that makes the
   * whole plan look like a streaming plan. Otherwise, a batch plan may throw not supported
   * exception simply for not being a streaming plan, even though that plan could exist as batch
   * subplan inside some streaming plan.
   */
  def assertSupportedInStreamingPlan(
      name: String,
      plan: LogicalPlan,
      outputMode: OutputMode): Unit = {
    test(s"streaming plan - $name: supported") {
      UnsupportedOperationChecker.checkForStreaming(wrapInStreaming(plan), outputMode)
    }
  }

  /** Assert that the logical plan is supported for continuous procsssing mode */
  def assertSupportedForContinuousProcessing(
    name: String,
    plan: LogicalPlan,
    outputMode: OutputMode): Unit = {
    test(s"continuous processing - $name: supported") {
      UnsupportedOperationChecker.checkForContinuous(plan, outputMode)
    }
  }

  /**
   * Assert that the logical plan is not supported inside a streaming plan.
   *
   * To test this correctly, the given logical plan is wrapped in a fake operator that makes the
   * whole plan look like a streaming plan. Otherwise, a batch plan may throw not supported
   * exception simply for not being a streaming plan, even though that plan could exist as batch
   * subplan inside some streaming plan.
   */
  def assertNotSupportedInStreamingPlan(
      name: String,
      plan: LogicalPlan,
      outputMode: OutputMode,
      expectedMsgs: Seq[String]): Unit = {
    testError(
      s"streaming plan - $name: not supported",
      expectedMsgs :+ "streaming" :+ "DataFrame" :+ "Dataset" :+ "not supported") {
      UnsupportedOperationChecker.checkForStreaming(wrapInStreaming(plan), outputMode)
    }
  }

  /** Assert that the logical plan is supported as a batch plan */
  def assertSupportedInBatchPlan(name: String, plan: LogicalPlan): Unit = {
    test(s"batch plan - $name: supported") {
      UnsupportedOperationChecker.checkForBatch(plan)
    }
  }

  /** Assert that the logical plan is not supported as a batch plan */
  def assertNotSupportedInBatchPlan(
      name: String,
      plan: LogicalPlan,
      expectedMsgs: Seq[String]): Unit = {
    testError(s"batch plan - $name: not supported", expectedMsgs) {
      UnsupportedOperationChecker.checkForBatch(plan)
    }
  }

  /**
   * Test whether the body of code will fail. If it does fail, then check if it has expected
   * messages.
   */
  def testError(testName: String, expectedMsgs: Seq[String])(testBody: => Unit): Unit = {

    test(testName) {
      val e = intercept[AnalysisException] {
        testBody
      }
      expectedMsgs.foreach { m =>
        if (!e.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT))) {
          fail(s"Exception message should contain: '$m', " +
            s"actual exception message:\n\t'${e.getMessage}'")
        }
      }
    }
  }

  def wrapInStreaming(plan: LogicalPlan): LogicalPlan = {
    new StreamingPlanWrapper(plan)
  }

  case class StreamingPlanWrapper(child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = child.output
    override def isStreaming: Boolean = true
  }

  case class TestStreamingRelation(output: Seq[Attribute]) extends LeafNode {
    def this(attribute: Attribute) = this(Seq(attribute))
    override def isStreaming: Boolean = true
  }

  case class TestStreamingRelationV2(output: Seq[Attribute]) extends LeafNode {
    def this(attribute: Attribute) = this(Seq(attribute))
    override def isStreaming: Boolean = true
    override def nodeName: String = "StreamingRelationV2"
  }
}
