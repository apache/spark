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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, MonotonicallyIncreasingID, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder}

/** A dummy command for testing unsupported operations. */
case class DummyCommand() extends LeafCommand

class UnsupportedOperationsSuite extends SparkFunSuite with SQLHelper {

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
    streamRelation.select($"`count(*)`"),
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
    UnsupportedOperationChecker.checkForStreaming(batchRelation.select($"`count(*)`"), Append)
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
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false, null,
        batchRelation))

    assertSupportedInBatchPlan(
      s"flatMapGroupsWithState - multiple flatMapGroupsWithState($funcMode)s on batch relation",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false, null,
        TestFlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false,
          null, batchRelation)))
  }

  // FlatMapGroupsWithState(Update) in streaming without aggregation
  assertSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Update) " +
      "on streaming relation without aggregation in update mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Update)

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Update) " +
      "on streaming relation without aggregation in append mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Append,
    expectedMsgs = Seq("flatMapGroupsWithState in update mode", "Append"))

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Update) " +
      "on streaming relation without aggregation in complete mode",
    TestFlatMapGroupsWithState(
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
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode,
      expectedMsgs = Seq("flatMapGroupsWithState in update mode", "with aggregation"))
  }

  // FlatMapGroupsWithState(Append) in streaming without aggregation
  assertSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
      "on streaming relation without aggregation in append mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
      "on streaming relation without aggregation in update mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
      streamRelation),
    outputMode = Update,
    expectedMsgs = Seq("flatMapGroupsWithState in append mode", "update"))

  // FlatMapGroupsWithState(Append) in streaming with aggregation
  // Only supported when `spark.sql.streaming.statefulOperator.correctnessCheck` is disabled.
  for (outputMode <- Seq(Append, Update, Complete)) {
    assertSupportedInStreamingPlan(
      "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
        s"on streaming relation before aggregation in $outputMode mode",
      Aggregate(
        Seq(attributeWithWatermark),
        aggExprs("c"),
        TestFlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
          streamRelation)),
      outputMode = outputMode,
      SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false")
  }

  for (outputMode <- Seq(Append, Update)) {
    assertNotSupportedInStreamingPlan(
      "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
        s"on streaming relation after aggregation in $outputMode mode",
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode,
      expectedMsgs = Seq("flatMapGroupsWithState", "after aggregation"))
  }

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState - " +
      "flatMapGroupsWithState(Update) on streaming relation in complete mode",
    TestFlatMapGroupsWithState(
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
        TestFlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, funcMode, isMapGroupsWithState = false,
          null, batchRelation),
        outputMode = outputMode
      )
    }
  }

  // multiple FlatMapGroupsWithStates
  // Only supported when `spark.sql.streaming.statefulOperator.correctnessCheck` is disabled.
  assertSupportedInStreamingPlan(
    "flatMapGroupsWithState - multiple flatMapGroupsWithStates on streaming relation and all are " +
      "in append mode",
    TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
      isMapGroupsWithState = false, null,
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null, streamRelation)),
    outputMode = Append,
    SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false")

  assertNotSupportedInStreamingPlan(
    "flatMapGroupsWithState -  multiple flatMapGroupsWithStates on s streaming relation but some" +
      " are not in append mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append, isMapGroupsWithState = false, null,
        streamRelation)),
    outputMode = Append,
    expectedMsgs = Seq("multiple flatMapGroupsWithState", "append"))

  // mapGroupsWithState
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState " +
      "on streaming relation without aggregation in append mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      streamRelation),
    outputMode = Append,
    // Disallowed by the aggregation check but let's still keep this test in case it's broken in
    // future.
    expectedMsgs = Seq("mapGroupsWithState", "append"))

  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState " +
      "on streaming relation without aggregation in complete mode",
    TestFlatMapGroupsWithState(
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
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Update,
        isMapGroupsWithState = true, null,
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode,
      expectedMsgs = Seq("mapGroupsWithState", "with aggregation"))
  }

  // multiple mapGroupsWithStates
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - multiple mapGroupsWithStates on streaming relation and all are " +
      "in append mode",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
        streamRelation)),
    outputMode = Append,
    expectedMsgs = Seq("multiple mapGroupsWithStates"))

  // mixing mapGroupsWithStates and flatMapGroupsWithStates
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - " +
      "mixing mapGroupsWithStates and flatMapGroupsWithStates on streaming relation",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true, null,
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = false, null,
        streamRelation)
      ),
    outputMode = Append,
    expectedMsgs = Seq("Mixing mapGroupsWithStates and flatMapGroupsWithStates"))

  // mapGroupsWithState with event time timeout + watermark
  assertNotSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState with event time timeout without watermark",
    TestFlatMapGroupsWithState(
      null, att, att, Seq(att), Seq(att), att, null, Update, isMapGroupsWithState = true,
      EventTimeTimeout, streamRelation),
    outputMode = Update,
    expectedMsgs = Seq("watermark"))

  assertSupportedInStreamingPlan(
    "mapGroupsWithState - mapGroupsWithState with event time timeout with watermark",
    TestFlatMapGroupsWithState(
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
    outputMode = Append)

  testBinaryOperationInStreamingPlan(
    "multiple inner joins in append mode",
    (x: LogicalPlan, y: LogicalPlan) => {
      x.join(y, joinType = Inner).join(streamRelation, joinType = Inner)
    },
    outputMode = Append)

  testBinaryOperationInStreamingPlan(
    "inner join in update mode",
    _.join(_, joinType = Inner),
    outputMode = Update,
    streamStreamSupported = false,
    expectedMsg = "is not supported in Update output mode")

  // Full outer joins: stream-batch/batch-stream join are not allowed,
  // and stream-stream join is allowed 'conditionally' - see below check
  testBinaryOperationInStreamingPlan(
    "FullOuter join",
    _.join(_, joinType = FullOuter),
    streamStreamSupported = false,
    batchStreamSupported = false,
    streamBatchSupported = false,
    expectedMsg = "FullOuter join")

  // Left outer, left semi, left anti join: *-stream not allowed
  Seq((LeftOuter, "LeftOuter join"), (LeftSemi, "LeftSemi join"), (LeftAnti, "LeftAnti join"))
    .foreach { case (joinType, name) =>
      testBinaryOperationInStreamingPlan(
        name,
        _.join(_, joinType = joinType),
        batchStreamSupported = false,
        streamStreamSupported = false,
        expectedMsg = name)
    }

  // Right outer joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "RightOuter join",
    _.join(_, joinType = RightOuter),
    streamBatchSupported = false,
    streamStreamSupported = false,
    expectedMsg = "RightOuter join")

  // Left outer, right outer, full outer, left semi joins
  Seq(LeftOuter, RightOuter, FullOuter, LeftSemi).foreach { joinType =>
    // Update mode not allowed
    assertNotSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and update mode",
      streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attribute === attribute)),
      OutputMode.Update(),
      Seq("is not supported in Update output mode"))

    // Complete mode not allowed
    assertNotSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and complete mode",
      Aggregate(Nil, aggExprs("d"), streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attribute === attribute))),
      OutputMode.Complete(),
      Seq("is not supported in Complete output mode"))

    // Stream-stream allowed with join on watermark attribute
    // Note that the attribute need not be watermarked on both sides.
    assertSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and join on attribute with left watermark",
      streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attributeWithWatermark === attribute)),
      OutputMode.Append())
    assertSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and join on attribute with right watermark",
      streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attribute === attributeWithWatermark)),
      OutputMode.Append())
    assertNotSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and join on non-watermarked attribute",
      streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attribute === attribute)),
      OutputMode.Append(),
      Seq("without a watermark in the join keys"))

    val timeWithWatermark =
      AttributeReference("b", IntegerType)().withMetadata(watermarkMetadata)
    val relationWithWatermark = new TestStreamingRelation(timeWithWatermark)
    val (leftRelation, rightRelation) =
      if (joinType == RightOuter) {
        (relationWithWatermark, streamRelation)
      } else {
        (streamRelation, relationWithWatermark)
      }

    // stream-stream allowed with range condition yielding state value watermark
    assertSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and state value watermark",
      leftRelation.join(rightRelation, joinType = joinType,
        condition = Some(attribute > timeWithWatermark + 10)),
      OutputMode.Append())

    // stream-stream not allowed with insufficient range condition
    assertNotSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and state value watermark",
      leftRelation.join(rightRelation, joinType = joinType,
        condition = Some(attribute < timeWithWatermark + 10)),
      OutputMode.Append(),
      Seq("is not supported without a watermark in the join keys, or a watermark on " +
        "the nullable side and an appropriate range condition"))
  }

  // stream-stream inner join doesn't emit late rows, whereas outer joins could
  Seq((Inner, false), (LeftOuter, true), (RightOuter, true)).foreach {
    case (joinType, expectFailure) =>
      assertPassOnGlobalWatermarkLimit(
        s"single $joinType join in Append mode",
        streamRelation.join(streamRelation, joinType = RightOuter,
          condition = Some(attributeWithWatermark === attribute)),
        OutputMode.Append())

      testGlobalWatermarkLimit(
        s"streaming aggregation after stream-stream $joinType join in Append mode",
        streamRelation.join(streamRelation, joinType = joinType,
          condition = Some(attributeWithWatermark === attribute))
          .groupBy("a")(count("*")),
        OutputMode.Append(),
        expectFailure = expectFailure)

      Seq(Inner, LeftOuter, RightOuter).foreach { joinType2 =>
        testGlobalWatermarkLimit(
          s"streaming-stream $joinType2 after stream-stream $joinType join in Append mode",
          streamRelation.join(
            streamRelation.join(streamRelation, joinType = joinType,
              condition = Some(attributeWithWatermark === attribute)),
            joinType = joinType2,
            condition = Some(attributeWithWatermark === attribute)),
          OutputMode.Append(),
          expectFailure = expectFailure)
      }

      testGlobalWatermarkLimit(
        s"FlatMapGroupsWithState after stream-stream $joinType join in Append mode",
        TestFlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, null,
          streamRelation.join(streamRelation, joinType = joinType,
            condition = Some(attributeWithWatermark === attribute))),
        OutputMode.Append(),
        expectFailure = expectFailure)

      testGlobalWatermarkLimit(
        s"deduplicate after stream-stream $joinType join in Append mode",
        Deduplicate(Seq(attribute), streamRelation.join(streamRelation, joinType = joinType,
          condition = Some(attributeWithWatermark === attribute))),
        OutputMode.Append(),
        expectFailure = expectFailure)
  }

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

  // Intersect: not supported
  testBinaryOperationInStreamingPlan(
    "intersect",
    _.intersect(_, isAll = false),
    batchStreamSupported = false,
    streamBatchSupported = false,
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

  // streaming aggregation
  {
    assertPassOnGlobalWatermarkLimit(
      "single streaming aggregation in Append mode",
      streamRelation.groupBy("a")(count("*")),
      OutputMode.Append())

    assertFailOnGlobalWatermarkLimit(
      "chained streaming aggregations in Append mode",
      streamRelation.groupBy("a")(count("*")).groupBy()(count("*")),
      OutputMode.Append())

    Seq(Inner, LeftOuter, RightOuter).foreach { joinType =>
      val plan = streamRelation.join(streamRelation.groupBy("a")(count("*")), joinType = joinType)
      assertFailOnGlobalWatermarkLimit(
        s"$joinType join after streaming aggregation in Append mode",
        streamRelation.join(streamRelation.groupBy("a")(count("*")), joinType = joinType),
        OutputMode.Append())
    }

    assertFailOnGlobalWatermarkLimit(
      "deduplicate after streaming aggregation in Append mode",
      Deduplicate(Seq(attribute), streamRelation.groupBy("a")(count("*"))),
      OutputMode.Append())

    assertFailOnGlobalWatermarkLimit(
      "FlatMapGroupsWithState after streaming aggregation in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        streamRelation.groupBy("a")(count("*"))),
      OutputMode.Append())
  }

  // FlatMapGroupsWithState
  {
    assertPassOnGlobalWatermarkLimit(
      "single FlatMapGroupsWithState in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null, streamRelation),
      OutputMode.Append())

    assertFailOnGlobalWatermarkLimit(
      "streaming aggregation after FlatMapGroupsWithState in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null, streamRelation).groupBy("*")(count("*")),
      OutputMode.Append())

    Seq(Inner, LeftOuter, RightOuter).foreach { joinType =>
      assertFailOnGlobalWatermarkLimit(
        s"stream-stream $joinType after FlatMapGroupsWithState in Append mode",
        streamRelation.join(
          TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, null, streamRelation), joinType = joinType,
          condition = Some(attributeWithWatermark === attribute)),
        OutputMode.Append())
    }

    assertFailOnGlobalWatermarkLimit(
      "FlatMapGroupsWithState after FlatMapGroupsWithState in Append mode",
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, null, streamRelation)),
      OutputMode.Append())

    assertFailOnGlobalWatermarkLimit(
      s"deduplicate after FlatMapGroupsWithState in Append mode",
      Deduplicate(Seq(attribute),
        TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, null, streamRelation)),
      OutputMode.Append())
  }

  // deduplicate
  {
    assertPassOnGlobalWatermarkLimit(
      "streaming aggregation after deduplicate in Append mode",
      Deduplicate(Seq(attribute), streamRelation).groupBy("a")(count("*")),
      OutputMode.Append())

    Seq(Inner, LeftOuter, RightOuter).foreach { joinType =>
      assertPassOnGlobalWatermarkLimit(
        s"$joinType join after deduplicate in Append mode",
        streamRelation.join(Deduplicate(Seq(attribute), streamRelation), joinType = joinType,
          condition = Some(attributeWithWatermark === attribute)),
        OutputMode.Append())
    }

    assertPassOnGlobalWatermarkLimit(
      "FlatMapGroupsWithState after deduplicate in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        Deduplicate(Seq(attribute), streamRelation)),
      OutputMode.Append())
  }

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
      outputMode: OutputMode,
      configs: (String, String)*): Unit = {
    test(s"streaming plan - $name: supported") {
      withSQLConf(configs: _*) {
        UnsupportedOperationChecker.checkForStreaming(wrapInStreaming(plan), outputMode)
      }
    }
  }

  /** Assert that the logical plan is supported for continuous processing mode */
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


  def assertPassOnGlobalWatermarkLimit(
      testNamePostfix: String,
      plan: LogicalPlan,
      outputMode: OutputMode): Unit = {
    testGlobalWatermarkLimit(testNamePostfix, plan, outputMode, expectFailure = false)
  }

  def assertFailOnGlobalWatermarkLimit(
      testNamePostfix: String,
      plan: LogicalPlan,
      outputMode: OutputMode): Unit = {
    testGlobalWatermarkLimit(testNamePostfix, plan, outputMode, expectFailure = true)
  }

  def testGlobalWatermarkLimit(
      testNamePostfix: String,
      plan: LogicalPlan,
      outputMode: OutputMode,
      expectFailure: Boolean): Unit = {
    test(s"Global watermark limit - $testNamePostfix") {
      if (expectFailure) {
        withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "true") {
          val e = intercept[AnalysisException] {
            UnsupportedOperationChecker.checkStreamingQueryGlobalWatermarkLimit(
              wrapInStreaming(plan), outputMode)
          }
          assert(e.message.contains("Detected pattern of possible 'correctness' issue"))
        }
      } else {
        withSQLConf(SQLConf.STATEFUL_OPERATOR_CHECK_CORRECTNESS_ENABLED.key -> "false") {
          UnsupportedOperationChecker.checkStreamingQueryGlobalWatermarkLimit(
            wrapInStreaming(plan), outputMode)
        }
      }
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
    override protected def withNewChildInternal(newChild: LogicalPlan): StreamingPlanWrapper =
      copy(child = newChild)
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

object TestFlatMapGroupsWithState {

  // scalastyle:off
  // Creating an apply constructor here as we changed the class by adding more fields
  def apply(func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      outputObjAttr: Attribute,
      stateEncoder: ExpressionEncoder[Any],
      outputMode: OutputMode,
      isMapGroupsWithState: Boolean = false,
      timeout: GroupStateTimeout,
      child: LogicalPlan): FlatMapGroupsWithState = {

    val attribute = AttributeReference("a", IntegerType, nullable = true)()
    val batchRelation = LocalRelation(attribute)
    new FlatMapGroupsWithState(
      func,
      keyDeserializer,
      valueDeserializer,
      groupingAttributes,
      dataAttributes,
      outputObjAttr,
      stateEncoder,
      outputMode,
      isMapGroupsWithState,
      timeout,
      false,
      groupingAttributes,
      dataAttributes,
      valueDeserializer,
      batchRelation,
      child
    )
  }
  // scalastyle:on
}
