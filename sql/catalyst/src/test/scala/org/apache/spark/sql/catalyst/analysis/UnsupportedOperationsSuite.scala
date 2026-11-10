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

import java.time.{Duration, Period}
import java.util.Locale

import org.apache.logging.log4j.Level

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, MonotonicallyIncreasingID, NamedExpression, TimestampAddInterval, TimestampAddYMInterval}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode, StatefulProcessor, TimeMode, TimerValues}
import org.apache.spark.sql.types.{IntegerType, LongType, MetadataBuilder, StructType, TimestampType, YearMonthIntervalType}

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

  private val emptyTableSpec = TableSpec(
    properties = Map.empty,
    provider = None,
    options = Map.empty,
    location = None,
    comment = None,
    collation = None,
    serde = None,
    external = false)

  assertSupportedInBatchPlan(
    "streaming table definition with streaming source",
    CreateStreamingTableAsSelect(
      name = batchRelation,
      columns = Nil,
      partitioning = Nil,
      tableSpec = emptyTableSpec,
      query = streamRelation,
      originalText = "",
      ifNotExists = false))

  assertSupportedInBatchPlan(
    "flow definition with streaming source",
    CreateFlowCommand(batchRelation, streamRelation, comment = None))

  assertSupportedInBatchPlan(
    "AUTO CDC streaming table definition with streaming source",
    CreateStreamingTableAutoCdc(
      name = batchRelation,
      columns = Nil,
      partitioning = Nil,
      tableSpec = emptyTableSpec,
      ifNotExists = false,
      source = streamRelation,
      keys = Nil,
      deleteCondition = None,
      sequenceByExpr = attribute,
      includeColumns = None,
      excludeColumns = None,
      storedAsScdType = 1,
      trackHistoryColumns = None,
      trackHistoryExceptColumns = None,
      ignoreNullUpdates = false,
      ignoreNullUpdatesColumns = None,
      ignoreNullUpdatesExceptColumns = None))

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

  assertOutputModeNotSupportedInStreamingPlan(
    "aggregate - streaming aggregations without watermark in append mode",
    Aggregate(Nil, aggExprs("d"), streamRelation),
    Append,
    "streaming aggregations without watermark")

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
    "Deduplicate - Deduplicate on streaming relation before aggregation - append",
    Aggregate(
      Seq(attributeWithWatermark),
      aggExprs("c"),
      Deduplicate(Seq(att), streamRelation)),
    outputMode = Append)

  assertSupportedInStreamingPlan(
    "Deduplicate - Deduplicate on batch relation inside a streaming query",
    Deduplicate(Seq(att), batchRelation),
    outputMode = Append
  )

  def asOfJoin(left: LogicalPlan, right: LogicalPlan): AsOfJoin = {
    AsOfJoin(
      left,
      right,
      left.output.head >= right.output.head,
      condition = None,
      joinType = Inner,
      tolerance = None,
      direction = Backward,
      leftSortExprs = Seq(left.output.head),
      rightSortExprs = Seq(right.output.head),
      orderExpression = Some(left.output.head - right.output.head))
  }

  assertSupportedInStreamingPlan(
    "ASOF join with stream-static relations",
    asOfJoin(streamRelation, batchRelation),
    outputMode = Append)

  assertNotSupportedInStreamingPlan(
    "ASOF join with static-stream relations",
    asOfJoin(batchRelation, streamRelation),
    outputMode = Append,
    expectedMsgs = Seq("ASOF join", "streaming DataFrame/Dataset on the right"))

  assertNotSupportedInStreamingPlan(
    "ASOF join with stream-stream relations",
    asOfJoin(streamRelation, streamRelation),
    outputMode = Append,
    expectedMsgs = Seq("ASOF join", "streaming DataFrame/Dataset on the right"))

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
    outputMode = Update)

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

  // Left outer, right outer, full outer joins: Update mode not allowed
  Seq(LeftOuter, RightOuter, FullOuter).foreach { joinType =>
    assertNotSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and update mode",
      streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attribute === attribute)),
      OutputMode.Update(),
      Seq("is not supported in Update output mode"))
  }

  // LeftSemi join: Update mode allowed (equivalent to Append mode for non-outer joins)
  {
    val right = attributeWithWatermark.newInstance()
    assertSupportedInStreamingPlan(
      "LeftSemi join with stream-stream relations and update mode",
      streamRelation.join(new TestStreamingRelation(right),
        joinType = LeftSemi, condition = Some(attribute === right)),
      OutputMode.Update())
  }

  // Complete mode not allowed for stream-stream joins. The error message also indicates
  // which output modes are actually supported for the given join type.
  Seq(
    (Inner, "only in Append and Update output modes"),
    (LeftSemi, "only in Append and Update output modes"),
    (LeftOuter, "only in Append output mode"),
    (RightOuter, "only in Append output mode"),
    (FullOuter, "only in Append output mode")
  ).foreach { case (joinType, allowedModesMsg) =>
    assertNotSupportedInStreamingPlan(
      s"$joinType join with stream-stream relations and complete mode",
      Aggregate(Nil, aggExprs("d"), streamRelation.join(streamRelation, joinType = joinType,
        condition = Some(attributeWithWatermark === attribute))),
      OutputMode.Complete(),
      Seq("is not supported in Complete output mode", allowedModesMsg))
  }

  for {
    joinType <- Seq(LeftOuter, RightOuter, FullOuter, LeftSemi)
    leftWatermark <- Seq(false, true)
    rightWatermark <- Seq(false, true)
    conditionType <- Seq("equality", "range", "left bound", "right bound")
  } {
    val leftTime = AttributeReference("leftTime", TimestampType)()
    val rightTime = AttributeReference("rightTime", TimestampType)()
    val left = if (leftWatermark) leftTime.withMetadata(watermarkMetadata) else leftTime
    val right = if (rightWatermark) rightTime.withMetadata(watermarkMetadata) else rightTime
    val condition = conditionType match {
      case "equality" => left === right
      case "range" => left > right - 10 && left < right + 10
      case "left bound" => left > right - 10
      case "right bound" => left < right + 10
    }
    val leftCanEvict = rightWatermark && conditionType != "right bound"
    val rightCanEvict = leftWatermark && conditionType != "left bound"
    val correctedSupported = joinType match {
      case LeftOuter | LeftSemi => leftCanEvict
      case RightOuter => rightCanEvict
      case FullOuter => leftCanEvict && rightCanEvict
      case _ => false
    }
    val plan = new TestStreamingRelation(left).join(new TestStreamingRelation(right),
      joinType = joinType, condition = Some(condition))
    val name = s"$joinType $conditionType with watermarks " +
      s"left=$leftWatermark right=$rightWatermark"
    if (correctedSupported) {
      assertSupportedInStreamingPlan(name, plan, OutputMode.Append())
    } else {
      assertNotSupportedInStreamingPlan(name, plan, OutputMode.Append(),
        Seq("requires", "allow unmatched state to be finalized"))
    }
  }

  {
    val leftTime = AttributeReference("leftTime", TimestampType)()
    val leftOther = AttributeReference("leftOther", TimestampType)()
      .withMetadata(watermarkMetadata)
    val rightTime = AttributeReference("rightTime", TimestampType)()
      .withMetadata(watermarkMetadata)
    val rightOther = AttributeReference("rightOther", TimestampType)()
    val left = new TestStreamingRelation(Seq(leftTime, leftOther))
    val right = new TestStreamingRelation(Seq(rightTime, rightOther))

    // Each side's state is evicted by the key that the opposite watermark bounds.
    assertSupportedInStreamingPlan("full outer with different watermark key ordinals",
      left.join(right, joinType = FullOuter,
        condition = Some(leftTime === rightTime && leftOther === rightOther)), OutputMode.Append())
  }

  {
    val leftTime = AttributeReference("leftTime", TimestampType)()
      .withMetadata(watermarkMetadata)
    val rightTime = AttributeReference("rightTime", TimestampType)()
      .withMetadata(watermarkMetadata)
    val rightOther = AttributeReference("rightOther", TimestampType)()
    val left = new TestStreamingRelation(leftTime)
    val right = new TestStreamingRelation(Seq(rightTime, rightOther))

    def shift(time: Expression, seconds: Int): Expression =
      TimestampAddInterval(time, Literal(Duration.ofSeconds(seconds)))

    // A key computed from the opposite event time by a constant is bounded by its watermark.
    Seq(10, -10).foreach { offset =>
      Seq(LeftOuter, RightOuter, FullOuter, LeftSemi).foreach { joinType =>
        assertSupportedInStreamingPlan(s"$joinType with a key shifted by $offset",
          left.join(right, joinType = joinType,
            condition = Some(leftTime === shift(rightTime, offset))), OutputMode.Append())
      }
      assertNotSupportedInStreamingPlan(
        s"LeftOuter with a key shifted by $offset from an unwatermarked column",
        left.join(right, joinType = LeftOuter,
          condition = Some(leftTime === shift(rightOther, offset))), OutputMode.Append(),
        Seq("requires a watermark on the right input"))
    }

    // Whole days are added in the session time zone, where a day can be 23 or 25 hours, and
    // months vary in length. A shift that never moves a time backward still bounds the key, in
    // every time zone; one that can move a time backward does not.
    for {
      zone <- Seq("UTC", "America/Los_Angeles")
      (name, interval, supported) <- Seq(
        ("one day", Literal(Duration.ofDays(1)), true),
        ("minus one day", Literal(Duration.ofDays(-1)), false),
        ("one month", Literal(Period.ofMonths(1)), true),
        ("minus one month", Literal(Period.ofMonths(-1)), false))
    } {
      val shifted = interval.dataType match {
        case _: YearMonthIntervalType => TimestampAddYMInterval(rightTime, interval, Some(zone))
        case _ => TimestampAddInterval(rightTime, interval, Some(zone))
      }
      Seq(LeftOuter, LeftSemi, FullOuter).foreach { joinType =>
        val plan = left.join(right, joinType = joinType, condition = Some(leftTime === shifted))
        val testName = s"$joinType with a key shifted by $name in $zone"
        if (supported) {
          assertSupportedInStreamingPlan(testName, plan, OutputMode.Append())
        } else {
          val requirement = if (joinType == FullOuter) {
            "watermarks on both inputs"
          } else {
            "a watermark on the right input"
          }
          assertNotSupportedInStreamingPlan(testName, plan, OutputMode.Append(),
            Seq(s"requires $requirement"))
        }
      }
    }

    // Truncation can map a future right row below the watermark, so it does not bound the key.
    assertNotSupportedInStreamingPlan("LeftOuter with a key truncated from the right event time",
      left.join(right, joinType = LeftOuter,
        condition = Some(leftTime === rightTime.cast(LongType).cast(TimestampType))),
      OutputMode.Append(), Seq("requires a watermark on the right input"))
  }

  // Disabling the finalization check turns the admission failure into a warning.
  Seq(LeftOuter, LeftSemi).foreach { joinType =>
    val leftTime = AttributeReference("leftTime", TimestampType)().withMetadata(watermarkMetadata)
    val rightTime = AttributeReference("rightTime", TimestampType)()
    val plan = new TestStreamingRelation(leftTime).join(new TestStreamingRelation(rightTime),
      joinType = joinType, condition = Some(leftTime === rightTime))
    val checkConf = SQLConf.STREAMING_JOIN_CHECK_FINALIZATION_ENABLED.key
    assertNotSupportedInStreamingPlan(s"$joinType without a right watermark",
      plan, OutputMode.Append(), Seq("requires a watermark on the right input", checkConf))
    assertSupportedInStreamingPlan(s"$joinType without a right watermark and $checkConf=false",
      plan, OutputMode.Append(), checkConf -> "false")

    test(s"$joinType without a right watermark logs a warning when $checkConf=false") {
      val appender = new LogAppender("finalization check")
      withLogAppender(appender, level = Some(Level.WARN)) {
        withSQLConf(checkConf -> "false") {
          UnsupportedOperationChecker.checkForStreaming(wrapInStreaming(plan), OutputMode.Append())
        }
      }
      assert(appender.loggingEvents.exists(_.getMessage.getFormattedMessage
        .contains("requires a watermark on the right input")))
    }
  }

  {
    val rightTime = AttributeReference("rightTime", TimestampType)()
      .withMetadata(watermarkMetadata)
    val unwatermarkedTime = AttributeReference("leftTime", TimestampType)()
    val right = new TestStreamingRelation(rightTime)

    // A range bound that counts a local day as 24 hours, or reads a truncated time as exact, can
    // put the cutoff above a row that can still match. Only an input with its own watermark keeps
    // such a bound, so an outer join of an input without one cannot finalize its rows.
    for {
      watermarked <- Seq(false, true)
      zone <- Seq("UTC", "America/Los_Angeles")
    } {
      val leftTime = if (watermarked) {
        unwatermarkedTime.withMetadata(watermarkMetadata)
      } else {
        unwatermarkedTime
      }
      val left = new TestStreamingRelation(leftTime)
      val dayLater = TimestampAddInterval(leftTime, Literal(Duration.ofDays(1)), Some(zone))
      val inputs = if (watermarked) "both inputs watermarked" else "only the right watermarked"
      val plan = left.join(right, joinType = LeftOuter,
        condition = Some(rightTime >= leftTime && rightTime <= dayLater))
      val name = s"LeftOuter with a one-day range in $zone, $inputs"
      if (watermarked || zone == "UTC") {
        assertSupportedInStreamingPlan(name, plan, OutputMode.Append())
      } else {
        assertNotSupportedInStreamingPlan(name, plan, OutputMode.Append(),
          Seq("requires a watermark on the right input"))
      }
    }
    assertNotSupportedInStreamingPlan("LeftOuter with a range over a truncating cast",
      new TestStreamingRelation(unwatermarkedTime).join(right, joinType = LeftOuter,
        condition = Some(unwatermarkedTime.cast(LongType) >= rightTime.cast(LongType) + 5L)),
      OutputMode.Append(), Seq("requires a watermark on the right input"))
  }

  // multi-aggregations only supported in Append mode
  assertPassOnGlobalWatermarkLimit(
    "aggregate - multiple streaming aggregations - append",
    Aggregate(Nil, aggExprs("c"), Aggregate(Nil, aggExprs("d"), streamRelation)),
    outputMode = Append)

  assertFailOnGlobalWatermarkLimit(
    "aggregate - multiple streaming aggregations - update",
    Aggregate(Nil, aggExprs("c"), Aggregate(Nil, aggExprs("d"), streamRelation)),
    outputMode = Update)

  assertFailOnGlobalWatermarkLimit(
    "aggregate - multiple streaming aggregations - complete",
    Aggregate(Nil, aggExprs("c"), Aggregate(Nil, aggExprs("d"), streamRelation)),
    outputMode = Complete)

  assertPassOnGlobalWatermarkLimit(
    "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
      s"on streaming relation after aggregation in Append mode",
    TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
      isMapGroupsWithState = false, null,
      Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
    outputMode = Append)

  // Aggregation not in Append mode followed by any stateful operators is disallowed
  assertFailOnGlobalWatermarkLimit(
    "flatMapGroupsWithState - flatMapGroupsWithState(Append) " +
      s"on streaming relation after aggregation in Update mode",
    TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
      isMapGroupsWithState = false, null,
      Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
    outputMode = Update)

  // Aggregation not in Append mode followed by any stateful operators is disallowed
  assertFailOnGlobalWatermarkLimit(
    "mapGroupsWithState - mapGroupsWithState on streaming relation " +
      "after aggregation in Update mode",
    TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Update,
      isMapGroupsWithState = true, null,
      Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
    outputMode = Update)

  // FlatMapGroupsWithState followed by any stateful op not allowed, here test aggregation
  assertFailOnGlobalWatermarkLimit(
    "multiple stateful ops - FlatMapGroupsWithState followed by agg",
    Aggregate(Nil, aggExprs("c"),
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, GroupStateTimeout.EventTimeTimeout(), streamRelation)),
    outputMode = Append)

  // But allows if the FlatMapGroupsWithState has timeout on processing time
  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - FlatMapGroupsWithState(process time) followed by agg",
    Aggregate(Nil, aggExprs("c"),
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, GroupStateTimeout.ProcessingTimeTimeout(), streamRelation)),
    outputMode = Append)

  // MapGroupsWithState followed by any stateful op not allowed, here test aggregation
  assertFailOnGlobalWatermarkLimit(
    "multiple stateful ops - MapGroupsWithState followed by agg",
    Aggregate(Nil, aggExprs("c"),
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Update,
        isMapGroupsWithState = true, GroupStateTimeout.EventTimeTimeout(), streamRelation)),
    outputMode = Append)

  // But allows if the MapGroupsWithState has timeout on processing time
  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - MapGroupsWithState(process time) followed by agg",
    Aggregate(Nil, aggExprs("c"),
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Update,
        isMapGroupsWithState = true, GroupStateTimeout.ProcessingTimeTimeout(), streamRelation)),
    outputMode = Append)

  // stream-stream relation, time interval join can be followed by any stateful operators
  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - stream-stream time-interval join followed by agg",
    Aggregate(Nil, aggExprs("c"),
      streamRelation.join(streamRelation, joinType = Inner,
        condition = Some(attribute === attribute &&
          attributeWithWatermark > attributeWithWatermark + 10))),
    outputMode = Append)

  // stream-stream relation, equality join can be followed by any stateful operators
  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - stream-stream equality join followed by agg",
    Aggregate(Nil, aggExprs("c"),
      streamRelation.join(streamRelation, joinType = Inner,
        condition = Some(attribute === attribute))),
    outputMode = Append)

  // Deduplication checks:
  // Deduplication, if on event time column, is a stateful operator
  // and cannot be placed after FlatMapGroupsWithState
  assertFailOnGlobalWatermarkLimit(
    "multiple stateful ops - FlatMapGroupsWithState followed by " +
      "dedup (with event-time)",
    Deduplicate(Seq(attributeWithWatermark),
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, GroupStateTimeout.EventTimeTimeout(), streamRelation)),
    outputMode = Append)

  // Deduplication, if not on event time column,
  // although it is still a stateful operator,
  // it can be placed after FlatMapGroupsWithState
  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - FlatMapGroupsWithState followed by " +
      "dedup (without event-time)",
    Deduplicate(Seq(att),
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null, streamRelation)),
    outputMode = Append)

  // Deduplication, if on event time column, is a stateful operator
  // and cannot be placed after aggregation
  for (outputMode <- Seq(Update, Complete)) {
    assertFailOnGlobalWatermarkLimit(
      s"multiple stateful ops - aggregation($outputMode mode) followed by " +
        "dedup (with event-time)",
      Deduplicate(Seq(attributeWithWatermark),
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode)

    // Deduplication, if not on event time column,
    // although it is still a stateful operator,
    // it can be placed after aggregation
    assertPassOnGlobalWatermarkLimit(
      s"multiple stateful ops - aggregation($outputMode mode) followed by " +
        "dedup (without event-time)",
      Deduplicate(Seq(att),
        Aggregate(Seq(attributeWithWatermark), aggExprs("c"), streamRelation)),
      outputMode = outputMode)
  }

  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - stream-stream time interval join followed by " +
      "dedup (with event-time)",
    Deduplicate(Seq(attributeWithWatermark),
      streamRelation.join(streamRelation, joinType = Inner,
        condition = Some(attribute === attribute &&
          attributeWithWatermark > attributeWithWatermark + 10))),
    outputMode = Append)

  assertPassOnGlobalWatermarkLimit(
    "multiple stateful ops - stream-stream time interval join followed by " +
      "dedup (without event-time)",
    Deduplicate(Seq(att),
      streamRelation.join(streamRelation, joinType = Inner,
        condition = Some(attribute === attribute &&
          attributeWithWatermark > attributeWithWatermark + 10))),
    outputMode = Append)

  Seq(Inner, LeftOuter, RightOuter, FullOuter).foreach {
    joinType =>
      assertPassOnGlobalWatermarkLimit(
        s"streaming aggregation after " +
          s"stream-stream $joinType join keyed on time interval in Append mode are not supported",
        streamRelation.join(streamRelation, joinType = joinType,
          condition = Some(attributeWithWatermark === attribute &&
            attributeWithWatermark < attributeWithWatermark + 10))
          .groupBy("a")(count("*")),
        outputMode = Append)

      assertPassOnGlobalWatermarkLimit(
        s"single $joinType join in Append mode",
        streamRelation.join(streamRelation, joinType = RightOuter,
          condition = Some(attributeWithWatermark === attribute)),
        outputMode = Append)

      assertPassOnGlobalWatermarkLimit(
        s"streaming aggregation after " +
          s"stream-stream $joinType join keyed on time equality in Append mode are supported",
        streamRelation.join(streamRelation, joinType = joinType,
          condition = Some(attributeWithWatermark === attribute))
          .groupBy("a")(count("*")),
        outputMode = Append)

      Seq(Inner, LeftOuter, RightOuter).foreach { joinType2 =>
        assertPassOnGlobalWatermarkLimit(
          s"streaming-stream $joinType2 after stream-stream $joinType join in Append mode",
          streamRelation.join(
            streamRelation.join(streamRelation, joinType = joinType,
              condition = Some(attributeWithWatermark === attribute)),
            joinType = joinType2,
            condition = Some(attributeWithWatermark === attribute)),
          outputMode = Append)
      }

      assertPassOnGlobalWatermarkLimit(
        s"FlatMapGroupsWithState after stream-stream $joinType join in Append mode",
        TestFlatMapGroupsWithState(
          null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, null,
          streamRelation.join(streamRelation, joinType = joinType,
            condition = Some(attributeWithWatermark === attribute))),
        outputMode = Append)

      assertPassOnGlobalWatermarkLimit(
        s"deduplicate after stream-stream $joinType join in Append mode",
        Deduplicate(Seq(attribute), streamRelation.join(streamRelation, joinType = joinType,
          condition = Some(attributeWithWatermark === attribute))),
        outputMode = Append)
  }

  assertPassOnGlobalWatermarkLimit(
    "streaming aggregation after stream-stream inner join in Update mode",
    streamRelation.join(streamRelation, joinType = Inner,
      condition = Some(attributeWithWatermark === attribute))
      .groupBy("a")(count("*")),
    outputMode = Update)

  assertFailOnGlobalWatermarkLimit(
    "streaming aggregation on both sides followed by stream-stream inner join in Update mode",
    streamRelation.groupBy("a")(count("*")).join(
      streamRelation.groupBy("a")(count("*")),
      joinType = Inner,
      condition = Some(attributeWithWatermark === attribute)),
    outputMode = Update)

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
    implicit val intEncoder = ExpressionEncoder[Int]()

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

  // Except: by default, streaming input on either side is not supported
  testBinaryOperationInStreamingPlan(
    "except",
    _.except(_, isAll = false),
    streamStreamSupported = false,
    streamBatchSupported = false,
    batchStreamSupported = false)

  testBinaryOperationInStreamingPlan(
    "except all",
    _.except(_, isAll = true),
    streamStreamSupported = false,
    streamBatchSupported = false,
    batchStreamSupported = false)

  assertSupportedInStreamingPlan(
    "except with stream-batch relations and legacy compatibility enabled",
    streamRelation.except(batchRelation, isAll = false),
    Append,
    SQLConf.ALLOW_EXCEPT_ON_STREAMING_DATAFRAME.key -> "true")

  assertSupportedInStreamingPlan(
    "except all with stream-batch relations and legacy compatibility enabled",
    streamRelation.except(batchRelation, isAll = true),
    Append,
    SQLConf.ALLOW_EXCEPT_ON_STREAMING_DATAFRAME.key -> "true")

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
    "window",
    Window(Nil, Nil, Nil, _),
    condition = "NON_TIME_WINDOW_NOT_SUPPORTED_IN_STREAMING")

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
      new TestStreamingRelationV2(attribute)), outputMode = Append)

  // streaming aggregation
  {
    assertPassOnGlobalWatermarkLimit(
      "single streaming aggregation in Append mode",
      streamRelation.groupBy("a")(count("*")), outputMode = Append)

    assertPassOnGlobalWatermarkLimit(
      "chained streaming aggregations in Append mode",
      streamRelation.groupBy("a")(count("*")).groupBy()(count("*")), outputMode = Append)

    Seq(Inner, LeftOuter, RightOuter).foreach { joinType =>
      val plan = streamRelation.join(streamRelation.groupBy("a")(count("*")), joinType = joinType)
      assertPassOnGlobalWatermarkLimit(
        s"$joinType join after streaming aggregation in Append mode",
        streamRelation.join(streamRelation.groupBy("a")(count("*")), joinType = joinType),
        OutputMode.Append())
    }

    assertPassOnGlobalWatermarkLimit(
      "deduplicate after streaming aggregation in Append mode",
      Deduplicate(Seq(attribute), streamRelation.groupBy("a")(count("*"))), OutputMode.Append())

    assertPassOnGlobalWatermarkLimit(
      "FlatMapGroupsWithState after streaming aggregation in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        streamRelation.groupBy("a")(count("*"))), outputMode = Append)
  }

  // FlatMapGroupsWithState
  {
    assertPassOnGlobalWatermarkLimit(
      "single FlatMapGroupsWithState in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null, streamRelation), outputMode = Append)

    assertFailOnGlobalWatermarkLimit(
      "streaming aggregation after FlatMapGroupsWithState in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, GroupStateTimeout.EventTimeTimeout(),
        streamRelation).groupBy("*")(count("*")), outputMode = Append)

    Seq(Inner, LeftOuter, RightOuter).foreach { joinType =>
      assertFailOnGlobalWatermarkLimit(
        s"stream-stream $joinType after FlatMapGroupsWithState in Append mode",
        streamRelation.join(
          TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, GroupStateTimeout.EventTimeTimeout(),
            streamRelation), joinType = joinType,
          condition = Some(attributeWithWatermark === attribute)), outputMode = Append)
    }

    assertFailOnGlobalWatermarkLimit(
      "FlatMapGroupsWithState after FlatMapGroupsWithState in Append mode",
      TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, GroupStateTimeout.EventTimeTimeout(), streamRelation)),
      outputMode = Append)

    assertPassOnGlobalWatermarkLimit(
      s"deduplicate after FlatMapGroupsWithState in Append mode",
      Deduplicate(Seq(attribute),
        TestFlatMapGroupsWithState(null, att, att, Seq(att), Seq(att), att, null, Append,
          isMapGroupsWithState = false, null, streamRelation)), outputMode = Append)
  }

  // deduplicate
  {
    assertPassOnGlobalWatermarkLimit(
      "streaming aggregation after deduplicate in Append mode",
      Deduplicate(Seq(attribute), streamRelation).groupBy("a")(count("*")), outputMode = Append)

    Seq(Inner, LeftOuter, RightOuter).foreach { joinType =>
      assertPassOnGlobalWatermarkLimit(
        s"$joinType join after deduplicate in Append mode",
        streamRelation.join(Deduplicate(Seq(attribute), streamRelation), joinType = joinType,
          condition = Some(attributeWithWatermark === attribute)), outputMode = Append)
    }

    assertPassOnGlobalWatermarkLimit(
      "FlatMapGroupsWithState after deduplicate in Append mode",
      TestFlatMapGroupsWithState(
        null, att, att, Seq(att), Seq(att), att, null, Append,
        isMapGroupsWithState = false, null,
        Deduplicate(Seq(attribute), streamRelation)), outputMode = Append)

    Seq(Append, Update).foreach { outputMode =>
      assertPassOnGlobalWatermarkLimit(
        s"stream-stream inner join with deduplicate on both sides " +
          s"(with event-time) in ${outputMode} mode",
        Deduplicate(Seq(attributeWithWatermark), streamRelation).join(
          Deduplicate(Seq(attributeWithWatermark), streamRelation),
          joinType = Inner,
          condition = Some(attributeWithWatermark === attribute)),
        outputMode = outputMode)

      assertPassOnGlobalWatermarkLimit(
        s"stream-stream inner join with deduplicate on both sides " +
          s"(without event-time) in ${outputMode} mode",
        Deduplicate(Seq(attribute), streamRelation).join(
          Deduplicate(Seq(attribute), streamRelation),
          joinType = Inner,
          condition = Some(attributeWithWatermark === attribute)),
        outputMode = outputMode)
    }
  }

  /*
    =======================================================================================
                                 REAL-TIME STREAMING
    =======================================================================================
  */

  {
    assertNotSupportedForRealTime(
      "real-time without operators - append mode",
      streamRelation,
      Append,
      "STREAMING_REAL_TIME_MODE.OUTPUT_MODE_NOT_SUPPORTED"
    )

    assertSupportedForRealTime(
      "real-time with stream-batch join - update mode",
      streamRelation.join(batchRelation, joinType = Inner),
      Update
    )
  }

  assertSupportedForRealTime(
    "real-time with Scala transformWithState - update mode",
    scalaTransformWithState(streamRelation),
    Update
  )

  assertSupportedForRealTime(
    "real-time with deduplicate within watermark - update mode",
    DeduplicateWithinWatermark(Seq(attribute), streamRelation),
    Update
  )

  assertSupportedForRealTime(
    "real-time with deduplicate within watermark after union - update mode",
    DeduplicateWithinWatermark(
      Seq(attribute),
      streamRelation.union(new TestStreamingRelation(attribute.newInstance()))),
    Update
  )

  assertNotSupportedForRealTime(
    "real-time with Scala transformWithState on both sides of union - update mode",
    scalaTransformWithState(streamRelation)
      .union(scalaTransformWithState(new TestStreamingRelation(attribute.newInstance()))),
    Update,
    "STREAMING_REAL_TIME_MODE.STATEFUL_OPERATORS_BEFORE_UNION_NOT_SUPPORTED"
  )

  assertNotSupportedForRealTime(
    "real-time with deduplicate within watermark before union - update mode",
    DeduplicateWithinWatermark(Seq(attribute), streamRelation)
      .union(new TestStreamingRelation(attribute.newInstance())),
    Update,
    "STREAMING_REAL_TIME_MODE.STATEFUL_OPERATORS_BEFORE_UNION_NOT_SUPPORTED"
  )

  assertSupportedForRealTime(
    "real-time with batch aggregate before union - update mode",
    streamRelation
      .join(Aggregate(Nil, aggExprs("c"), batchRelation), joinType = Inner)
      .select(attribute)
      .union(new TestStreamingRelation(attribute.newInstance())),
    Update
  )

  private def scalaTransformWithState(child: LogicalPlan): TransformWithState = {
    val statefulProcessor = new StatefulProcessor[Any, Any, Any] {
      override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {}

      override def handleInputRows(
          key: Any,
          inputRows: Iterator[Any],
          timerValues: TimerValues): Iterator[Any] = Iterator.empty
    }
    val keyEncoder = ExpressionEncoder(new StructType().add("a", IntegerType))
      .asInstanceOf[ExpressionEncoder[Any]]
    new TransformWithState(
      keyDeserializer = attribute,
      valueDeserializer = attribute,
      groupingAttributes = Seq(attribute),
      dataAttributes = Seq(attribute),
      statefulProcessor = statefulProcessor,
      timeMode = NoTime,
      outputMode = Update,
      keyEncoder = keyEncoder,
      outputObjAttr = attribute,
      child = child,
      hasInitialState = false,
      initialStateGroupingAttrs = Seq(attribute),
      initialStateDataAttrs = Seq(attribute),
      initialStateDeserializer = attribute,
      initialState = LocalRelation(Seq.empty[Attribute]))
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
      expectedMsg: String = "",
      condition: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    assertNotSupportedInStreamingPlan(
      s"$operationName with stream relation",
      wrapInStreaming(logicalPlanGenerator(streamRelation)),
      outputMode,
      expectedMsgs,
      condition)

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
      assertOutputModeNotSupportedInStreamingPlan(
        s"$outputMode output mode - aggregation without watermark",
        streamRelation.groupBy("a")("count(*)"),
        outputMode,
        "streaming aggregations without watermark")
    }

    // non aggregation
    if (shouldSupportNonAggregation) {
      assertSupportedInStreamingPlan(
        s"$outputMode output mode - no aggregation",
        streamRelation.where($"a" > 1),
        outputMode = outputMode)
    } else {
      assertOutputModeNotSupportedInStreamingPlan(
        s"$outputMode output mode - no aggregation",
        streamRelation.where($"a" > 1),
        outputMode,
        "no streaming aggregations")
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

  /** Assert that the logical plan is supported for real-time mode */
  def assertSupportedForRealTime(name: String, plan: LogicalPlan, outputMode: OutputMode): Unit = {
    test(s"real-time trigger - $name: supported") {
      UnsupportedOperationChecker.checkAdditionalRealTimeModeConstraints(plan, outputMode)
    }
  }

  /**
   * Assert that the logical plan is not supported inside a streaming plan with the
   * real-time trigger.
   */
  def assertNotSupportedForRealTime(
      name: String,
      plan: LogicalPlan,
      outputMode: OutputMode,
      condition: String): Unit = {
    testError(
      s"real-time trigger - $name: not supported",
      Seq("Streaming real-time mode"),
      condition
    ) {
      UnsupportedOperationChecker.checkAdditionalRealTimeModeConstraints(plan, outputMode)
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
      expectedMsgs: Seq[String],
      condition: String = ""): Unit = {
    testError(
      s"streaming plan - $name: not supported",
      expectedMsgs :+ "streaming" :+ "DataFrame" :+ "Dataset" :+ "not supported",
      condition) {
      UnsupportedOperationChecker.checkForStreaming(wrapInStreaming(plan), outputMode)
    }
  }

  /**
   * Assert that output model is not supported in streaming plan.
   */
  def assertOutputModeNotSupportedInStreamingPlan(
      name: String,
      plan: LogicalPlan,
      outputMode: OutputMode,
      operation: String): Unit = {
    test(s"streaming plan - $name: not supported") {
      checkError(
        exception = intercept[AnalysisException] {
          UnsupportedOperationChecker.checkForStreaming(wrapInStreaming(plan), outputMode)
        },
        condition = "STREAMING_OUTPUT_MODE.UNSUPPORTED_OPERATION",
        sqlState = "42KDE",
        parameters = Map(
          "outputMode" -> outputMode.toString.toLowerCase(Locale.ROOT),
          "operation" -> operation))
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
    testGlobalWatermarkLimit(testNamePostfix, plan, expectFailure = false, outputMode)
  }

  def assertFailOnGlobalWatermarkLimit(
      testNamePostfix: String,
      plan: LogicalPlan,
      outputMode: OutputMode): Unit = {
    testGlobalWatermarkLimit(testNamePostfix, plan, expectFailure = true, outputMode)
  }

  def testGlobalWatermarkLimit(
      testNamePostfix: String,
      plan: LogicalPlan,
      expectFailure: Boolean,
      outputMode: OutputMode): Unit = {
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
        UnsupportedOperationChecker.checkStreamingQueryGlobalWatermarkLimit(
          wrapInStreaming(plan), outputMode)
      }
    }
  }

  /**
   * Test whether the body of code will fail. If it does fail, then check if it has expected
   * messages.
   */
  def testError(
      testName: String,
      expectedMsgs: Seq[String],
      condition: String = "")(testBody: => Unit): Unit = {

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
      if (!condition.isEmpty) {
        assert(e.getCondition == condition)
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
