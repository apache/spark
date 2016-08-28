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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.InternalOutputModes._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, NamedExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.IntegerType

/** A dummy command for testing unsupported operations. */
case class DummyCommand() extends LogicalPlan with Command {
  override def output: Seq[Attribute] = Nil
  override def children: Seq[LogicalPlan] = Nil
}

class UnsupportedOperationsSuite extends SparkFunSuite {

  val attribute = AttributeReference("a", IntegerType, nullable = true)()
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

  // Inner joins: Stream-stream not supported
  testBinaryOperationInStreamingPlan(
    "inner join",
    _.join(_, joinType = Inner),
    streamStreamSupported = false)

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
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left outer/semi/anti joins")

  // Left semi joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "left semi join",
    _.join(_, joinType = LeftSemi),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left outer/semi/anti joins")

  // Left anti joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "left anti join",
    _.join(_, joinType = LeftAnti),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left outer/semi/anti joins")

  // Right outer joins: stream-* not allowed
  testBinaryOperationInStreamingPlan(
    "right outer join",
    _.join(_, joinType = RightOuter),
    streamStreamSupported = false,
    streamBatchSupported = false)

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
    _.except(_),
    streamStreamSupported = false,
    batchStreamSupported = false)

  // Intersect: stream-stream not supported
  testBinaryOperationInStreamingPlan(
    "intersect",
    _.intersect(_),
    streamStreamSupported = false)

  // Sort: supported only on batch subplans and on aggregation + complete output mode
  testUnaryOperatorInStreamingPlan("sort", Sort(Nil, true, _))
  assertSupportedInStreamingPlan(
    "sort - sort over aggregated data in Complete output mode",
    streamRelation.groupBy()(Count("*")).sortBy(),
    Complete)
  assertNotSupportedInStreamingPlan(
    "sort - sort over aggregated data in Update output mode",
    streamRelation.groupBy()(Count("*")).sortBy(),
    Update,
    Seq("sort", "aggregat", "complete")) // sort on aggregations is supported on Complete mode only


  // Other unary operations
  testUnaryOperatorInStreamingPlan("sort partitions", SortPartitions(Nil, _), expectedMsg = "sort")
  testUnaryOperatorInStreamingPlan(
    "sample", Sample(0.1, 1, true, 1L, _)(), expectedMsg = "sampling")
  testUnaryOperatorInStreamingPlan(
    "window", Window(Nil, Nil, Nil, _), expectedMsg = "non-time-based windows")

  // Output modes with aggregation and non-aggregation plans
  testOutputMode(Append, shouldSupportAggregation = false)
  testOutputMode(Update, shouldSupportAggregation = true)
  testOutputMode(Complete, shouldSupportAggregation = true)

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
      shouldSupportAggregation: Boolean): Unit = {

    // aggregation
    if (shouldSupportAggregation) {
      assertNotSupportedInStreamingPlan(
        s"$outputMode output mode - no aggregation",
        streamRelation.where($"a" > 1),
        outputMode = outputMode,
        Seq("aggregation", s"$outputMode output mode"))

      assertSupportedInStreamingPlan(
        s"$outputMode output mode - aggregation",
        streamRelation.groupBy("a")("count(*)"),
        outputMode = outputMode)

    } else {
      assertSupportedInStreamingPlan(
        s"$outputMode output mode - no aggregation",
        streamRelation.where($"a" > 1),
        outputMode = outputMode)

      assertNotSupportedInStreamingPlan(
        s"$outputMode output mode - aggregation",
        streamRelation.groupBy("a")("count(*)"),
        outputMode = outputMode,
        Seq("aggregation", s"$outputMode output mode"))
    }
  }

  /**
   * Assert that the logical plan is supported as subplan insider a streaming plan.
   *
   * To test this correctly, the given logical plan is wrapped in a fake operator that makes the
   * whole plan look like a streaming plan. Otherwise, a batch plan may throw not supported
   * exception simply for not being a streaming plan, even though that plan could exists as batch
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

  /**
   * Assert that the logical plan is not supported inside a streaming plan.
   *
   * To test this correctly, the given logical plan is wrapped in a fake operator that makes the
   * whole plan look like a streaming plan. Otherwise, a batch plan may throw not supported
   * exception simply for not being a streaming plan, even though that plan could exists as batch
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
        if (!e.getMessage.toLowerCase.contains(m.toLowerCase)) {
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
}
