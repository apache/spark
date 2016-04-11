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
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types.IntegerType

class UnsupportedOperationsSuite extends SparkFunSuite {

  val batchRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())

  val streamRelation = new LocalRelation(
    Seq(AttributeReference("a", IntegerType, nullable = true)())) {
    override def isStreaming: Boolean = true
  }

  /** Non-streaming queries */

  testSupportedForBatch("local relation", batchRelation)

  testNotSupportedForBatch(
    "streaming source",
    streamRelation,
    Seq("startStream", "streaming", "source"))

  testNotSupportedForBatch(
    "select on streaming source",
    streamRelation.select($"count(*)"),
    Seq("startStream", "streaming", "source"))


  /** Streaming queries */

  // Commands
  testNotSupportedForStreaming(
    "commmands",
    DescribeFunction("func", true),
    outputMode = Append,
    expectedMsgs = "commands" :: Nil)

  // Aggregates: Not supported on streams in Append mode
  testSupportedForStreaming(
    "aggregate - stream with update output mode",
    batchRelation.groupBy("a")("count(*)"),
    outputMode = Update)

  testSupportedForStreaming(
    "aggregate - batch with update output mode",
    streamRelation.groupBy("a")("count(*)"),
    outputMode = Update)

  testSupportedForStreaming(
    "aggregate - batch with append output mode",
    batchRelation.groupBy("a")("count(*)"),
    outputMode = Append)

  testNotSupportedForStreaming(
    "aggregate - stream with append output mode",
    streamRelation.groupBy("a")("count(*)"),
    outputMode = Append,
    Seq("aggregation", "append output mode"))

  // Inner joins: Stream-stream not supported
  testBinaryOperationForStreaming(
    "inner join",
    _.join(_, joinType = Inner),
    streamStreamSupported = false)

  // Full outer joins: only batch-batch is allowed
  testBinaryOperationForStreaming(
    "full outer join",
    _.join(_, joinType = FullOuter),
    streamStreamSupported = false,
    batchStreamSupported = false,
    streamBatchSupported = false)

  // Left outer joins: *-stream not allowed
  testBinaryOperationForStreaming(
    "left outer join",
    _.join(_, joinType = LeftOuter),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left outer/semi/anti joins")

  // Left semi joins: stream-* not allowed
  testBinaryOperationForStreaming(
    "left semi join",
    _.join(_, joinType = LeftSemi),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left outer/semi/anti joins")

  // Left anti joins: stream-* not allowed
  testBinaryOperationForStreaming(
    "left anti join",
    _.join(_, joinType = LeftAnti),
    streamStreamSupported = false,
    batchStreamSupported = false,
    expectedMsg = "left outer/semi/anti joins")

  // Right outer joins: stream-* not allowed
  testBinaryOperationForStreaming(
    "right outer join",
    _.join(_, joinType = RightOuter),
    streamStreamSupported = false,
    streamBatchSupported = false)

  // Cogroup: only batch-batch is allowed
  testBinaryOperationForStreaming(
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
    CoGroup[Int, Int, Int, Int](
      func,
      AppendColumns[Int, Int]((x: Int) => x, left).newColumns,
      AppendColumns[Int, Int]((x: Int) => x, right).newColumns,
      left.output,
      right.output,
      left,
      right
    )
  }

  // Union
  testBinaryOperationForStreaming(
    "union",
    _.union(_),
    streamBatchSupported = false,
    batchStreamSupported = false)

  // Except: *-stream not supported
  testBinaryOperationForStreaming(
    "except",
    _.except(_),
    streamStreamSupported = false,
    batchStreamSupported = false)

  // Intersect: stream-stream not supported
  testBinaryOperationForStreaming(
    "intersect",
    _.intersect(_),
    streamStreamSupported = false)


  // Unary operations
  testUnaryOperationForStreaming("distinct", Distinct(_))
  testUnaryOperationForStreaming("sort", Sort(Nil, true, _))
  testUnaryOperationForStreaming("sort partitions", SortPartitions(Nil, _), expectedMsg = "sort")
  testUnaryOperationForStreaming("sample", Sample(0.1, 1, true, 1L, _)(), expectedMsg = "sampling")
  testUnaryOperationForStreaming(
    "window", Window(Nil, Nil, Nil, _), expectedMsg = "non-time-based windows")


  // Utility functions

  def testUnaryOperationForStreaming(
    operationName: String,
    logicalPlanGenerator: LogicalPlan => LogicalPlan,
    outputMode: OutputMode = Append,
    expectedMsg: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    testNotSupportedForStreaming(
      s"$operationName with stream relation",
      logicalPlanGenerator(streamRelation),
      outputMode,
      expectedMsgs)

    testSupportedForStreaming(
      s"$operationName with batch relation",
      logicalPlanGenerator(batchRelation),
      outputMode)
  }


  def testBinaryOperationForStreaming(
      operationName: String,
      logicalPlanGenerator: (LogicalPlan, LogicalPlan) => LogicalPlan,
      outputMode: OutputMode = Append,
      streamStreamSupported: Boolean = true,
      streamBatchSupported: Boolean = true,
      batchStreamSupported: Boolean = true,
      expectedMsg: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    if (streamStreamSupported) {
      testSupportedForStreaming(
        s"$operationName with stream-stream relations",
        logicalPlanGenerator(streamRelation, streamRelation),
        outputMode)
    } else {
      testNotSupportedForStreaming(
        s"$operationName with stream-stream relations",
        logicalPlanGenerator(streamRelation, streamRelation),
        outputMode,
        expectedMsgs)
    }

    if (streamBatchSupported) {
      testSupportedForStreaming(
        s"$operationName with stream-batch relations",
        logicalPlanGenerator(streamRelation, batchRelation),
        outputMode)
    } else {
      testNotSupportedForStreaming(
        s"$operationName with stream-batch relations",
        logicalPlanGenerator(streamRelation, batchRelation),
        outputMode,
        expectedMsgs)
    }

    if (batchStreamSupported) {
      testSupportedForStreaming(
        s"$operationName with batch-stream relations",
        logicalPlanGenerator(batchRelation, streamRelation),
        outputMode)
    } else {
      testNotSupportedForStreaming(
        s"$operationName with batch-stream relations",
        logicalPlanGenerator(batchRelation, streamRelation),
        outputMode,
        expectedMsgs)
    }

    testSupportedForStreaming(
      s"$operationName with batch-batch relations",
      logicalPlanGenerator(batchRelation, batchRelation),
      outputMode)
  }



  def testSupportedForStreaming(name: String, plan: LogicalPlan, outputMode: OutputMode): Unit = {
    test(s"streaming plan - $name: supported", plan, shouldBeSupported = true, Nil) {
      UnsupportedOperationChecker.checkForStreaming(plan, outputMode)
    }
  }

  def testNotSupportedForStreaming(
      name: String,
      plan: LogicalPlan,
      outputMode: OutputMode,
      expectedMsgs: Seq[String]): Unit = {
    test(
      s"streaming plan - $name: not supported",
      plan,
      shouldBeSupported = false,
      expectedMsgs :+ "streaming" :+ "DataFrame" :+ "Dataset" :+ "not supported") {
      UnsupportedOperationChecker.checkForStreaming(plan, outputMode)
    }
  }

  def testSupportedForBatch(name: String, plan: LogicalPlan): Unit = {
    test(s"batch plan - $name: supported", plan, shouldBeSupported = true, Nil) {
      UnsupportedOperationChecker.checkForBatch(plan)
    }
  }

  def testNotSupportedForBatch(name: String, plan: LogicalPlan, expectedMsgs: Seq[String]): Unit = {
    test(s"batch plan - $name: not supported", plan, shouldBeSupported = false, expectedMsgs) {
      UnsupportedOperationChecker.checkForBatch(plan)
    }
  }

  def test(
      testName: String,
      plan: LogicalPlan,
      shouldBeSupported: Boolean,
      expectedMsgs: Seq[String])(testBody: => Unit): Unit = {

    test(testName) {
      if (shouldBeSupported) {
        testBody
      } else {
        val e = intercept[AnalysisException] {
          testBody
        }

        if (!expectedMsgs.map(_.toLowerCase).forall(e.getMessage.toLowerCase.contains)) {
          fail(
            s"""Exception message should contain the following substrings:
                |
           |  ${expectedMsgs.mkString("\n  ")}
                |
           |Actual exception message:
                |
           |  ${e.getMessage}
         """.stripMargin)
        }
      }
    }
  }
}
