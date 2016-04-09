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

  // Non-streaming queries

  testSupported(
    "local relation",
    batchRelation,
    forStreaming = false)

  testNonSupported(
    "streaming source",
    streamRelation,
    forStreaming = false,
    Seq("startStream", "streaming", "source"))

  testNonSupported(
    "select on streaming source",
    streamRelation.select($"count(*)"),
    forStreaming = false,
    "startStream" :: "streaming" :: "source" :: Nil)


  // Inner joins
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
  testUnaryOperationForStreaming("sort partitions", SortPartitions(Nil, _), "sort")
  testUnaryOperationForStreaming("sample", Sample(0.1, 1.0, true, 1L, _)(), "sampling")
  testUnaryOperationForStreaming("window", Window(Nil, Nil, Nil, _), "non-time-based windows")


  // Utility functions

  def testUnaryOperationForStreaming(
    operationName: String,
    logicalPlanGenerator: LogicalPlan => LogicalPlan,
    expectedMsg: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    testNotSupportedForStreaming(
      s"$operationName - batch supported",
      logicalPlanGenerator(streamRelation),
      expectedMsgs)

    testSupportedForStreaming(
      s"$operationName - stream not supported",
      logicalPlanGenerator(batchRelation))
  }


  def testBinaryOperationForStreaming(
      operationName: String,
      logicalPlanGenerator: (LogicalPlan, LogicalPlan) => LogicalPlan,
      streamStreamSupported: Boolean = true,
      streamBatchSupported: Boolean = true,
      batchStreamSupported: Boolean = true,
      expectedMsg: String = ""): Unit = {

    val expectedMsgs = if (expectedMsg.isEmpty) Seq(operationName) else Seq(expectedMsg)

    if (streamStreamSupported) {
      testSupportedForStreaming(
        s"$operationName - stream-stream supported",
        logicalPlanGenerator(streamRelation, streamRelation))
    } else {
      testNotSupportedForStreaming(
        s"$operationName - stream-stream not supported",
        logicalPlanGenerator(streamRelation, streamRelation),
        expectedMsgs)
    }

    if (streamBatchSupported) {
      testSupportedForStreaming(
        s"$operationName - stream-batch supported",
        logicalPlanGenerator(streamRelation, batchRelation))
    } else {
      testNotSupportedForStreaming(
        s"$operationName - stream-batch not supported",
        logicalPlanGenerator(streamRelation, batchRelation),
        expectedMsgs)
    }

    if (batchStreamSupported) {
      testSupportedForStreaming(
        s"$operationName - batch-stream supported",
        logicalPlanGenerator(batchRelation, streamRelation))
    } else {
      testNotSupportedForStreaming(
        s"$operationName - batch-stream not supported",
        logicalPlanGenerator(batchRelation, streamRelation),
        expectedMsgs)
    }

    testSupportedForStreaming(
      s"$operationName - batch-batch supported",
      logicalPlanGenerator(batchRelation, batchRelation))
  }

  def testNotSupportedForStreaming(
    name: String,
    plan: LogicalPlan,
    expectedMsgs: Seq[String]): Unit = {
    testNonSupported(
      name,
      plan,
      forStreaming = true,
      expectedMsgs :+ "streaming" :+ "DataFrame" :+ "Dataset" :+ "not supported")
  }

  def testSupportedForStreaming(
    name: String,
    plan: LogicalPlan): Unit = {
    testSupported(name, plan, forStreaming = true)
  }

  def testNonSupported(
    name: String,
    plan: LogicalPlan,
    forStreaming: Boolean,
    expectedMsgs: Seq[String]): Unit = {
    val testName = if (forStreaming) {
      s"streaming plan - $name"
    } else {
      s"batch plan - $name"
    }

    test(testName) {
      val e = intercept[AnalysisException] {
        UnsupportedOperationChecker.check(plan, forStreaming)
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

  def testSupported(
      name: String,
      plan: LogicalPlan,
      forStreaming: Boolean): Unit = {
    val testName = if (forStreaming) {
      s"streaming plan - $name"
    } else {
      s"batch plan - $name"
    }

    test(testName) {
      UnsupportedOperationChecker.check(plan, forStreaming) // should not throw exception
    }
  }
}
