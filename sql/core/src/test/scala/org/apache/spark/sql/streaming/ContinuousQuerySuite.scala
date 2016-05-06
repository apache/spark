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

package org.apache.spark.sql.streaming

import org.apache.spark.SparkException
import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.execution.streaming.{CompositeOffset, LongOffset, MemoryStream, StreamExecution}
import org.apache.spark.sql.test.SharedSQLContext

class ContinuousQuerySuite extends StreamTest with SharedSQLContext {

  import AwaitTerminationTester._
  import testImplicits._

  testQuietly("lifecycle states and awaitTermination") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map { 6 / _}

    testStream(mapped)(
      AssertOnQuery(_.isActive === true),
      AssertOnQuery(_.exception.isEmpty),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3),
      TestAwaitTermination(ExpectBlocked),
      TestAwaitTermination(ExpectBlocked, timeoutMs = 2000),
      TestAwaitTermination(ExpectNotBlocked, timeoutMs = 10, expectedReturnValue = false),
      StopStream,
      AssertOnQuery(_.isActive === false),
      AssertOnQuery(_.exception.isEmpty),
      TestAwaitTermination(ExpectNotBlocked),
      TestAwaitTermination(ExpectNotBlocked, timeoutMs = 2000, expectedReturnValue = true),
      TestAwaitTermination(ExpectNotBlocked, timeoutMs = 10, expectedReturnValue = true),
      StartStream(),
      AssertOnQuery(_.isActive === true),
      AddData(inputData, 0),
      ExpectFailure[SparkException],
      AssertOnQuery(_.isActive === false),
      TestAwaitTermination(ExpectException[SparkException]),
      TestAwaitTermination(ExpectException[SparkException], timeoutMs = 2000),
      TestAwaitTermination(ExpectException[SparkException], timeoutMs = 10),
      AssertOnQuery(
        q =>
          q.exception.get.startOffset.get === q.committedOffsets.toCompositeOffset(Seq(inputData)),
        "incorrect start offset on exception")
    )
  }

  testQuietly("source and sink statuses") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(6 / _)

    testStream(mapped)(
      AssertOnQuery(_.sourceStatuses.length === 1),
      AssertOnQuery(_.sourceStatuses(0).description.contains("Memory")),
      AssertOnQuery(_.sourceStatuses(0).offset === None),
      AssertOnQuery(_.sinkStatus.description.contains("Memory")),
      AssertOnQuery(_.sinkStatus.offset === new CompositeOffset(None :: Nil)),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3),
      AssertOnQuery(_.sourceStatuses(0).offset === Some(LongOffset(0))),
      AssertOnQuery(_.sinkStatus.offset === CompositeOffset.fill(LongOffset(0))),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3, 6, 3),
      AssertOnQuery(_.sourceStatuses(0).offset === Some(LongOffset(1))),
      AssertOnQuery(_.sinkStatus.offset === CompositeOffset.fill(LongOffset(1))),
      AddData(inputData, 0),
      ExpectFailure[SparkException],
      AssertOnQuery(_.sourceStatuses(0).offset === Some(LongOffset(2))),
      AssertOnQuery(_.sinkStatus.offset === CompositeOffset.fill(LongOffset(1)))
    )
  }

  /**
   * A [[StreamAction]] to test the behavior of `ContinuousQuery.awaitTermination()`.
   *
   * @param expectedBehavior  Expected behavior (not blocked, blocked, or exception thrown)
   * @param timeoutMs         Timeout in milliseconds
   *                          When timeoutMs <= 0, awaitTermination() is tested (i.e. w/o timeout)
   *                          When timeoutMs > 0, awaitTermination(timeoutMs) is tested
   * @param expectedReturnValue Expected return value when awaitTermination(timeoutMs) is used
   */
  case class TestAwaitTermination(
      expectedBehavior: ExpectedBehavior,
      timeoutMs: Int = -1,
      expectedReturnValue: Boolean = false
    ) extends AssertOnQuery(
      TestAwaitTermination.assertOnQueryCondition(expectedBehavior, timeoutMs, expectedReturnValue),
      "Error testing awaitTermination behavior"
    ) {
    override def toString(): String = {
      s"TestAwaitTermination($expectedBehavior, timeoutMs = $timeoutMs, " +
        s"expectedReturnValue = $expectedReturnValue)"
    }
  }

  object TestAwaitTermination {

    /**
     * Tests the behavior of `ContinuousQuery.awaitTermination`.
     *
     * @param expectedBehavior  Expected behavior (not blocked, blocked, or exception thrown)
     * @param timeoutMs         Timeout in milliseconds
     *                          When timeoutMs <= 0, awaitTermination() is tested (i.e. w/o timeout)
     *                          When timeoutMs > 0, awaitTermination(timeoutMs) is tested
     * @param expectedReturnValue Expected return value when awaitTermination(timeoutMs) is used
     */
    def assertOnQueryCondition(
        expectedBehavior: ExpectedBehavior,
        timeoutMs: Int,
        expectedReturnValue: Boolean
      )(q: StreamExecution): Boolean = {

      def awaitTermFunc(): Unit = {
        if (timeoutMs <= 0) {
          q.awaitTermination()
        } else {
          val returnedValue = q.awaitTermination(timeoutMs)
          assert(returnedValue === expectedReturnValue, "Returned value does not match expected")
        }
      }
      AwaitTerminationTester.test(expectedBehavior, awaitTermFunc)
      true // If the control reached here, then everything worked as expected
    }
  }
}
