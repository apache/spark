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

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.execution.streaming.{CompositeOffset, LongOffset, MemoryStream, StreamExecution}
import org.apache.spark.util.Utils


class StreamingQuerySuite extends StreamTest with BeforeAndAfter {

  import AwaitTerminationTester._
  import testImplicits._

  after {
    sqlContext.streams.active.foreach(_.stop())
  }

  test("names unique across active queries, ids unique across all started queries") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map { 6 / _}

    def startQuery(queryName: String): StreamingQuery = {
      val metadataRoot = Utils.createTempDir(namePrefix = "streaming.checkpoint").getCanonicalPath
      val writer = mapped.writeStream
      writer
        .queryName(queryName)
        .format("memory")
        .option("checkpointLocation", metadataRoot)
        .start()
    }

    val q1 = startQuery("q1")
    assert(q1.name === "q1")

    // Verify that another query with same name cannot be started
    val e1 = intercept[IllegalArgumentException] {
      startQuery("q1")
    }
    Seq("q1", "already active").foreach { s => assert(e1.getMessage.contains(s)) }

    // Verify q1 was unaffected by the above exception and stop it
    assert(q1.isActive)
    q1.stop()

    // Verify another query can be started with name q1, but will have different id
    val q2 = startQuery("q1")
    assert(q2.name === "q1")
    assert(q2.id !== q1.id)
    q2.stop()
  }

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
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === None),
      AssertOnQuery(_.sinkStatus.description.contains("Memory")),
      AssertOnQuery(_.sinkStatus.offsetDesc === new CompositeOffset(None :: Nil).toString),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === Some(LongOffset(0).toString)),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(0)).toString),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3, 6, 3),
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === Some(LongOffset(1).toString)),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(1)).toString),
      AddData(inputData, 0),
      ExpectFailure[SparkException],
      AssertOnQuery(_.sourceStatuses(0).offsetDesc === Some(LongOffset(2).toString)),
      AssertOnQuery(_.sinkStatus.offsetDesc === CompositeOffset.fill(LongOffset(1)).toString)
    )
  }

  testQuietly("StreamExecution metadata garbarge collection") {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS().map(6 / _)

    // Run a few batches through the application
    testStream(mapped)(
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3),
      AddData(inputData, 1, 2),
      CheckAnswer(6, 3, 6, 3),
      AddData(inputData, 4, 6),
      CheckAnswer(6, 3, 6, 3, 1, 1),

      // Three batches have run, but only one set of metadata should be present
      AssertOnQuery(
        q => {
          val metadataLogDir = new java.io.File(q.offsetLog.metadataPath.toString)
          val logFileNames = metadataLogDir.listFiles().toSeq.map(_.getName())
          val toTest = logFileNames.filter(! _.endsWith(".crc")) // Workaround for SPARK-17475
          toTest.size == 1 && toTest.head == "2"
          true
        }
      )
    )
  }

  /**
   * A [[StreamAction]] to test the behavior of `StreamingQuery.awaitTermination()`.
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
     * Tests the behavior of `StreamingQuery.awaitTermination`.
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
