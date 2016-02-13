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

import scala.concurrent.Future
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.BeforeAndAfter
import org.scalatest.concurrent.Eventually._
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.{ContinuousQuery, Dataset, StreamTest}
import org.apache.spark.sql.execution.streaming.{MemorySink, MemoryStream, StreamExecution, StreamingRelation}
import org.apache.spark.sql.test.SharedSQLContext

class ContinuousQueryManagerSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import AwaitTerminationTester._
  import testImplicits._

  override val streamingTimeout = 20.seconds

  before {
    assert(sqlContext.streams.active.isEmpty)
    sqlContext.streams.resetTerminated()
  }

  after {
    assert(sqlContext.streams.active.isEmpty)
    sqlContext.streams.resetTerminated()
  }

  test("listing") {
    val (m1, ds1) = makeDataset
    val (m2, ds2) = makeDataset
    val (m3, ds3) = makeDataset

    withQueriesOn(ds1, ds2, ds3) { queries =>
      require(queries.size === 3)
      assert(sqlContext.streams.active.toSet === queries.toSet)
      val (q1, q2, q3) = (queries(0), queries(1), queries(2))

      assert(sqlContext.streams.get(q1.name).eq(q1))
      assert(sqlContext.streams.get(q2.name).eq(q2))
      assert(sqlContext.streams.get(q3.name).eq(q3))
      intercept[IllegalArgumentException] {
        sqlContext.streams.get("non-existent-name")
      }

      q1.stop()

      assert(sqlContext.streams.active.toSet === Set(q2, q3))
      val ex1 = withClue("no error while getting non-active query") {
        intercept[IllegalArgumentException] {
          sqlContext.streams.get(q1.name)
        }
      }
      assert(ex1.getMessage.contains(q1.name), "error does not contain name of query to be fetched")
      assert(sqlContext.streams.get(q2.name).eq(q2))

      m2.addData(0)   // q2 should terminate with error

      eventually(Timeout(streamingTimeout)) {
        require(!q2.isActive)
        require(q2.exception.isDefined)
      }
      val ex2 = withClue("no error while getting non-active query") {
        intercept[IllegalArgumentException] {
          sqlContext.streams.get(q2.name).eq(q2)
        }
      }

      assert(sqlContext.streams.active.toSet === Set(q3))
    }
  }

  test("awaitAnyTermination without timeout and resetTerminated") {
    val datasets = Seq.fill(5)(makeDataset._2)
    withQueriesOn(datasets: _*) { queries =>
      require(queries.size === datasets.size)
      assert(sqlContext.streams.active.toSet === queries.toSet)

      // awaitAnyTermination should be blocking
      testAwaitAnyTermination(ExpectBlocked)

      // Stop a query asynchronously and see if it is reported through awaitAnyTermination
      val q1 = stopRandomQueryAsync(stopAfter = 100 milliseconds, withError = false)
      testAwaitAnyTermination(ExpectNotBlocked)
      require(!q1.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should be non-blocking
      testAwaitAnyTermination(ExpectNotBlocked)

      // Resetting termination should make awaitAnyTermination() blocking again
      sqlContext.streams.resetTerminated()
      testAwaitAnyTermination(ExpectBlocked)

      // Terminate a query asynchronously with exception and see awaitAnyTermination throws
      // the exception
      val q2 = stopRandomQueryAsync(100 milliseconds, withError = true)
      testAwaitAnyTermination(ExpectException[SparkException])
      require(!q2.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should throw the exception
      testAwaitAnyTermination(ExpectException[SparkException])

      // Resetting termination should make awaitAnyTermination() blocking again
      sqlContext.streams.resetTerminated()
      testAwaitAnyTermination(ExpectBlocked)

      // Terminate multiple queries, one with failure and see whether awaitAnyTermination throws
      // the exception
      val q3 = stopRandomQueryAsync(10 milliseconds, withError = false)
      testAwaitAnyTermination(ExpectNotBlocked)
      require(!q3.isActive)
      val q4 = stopRandomQueryAsync(10 milliseconds, withError = true)
      eventually(Timeout(streamingTimeout)) { require(!q4.isActive) }
      // After q4 terminates with exception, awaitAnyTerm should start throwing exception
      testAwaitAnyTermination(ExpectException[SparkException])
    }
  }

  test("awaitAnyTermination with timeout and resetTerminated") {
    val datasets = Seq.fill(6)(makeDataset._2)
    withQueriesOn(datasets: _*) { queries =>
      require(queries.size === datasets.size)
      assert(sqlContext.streams.active.toSet === queries.toSet)

      // awaitAnyTermination should be blocking or non-blocking depending on timeout values
      testAwaitAnyTermination(
        ExpectBlocked,
        awaitTimeout = 2 seconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1 second)

      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 50 milliseconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1 second)

      // Stop a query asynchronously within timeout and awaitAnyTerm should be unblocked
      val q1 = stopRandomQueryAsync(stopAfter = 100 milliseconds, withError = false)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 1 second,
        expectedReturnedValue = true,
        testBehaviorFor = 2 seconds)
      require(!q1.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should be non-blocking even if timeout is high
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 2 seconds, expectedReturnedValue = true)

      // Resetting termination should make awaitAnyTermination() blocking again
      sqlContext.streams.resetTerminated()
      testAwaitAnyTermination(
        ExpectBlocked,
        awaitTimeout = 2 seconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1 second)

      // Terminate a query asynchronously with exception within timeout, awaitAnyTermination should
      // throws the exception
      val q2 = stopRandomQueryAsync(100 milliseconds, withError = true)
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 1 second,
        testBehaviorFor = 2 seconds)
      require(!q2.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should throw the exception
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 1 second,
        testBehaviorFor = 2 seconds)

      // Terminate a query asynchronously outside the timeout, awaitAnyTerm should be blocked
      sqlContext.streams.resetTerminated()
      val q3 = stopRandomQueryAsync(1 second, withError = true)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 100 milliseconds,
        expectedReturnedValue = false,
        testBehaviorFor = 2 seconds)

      // After that query is stopped, awaitAnyTerm should throw exception
      eventually(Timeout(streamingTimeout)) { require(!q3.isActive) } // wait for query to stop
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 100 milliseconds,
        testBehaviorFor = 2 seconds)


      // Terminate multiple queries, one with failure and see whether awaitAnyTermination throws
      // the exception
      sqlContext.streams.resetTerminated()

      val q4 = stopRandomQueryAsync(10 milliseconds, withError = false)
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 1 second, expectedReturnedValue = true)
      require(!q4.isActive)
      val q5 = stopRandomQueryAsync(10 milliseconds, withError = true)
      eventually(Timeout(streamingTimeout)) { require(!q5.isActive) }
      // After q5 terminates with exception, awaitAnyTerm should start throwing exception
      testAwaitAnyTermination(ExpectException[SparkException], awaitTimeout = 100 milliseconds)
    }
  }


  /** Run a body of code by defining a query each on multiple datasets */
  private def withQueriesOn(datasets: Dataset[_]*)(body: Seq[ContinuousQuery] => Unit): Unit = {
    failAfter(streamingTimeout) {
      val queries = withClue("Error starting queries") {
        datasets.map { ds =>
          @volatile var query: StreamExecution = null
          try {
            val df = ds.toDF
            query = sqlContext
              .streams
              .startQuery(StreamExecution.nextName, df, new MemorySink(df.schema))
              .asInstanceOf[StreamExecution]
          } catch {
            case NonFatal(e) =>
              if (query != null) query.stop()
              throw e
          }
          query
        }
      }
      try {
        body(queries)
      } finally {
        queries.foreach(_.stop())
      }
    }
  }

  /** Test the behavior of awaitAnyTermination */
  private def testAwaitAnyTermination(
      expectedBehavior: ExpectedBehavior,
      expectedReturnedValue: Boolean = false,
      awaitTimeout: Span = null,
      testBehaviorFor: Span = 2 seconds
    ): Unit = {

    def awaitTermFunc(): Unit = {
      if (awaitTimeout != null && awaitTimeout.toMillis > 0) {
        val returnedValue = sqlContext.streams.awaitAnyTermination(awaitTimeout.toMillis)
        assert(returnedValue === expectedReturnedValue, "Returned value does not match expected")
      } else {
        sqlContext.streams.awaitAnyTermination()
      }
    }

    AwaitTerminationTester.test(expectedBehavior, awaitTermFunc, testBehaviorFor)
  }

  /** Stop a random active query either with `stop()` or with an error */
  private def stopRandomQueryAsync(stopAfter: Span, withError: Boolean): ContinuousQuery = {

    import scala.concurrent.ExecutionContext.Implicits.global

    val activeQueries = sqlContext.streams.active
    val queryToStop = activeQueries(Random.nextInt(activeQueries.length))
    Future {
      Thread.sleep(stopAfter.toMillis)
      if (withError) {
        logDebug(s"Terminating query ${queryToStop.name} with error")
        queryToStop.asInstanceOf[StreamExecution].logicalPlan.collect {
          case StreamingRelation(memoryStream, _) =>
            memoryStream.asInstanceOf[MemoryStream[Int]].addData(0)
        }
      } else {
        logDebug(s"Stopping query ${queryToStop.name}")
        queryToStop.stop()
      }
    }
    queryToStop
  }

  private def makeDataset: (MemoryStream[Int], Dataset[Int]) = {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS.map(6 / _)
    (inputData, mapped)
  }
}
