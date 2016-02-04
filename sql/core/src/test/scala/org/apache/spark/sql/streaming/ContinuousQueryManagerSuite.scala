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

import org.apache.spark.sql.execution.streaming.{MemorySink, MemoryStream, StreamExecution}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{Dataset, ContinuousQuery, StreamTest}

class ContinuousQueryManagerSuite extends StreamTest with SharedSQLContext with BeforeAndAfter {

  import AwaitTerminationTester._
  import testImplicits._

  after {
    assert(sqlContext.streams.active.isEmpty)
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

      q1.stop()

      assert(sqlContext.streams.active.toSet === Set(q2, q3))
      val ex1 = withClue("no error while getting non-active query") {
        intercept[IllegalArgumentException] {
          sqlContext.streams.get(q1.name).eq(q1)
        }
      }
      assert(ex1.getMessage.contains(q1.name), "error does not contain name of query to be fetched")
      assert(sqlContext.streams.get(q2.name).eq(q2))

      m2.addData(0)   // q2 should terminate with error

      eventually(Timeout(streamingTimout)) {
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

  test("awaitAnyTermination") {
    val dss = Seq.fill(5)(makeDataset._2)
    withQueriesOn(dss:_*) { queries =>
      require(queries.size === dss.size)
      assert(sqlContext.streams.active.toSet === queries.toSet)

      testAwaitAnyTermination(ExpectBlocked)
      testAwaitAnyTermination(ExpectBlocked, awaitTimeout = 2 seconds)
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 100 milliseconds, expectedReturnedValue = None)

      val q1 = stopRandomQueryAsync(500 milliseconds)
      testAwaitAnyTermination(ExpectNotBlocked, expectedReturnedValue = q1)
      eventually(Timeout(streamingTimout)) { require(!q1.isActive) }


      val q2 = stopRandomQueryAsync(500 milliseconds)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 2 seconds,
        expectedReturnedValue = Some(q2),
        testTimeout = 4 seconds)
      eventually(Timeout(streamingTimout)) { require(!q2.isActive) }

      val q3 = stopRandomQueryAsync(500 milliseconds)
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 100 milliseconds, expectedReturnedValue = None)
      eventually(Timeout(streamingTimout)) { require(!q3.isActive) }

      assert(sqlContext.streams.active.size === 2)
    }
  }

  private def withQueriesOn(datasets: Dataset[_]*)(body: Seq[ContinuousQuery] => Unit): Unit = {
    failAfter(streamingTimout) {
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

  private def makeDataset: (MemoryStream[Int], Dataset[Int]) = {
    val inputData = MemoryStream[Int]
    val mapped = inputData.toDS.map(6 / _)
    (inputData, mapped)
  }

  private def testAwaitAnyTermination(
      expectedBehavior: ExpectedBehavior,
      expectedReturnedValue: Any = null,
      awaitTimeout: Span = null,
      testTimeout: Span = 1000 milliseconds
    ): Unit = {
    require(expectedBehavior != ExpectNotBlocked || expectedReturnedValue != null,
      "Expected returned value not specified when awaitTermination is expected to be not blocked")

    def awaitTermFunc(): Unit = {
      val returnedValue = if (awaitTimeout != null && awaitTimeout.toMillis > 0) {
        sqlContext.streams.awaitAnyTermination(awaitTimeout.toMillis)
      } else {
        sqlContext.streams.awaitAnyTermination()
      }
      assert(returnedValue === expectedReturnedValue,
        "Returned value does not match expected")
    }

    AwaitTerminationTester.test(expectedBehavior, awaitTermFunc, testTimeout)
  }

  private def stopRandomQueryAsync(delay: Span): ContinuousQuery = {
    import scala.concurrent.ExecutionContext.Implicits.global

    val activeQueries = sqlContext.streams.active
    val queryToStop = activeQueries(Random.nextInt(activeQueries.length))
    Future {
      Thread.sleep(delay.toMillis)
      logDebug(s"Stopping query ${queryToStop.name}")
      queryToStop.stop()
    }
    queryToStop
  }
}
