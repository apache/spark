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

import java.io.File
import java.util.concurrent.CountDownLatch

import scala.concurrent.Future
import scala.util.Random
import scala.util.control.NonFatal

import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.BlockingSource
import org.apache.spark.tags.SlowSQLTest
import org.apache.spark.util.Utils

@SlowSQLTest
class StreamingQueryManagerSuite extends StreamTest {

  import AwaitTerminationTester._
  import testImplicits._

  override val streamingTimeout = 20.seconds

  override def beforeEach(): Unit = {
    super.beforeEach()
    assert(spark.streams.active.isEmpty)
    spark.streams.resetTerminated()
  }

  override def afterEach(): Unit = {
    try {
      assert(spark.streams.active.isEmpty)
      spark.streams.resetTerminated()
    } finally {
      super.afterEach()
    }
  }

  testQuietly("listing") {
    val (m1, ds1) = makeDataset
    val (m2, ds2) = makeDataset
    val (m3, ds3) = makeDataset

    withQueriesOn(ds1, ds2, ds3) { queries =>
      require(queries.size === 3)
      assert(spark.streams.active.toSet === queries.toSet)
      val (q1, q2, q3) = (queries(0), queries(1), queries(2))

      assert(spark.streams.get(q1.id).eq(q1))
      assert(spark.streams.get(q2.id).eq(q2))
      assert(spark.streams.get(q3.id).eq(q3))
      assert(spark.streams.get(java.util.UUID.randomUUID()) === null) // non-existent id
      q1.stop()

      assert(spark.streams.active.toSet === Set(q2, q3))
      assert(spark.streams.get(q1.id) === null)
      assert(spark.streams.get(q2.id).eq(q2))

      m2.addData(0)   // q2 should terminate with error

      eventually(Timeout(streamingTimeout)) {
        require(!q2.isActive)
        require(q2.exception.isDefined)
        assert(spark.streams.get(q2.id) === null)
        assert(spark.streams.active.toSet === Set(q3))
      }
    }
  }

  testRetry("awaitAnyTermination without timeout and resetTerminated") {
    val datasets = Seq.fill(5)(makeDataset._2)
    withQueriesOn(datasets: _*) { queries =>
      require(queries.size === datasets.size)
      assert(spark.streams.active.toSet === queries.toSet)

      // awaitAnyTermination should be blocking
      testAwaitAnyTermination(ExpectBlocked)

      // Stop a query asynchronously and see if it is reported through awaitAnyTermination
      val q1 = stopRandomQueryAsync(stopAfter = 100.milliseconds, withError = false)
      testAwaitAnyTermination(ExpectNotBlocked)
      require(!q1.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should be non-blocking
      testAwaitAnyTermination(ExpectNotBlocked)

      // Resetting termination should make awaitAnyTermination() blocking again
      spark.streams.resetTerminated()
      testAwaitAnyTermination(ExpectBlocked)

      // Terminate a query asynchronously with exception and see awaitAnyTermination throws
      // the exception
      val q2 = stopRandomQueryAsync(100.milliseconds, withError = true)
      testAwaitAnyTermination(ExpectException[SparkException])
      require(!q2.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should throw the exception
      testAwaitAnyTermination(ExpectException[SparkException])

      // Resetting termination should make awaitAnyTermination() blocking again
      spark.streams.resetTerminated()
      testAwaitAnyTermination(ExpectBlocked)

      // Terminate multiple queries, one with failure and see whether awaitAnyTermination throws
      // the exception
      val q3 = stopRandomQueryAsync(10.milliseconds, withError = false)
      testAwaitAnyTermination(ExpectNotBlocked)
      require(!q3.isActive)
      val q4 = stopRandomQueryAsync(10.milliseconds, withError = true)
      eventually(Timeout(streamingTimeout)) { require(!q4.isActive) }
      // After q4 terminates with exception, awaitAnyTerm should start throwing exception
      testAwaitAnyTermination(ExpectException[SparkException])
    }
  }

  testQuietly("awaitAnyTermination with timeout and resetTerminated") {
    val datasets = Seq.fill(6)(makeDataset._2)
    withQueriesOn(datasets: _*) { queries =>
      require(queries.size === datasets.size)
      assert(spark.streams.active.toSet === queries.toSet)

      // awaitAnyTermination should be blocking or non-blocking depending on timeout values
      testAwaitAnyTermination(
        ExpectBlocked,
        awaitTimeout = 4.seconds,
        expectedReturnedValue = false,
        testBehaviorFor = 2.seconds)

      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 50.milliseconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1.second)

      // Stop a query asynchronously within timeout and awaitAnyTerm should be unblocked
      val q1 = stopRandomQueryAsync(stopAfter = 100.milliseconds, withError = false)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 2.seconds,
        expectedReturnedValue = true,
        testBehaviorFor = 4.seconds)
      require(!q1.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should be non-blocking even if timeout is high
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 4.seconds, expectedReturnedValue = true)

      // Resetting termination should make awaitAnyTermination() blocking again
      spark.streams.resetTerminated()
      testAwaitAnyTermination(
        ExpectBlocked,
        awaitTimeout = 4.seconds,
        expectedReturnedValue = false,
        testBehaviorFor = 1.second)

      // Terminate a query asynchronously with exception within timeout, awaitAnyTermination should
      // throws the exception
      val q2 = stopRandomQueryAsync(100.milliseconds, withError = true)
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 4.seconds,
        testBehaviorFor = 6.seconds)
      require(!q2.isActive) // should be inactive by the time the prev awaitAnyTerm returned

      // All subsequent calls to awaitAnyTermination should throw the exception
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 2.seconds,
        testBehaviorFor = 4.seconds)

      // Terminate a query asynchronously outside the timeout, awaitAnyTerm should be blocked
      spark.streams.resetTerminated()
      val q3 = stopRandomQueryAsync(2.seconds, withError = true)
      testAwaitAnyTermination(
        ExpectNotBlocked,
        awaitTimeout = 100.milliseconds,
        expectedReturnedValue = false,
        testBehaviorFor = 4.seconds)

      // After that query is stopped, awaitAnyTerm should throw exception
      eventually(Timeout(streamingTimeout)) { require(!q3.isActive) } // wait for query to stop
      // When `isActive` becomes `false`, `StreamingQueryManager` may not receive the error yet.
      // Hence, call `stop` to wait until the thread of `q3` exits so that we can ensure
      // `StreamingQueryManager` has already received the error.
      q3.stop()
      testAwaitAnyTermination(
        ExpectException[SparkException],
        awaitTimeout = 100.milliseconds,
        testBehaviorFor = 4.seconds)


      // Terminate multiple queries, one with failure and see whether awaitAnyTermination throws
      // the exception
      spark.streams.resetTerminated()

      val q4 = stopRandomQueryAsync(10.milliseconds, withError = false)
      testAwaitAnyTermination(
        ExpectNotBlocked, awaitTimeout = 2.seconds, expectedReturnedValue = true)
      require(!q4.isActive)
      val q5 = stopRandomQueryAsync(10.milliseconds, withError = true)
      eventually(Timeout(streamingTimeout)) { require(!q5.isActive) }
      // When `isActive` becomes `false`, `StreamingQueryManager` may not receive the error yet.
      // Hence, call `stop` to wait until the thread of `q5` exits so that we can ensure
      // `StreamingQueryManager` has already received the error.
      q5.stop()
      // After q5 terminates with exception, awaitAnyTerm should start throwing exception
      testAwaitAnyTermination(ExpectException[SparkException], awaitTimeout = 2.seconds)
    }
  }

  test("SPARK-18811: Source resolution should not block main thread") {
    failAfter(streamingTimeout) {
      BlockingSource.latch = new CountDownLatch(1)
      withTempDir { tempDir =>
        // if source resolution was happening on the main thread, it would block the start call,
        // now it should only be blocking the stream execution thread
        val sq = spark.readStream
          .format("org.apache.spark.sql.streaming.util.BlockingSource")
          .load()
          .writeStream
          .format("org.apache.spark.sql.streaming.util.BlockingSource")
          .option("checkpointLocation", tempDir.toString)
          .start()
        eventually(Timeout(streamingTimeout)) {
          assert(sq.status.message.contains("Initializing sources"))
        }
        BlockingSource.latch.countDown()
        sq.stop()
      }
    }
  }

  testQuietly("can't start a streaming query with the same name in the same session") {
    val ds1 = makeDataset._2
    val ds2 = makeDataset._2
    val queryName = "abc"

    val query1 = ds1.writeStream.format("noop").queryName(queryName).start()
    try {
      val e = intercept[IllegalArgumentException] {
        ds2.writeStream.format("noop").queryName(queryName).start()
      }
      assert(e.getMessage.contains("query with that name is already active"))
    } finally {
      query1.stop()
    }
  }

  testQuietly("can start a streaming query with the same name in a different session") {
    val session2 = spark.cloneSession()

    val ds1 = MemoryStream(Encoders.INT, spark.sqlContext).toDS()
    val ds2 = MemoryStream(Encoders.INT, session2.sqlContext).toDS()
    val queryName = "abc"

    val query1 = ds1.writeStream.format("noop").queryName(queryName).start()
    val query2 = ds2.writeStream.format("noop").queryName(queryName).start()

    query1.stop()
    query2.stop()
  }

  testQuietly("can't start multiple instances of the same streaming query in the same session") {
    withSQLConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key -> "false") {
      withTempDir { dir =>
        val (ms1, ds1) = makeDataset
        val (ms2, ds2) = makeDataset
        val chkLocation = new File(dir, "_checkpoint").getCanonicalPath
        val dataLocation = new File(dir, "data").getCanonicalPath

        val query1 = ds1.writeStream.format("parquet")
          .option("checkpointLocation", chkLocation).start(dataLocation)
        ms1.addData(1, 2, 3)
        try {
          val e = intercept[IllegalStateException] {
            ds2.writeStream.format("parquet")
              .option("checkpointLocation", chkLocation).start(dataLocation)
          }
          assert(e.getMessage.contains("same id"))
        } finally {
          spark.streams.active.foreach(_.stop())
        }
      }
    }
  }

  testQuietly("new instance of the same streaming query stops old query in the same session") {
    failAfter(90 seconds) {
      withSQLConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key -> "true") {
        withTempDir { dir =>
          val (ms1, ds1) = makeDataset
          val (ms2, ds2) = makeDataset
          val chkLocation = new File(dir, "_checkpoint").getCanonicalPath
          val dataLocation = new File(dir, "data").getCanonicalPath

          val query1 = ds1.writeStream.format("parquet")
            .option("checkpointLocation", chkLocation).start(dataLocation)
          ms1.addData(1, 2, 3)
          val query2 = ds2.writeStream.format("parquet")
            .option("checkpointLocation", chkLocation).start(dataLocation)
          try {
            ms2.addData(1, 2, 3)
            query2.processAllAvailable()
            assert(spark.sharedState.activeStreamingQueries.get(query2.id) ===
              query2.asInstanceOf[StreamingQueryWrapper].streamingQuery,
              "The correct streaming query is not being tracked in global state")

            assert(!query1.isActive,
              "First query should have stopped before starting the second query")
          } finally {
            spark.streams.active.foreach(_.stop())
          }
        }
      }
    }
  }

  testQuietly(
    "can't start multiple instances of the same streaming query in the different sessions") {
    withSQLConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key -> "false") {
      withTempDir { dir =>
        val session2 = spark.cloneSession()

        val ms1 = MemoryStream(Encoders.INT, spark.sqlContext)
        val ds2 = MemoryStream(Encoders.INT, session2.sqlContext).toDS()
        val chkLocation = new File(dir, "_checkpoint").getCanonicalPath
        val dataLocation = new File(dir, "data").getCanonicalPath

        val query1 = ms1.toDS().writeStream.format("parquet")
          .option("checkpointLocation", chkLocation).start(dataLocation)
        ms1.addData(1, 2, 3)
        try {
          val e = intercept[IllegalStateException] {
            ds2.writeStream.format("parquet")
              .option("checkpointLocation", chkLocation).start(dataLocation)
          }
          assert(e.getMessage.contains("same id"))
        } finally {
          spark.streams.active.foreach(_.stop())
          session2.streams.active.foreach(_.stop())
        }
      }
    }
  }

  testQuietly(
    "new instance of the same streaming query stops old query in a different session") {
    failAfter(90 seconds) {
      withSQLConf(SQLConf.STREAMING_STOP_ACTIVE_RUN_ON_RESTART.key -> "true") {
        withTempDir { dir =>
          val session2 = spark.cloneSession()

          val ms1 = MemoryStream(Encoders.INT, spark.sqlContext)
          val ds2 = MemoryStream(Encoders.INT, session2.sqlContext).toDS()
          val chkLocation = new File(dir, "_checkpoint").getCanonicalPath
          val dataLocation = new File(dir, "data").getCanonicalPath

          val query1 = ms1.toDS().writeStream.format("parquet")
            .option("checkpointLocation", chkLocation).start(dataLocation)
          ms1.addData(1, 2, 3)
          val query2 = ds2.writeStream.format("parquet")
            .option("checkpointLocation", chkLocation).start(dataLocation)
          try {
            ms1.addData(1, 2, 3)
            query2.processAllAvailable()
            assert(spark.sharedState.activeStreamingQueries.get(query2.id) ===
              query2.asInstanceOf[StreamingQueryWrapper].streamingQuery,
              "The correct streaming execution is not being tracked in global state")

            assert(!query1.isActive,
              "First query should have stopped before starting the second query")
          } finally {
            spark.streams.active.foreach(_.stop())
            session2.streams.active.foreach(_.stop())
          }
        }
      }
    }
  }

  /** Run a body of code by defining a query on each dataset */
  private def withQueriesOn(datasets: Dataset[_]*)(body: Seq[StreamingQuery] => Unit): Unit = {
    failAfter(streamingTimeout) {
      val queries = withClue("Error starting queries") {
        datasets.zipWithIndex.map { case (ds, i) =>
          var query: StreamingQuery = null
          try {
            val df = ds.toDF
            val metadataRoot =
              Utils.createTempDir(namePrefix = "streaming.checkpoint").getCanonicalPath
            query =
              df.writeStream
                .format("memory")
                .queryName(s"query$i")
                .option("checkpointLocation", metadataRoot)
                .outputMode("append")
                .start()
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
      testBehaviorFor: Span = 4.seconds
    ): Unit = {

    def awaitTermFunc(): Unit = {
      if (awaitTimeout != null && awaitTimeout.toMillis > 0) {
        val returnedValue = spark.streams.awaitAnyTermination(awaitTimeout.toMillis)
        assert(returnedValue === expectedReturnedValue, "Returned value does not match expected")
      } else {
        spark.streams.awaitAnyTermination()
      }
    }

    AwaitTerminationTester.test(expectedBehavior, () => awaitTermFunc(), testBehaviorFor)
  }

  /** Stop a random active query either with `stop()` or with an error */
  private def stopRandomQueryAsync(stopAfter: Span, withError: Boolean): StreamingQuery = {
    // scalastyle:off executioncontextglobal
    import scala.concurrent.ExecutionContext.Implicits.global
    // scalastyle:on executioncontextglobal
    val activeQueries = spark.streams.active
    val queryToStop = activeQueries(Random.nextInt(activeQueries.length))
    Future {
      Thread.sleep(stopAfter.toMillis)
      if (withError) {
        logDebug(s"Terminating query ${queryToStop.name} with error")
        queryToStop.asInstanceOf[StreamingQueryWrapper].streamingQuery.logicalPlan.collect {
          case r: StreamingDataSourceV2Relation =>
            r.stream.asInstanceOf[MemoryStream[Int]].addData(0)
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
