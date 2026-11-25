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

package org.apache.spark.sql.execution

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.config.EXECUTOR_HEARTBEAT_INTERVAL
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.streaming.runtime.StreamExecution
import org.apache.spark.sql.functions.{broadcast, max, udf}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.ThreadUtils

class ScanLimitSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // A running task reports the bytes it has read with the heartbeats of its executor.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(EXECUTOR_HEARTBEAT_INTERVAL.key, "100ms")

  // Keeps a task running for much longer than it takes to cancel its query.
  private val block = udf((id: Long) => { Thread.sleep(30000); id })

  // Slow enough for several heartbeats to report what the task has read.
  private val slow = udf((id: Long) => { Thread.sleep(200); id })

  /** Runs `f` with the path of a Parquet file that has the IDs 0 to 3. */
  private def withData(f: String => Unit): Unit = {
    withTempPath { path =>
      spark.range(0, 4, 1, 1).write.parquet(path.getCanonicalPath)
      f(path.getCanonicalPath)
    }
  }

  /** Checks that `query` is cancelled for reading more than a limit of `limit` bytes. */
  private def checkScanLimitExceeded(query: => Unit, limit: Long = 1): Unit = {
    withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> limit.toString) {
      val e = intercept[SparkException](query)
      checkErrorMatchPVals(
        exception = e,
        condition = "SCAN_BYTES_LIMIT_EXCEEDED",
        parameters = Map(
          "scannedBytes" -> "\\d+ bytes \\(.+B\\)",
          // A limit below 2 KiB is shown in bytes.
          "limit" -> s"$limit bytes \\($limit\\.0 B\\)",
          "config" -> "\"spark.sql.execution.maxScanBytes\""))
      // The error shows what the query had read when it went over the limit.
      val scannedBytes = e.getMessageParameters.get("scannedBytes").takeWhile(_.isDigit).toLong
      assert(scannedBytes > limit)
    }
    checkNothingTracked()
  }

  /** Checks that the listener keeps nothing of the queries with a limit that have ended. */
  private def checkNothingTracked(): Unit = {
    val listener = spark.sharedState.scanLimitListener
    assert(listener.numExecutions == 0)
    assert(listener.numStages == 0)
  }

  test("a query that reads more than the limit is cancelled") {
    withData { path =>
      checkScanLimitExceeded {
        spark.read.parquet(path).select(block($"id")).collect()
      }
    }
  }

  test("a query that reads less than the limit runs") {
    withData { path =>
      withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> "1g") {
        checkAnswer(spark.read.parquet(path), spark.range(4).toDF())
      }
      checkNothingTracked()
    }
  }

  test("a limit of 0 bytes cancels a query that reads any data") {
    withData { path =>
      checkScanLimitExceeded(spark.read.parquet(path).select(block($"id")).collect(), limit = 0)
    }
  }

  test("the cached blocks that a query reads count towards its limit") {
    // A range reads no data source, so the query only reads the cached blocks.
    val cached = spark.range(0, 4, 1, 1).toDF().cache()
    try {
      cached.count()
      checkScanLimitExceeded {
        cached.select(block($"id")).collect()
      }
    } finally {
      cached.unpersist(blocking = true)
    }
  }

  test("cancelling a query does not cancel the other queries of its session") {
    withData { path =>
      withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> "1") {
        val executor = ThreadUtils.newDaemonSingleThreadExecutor("scan-limit-other-query")
        try {
          // The other query reads no data source, so it stays within the limit, and it runs for
          // about 2 seconds.
          val other = Future {
            spark.range(0, 10, 1, 1).select(slow($"id")).as[Long].collect()
          }(ExecutionContext.fromExecutorService(executor))
          eventually(timeout(1.minute)) {
            assert(spark.sparkContext.statusTracker.getActiveJobIds().nonEmpty)
          }
          val e = intercept[SparkException] {
            spark.read.parquet(path).select(block($"id")).collect()
          }
          assert(e.getCondition == "SCAN_BYTES_LIMIT_EXCEEDED")
          assert(ThreadUtils.awaitResult(other, 1.minute).toSeq == (0L until 10L))
        } finally {
          executor.shutdown()
        }
      }
      checkNothingTracked()
    }
  }

  // After a query read more than the limit, it may still fail for another reason, or be
  // cancelled by the user before the listener cancels it.
  Seq(
    "fails for another reason" -> new IllegalStateException("unrelated"),
    "is cancelled for another reason" ->
      SparkCoreErrors.sparkJobCancelled(0, "part of cancelled job tags user", null)
  ).foreach { case (failure, error) =>
    test(s"a query that $failure after it read more than the limit keeps its error") {
      withData { path =>
        withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> "1") {
          val df = spark.read.parquet(path)
          val e = intercept[Exception] {
            SQLExecution.withNewExecutionId(df.queryExecution) {
              // A heartbeat may report what the task read before the task ends, and so cancel
              // the job. Either way the query has read more than the limit once the job is over.
              try {
                df.queryExecution.executedPlan.executeCollect()
              } catch {
                case _: SparkException =>
              }
              spark.sparkContext.listenerBus.waitUntilEmpty()
              val executionId =
                spark.sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY).toLong
              assert(
                spark.sharedState.scanLimitListener.bytesReadOverLimit(executionId).isDefined)
              throw error
            }
          }
          assert(e eq error)
        }
        checkNothingTracked()
      }
    }
  }

  test("the limit of a query covers its nested executions") {
    withData { path =>
      withTempView("blocking") {
        spark.read.parquet(path).select(block($"id")).createOrReplaceTempView("blocking")
        // The command runs the query that fills the cache as a nested execution.
        checkScanLimitExceeded {
          sql("CACHE TABLE blocking")
        }
      }
    }
  }

  // The jobs of a scalar subquery and of a broadcast run on threads of their own, which get the
  // root execution ID and the job tags of the query from its captured local properties. Only
  // those jobs read data here.
  Seq(false, true).foreach { aqe =>
    test(s"the limit of a query covers its scalar subqueries (AQE enabled: $aqe)") {
      withData { path =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          val subquery = spark.read.parquet(path).select(max(block($"id"))).scalar()
          checkScanLimitExceeded {
            spark.range(1).select(subquery).collect()
          }
        }
      }
    }

    test(s"the limit of a query covers its broadcasts (AQE enabled: $aqe)") {
      withData { path =>
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> aqe.toString) {
          val blocking = spark.read.parquet(path).select(block($"id").as("id"))
          checkScanLimitExceeded {
            spark.range(4).join(broadcast(blocking), "id").collect()
          }
        }
      }
    }
  }

  test("streaming queries are not limited") {
    withData { path =>
      withTempView("stream") {
        withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> "1") {
          val query = spark.readStream.schema("id LONG").parquet(path).select(slow($"id"))
            .writeStream.format("memory").queryName("stream").start()
          try {
            query.processAllAvailable()
          } finally {
            query.stop()
          }
        }
        checkAnswer(spark.table("stream"), spark.range(4).toDF())
      }
    }
  }

  test("the queries that a stream runs outside its batches are not limited") {
    withData { path =>
      withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> "1") {
        // A source may run queries on the thread of the stream while it plans a batch, with the
        // session it was created with rather than the session of the stream.
        spark.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, "stream")
        try {
          checkAnswer(spark.read.parquet(path).select(slow($"id")), spark.range(4).toDF())
        } finally {
          spark.sparkContext.setLocalProperty(StreamExecution.QUERY_ID_KEY, null)
        }
      }
    }
  }
}
