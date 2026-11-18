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

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.config.EXECUTOR_HEARTBEAT_INTERVAL
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class ScanLimitSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  // A running task reports the bytes it has read with the heartbeats of its executor.
  override protected def sparkConf: SparkConf =
    super.sparkConf.set(EXECUTOR_HEARTBEAT_INTERVAL.key, "100ms")

  // Keeps a task running for much longer than it takes to cancel its query.
  private val block = udf((id: Long) => { Thread.sleep(30000); id })

  /** Runs `f` with the path of a Parquet file that has the IDs 0 to 3. */
  private def withData(f: String => Unit): Unit = {
    withTempPath { path =>
      spark.range(0, 4, 1, 1).write.parquet(path.getCanonicalPath)
      f(path.getCanonicalPath)
    }
  }

  /** Checks that `query` is cancelled for reading more than a limit of 1 byte. */
  private def checkScanLimitExceeded(query: => Unit): Unit = {
    withSQLConf(SQLConf.MAX_SCAN_BYTES.key -> "1") {
      checkErrorMatchPVals(
        exception = intercept[SparkException](query),
        condition = "SCAN_BYTES_LIMIT_EXCEEDED",
        parameters = Map(
          "scannedBytes" -> ".+ B",
          "limit" -> "1\\.0 B",
          "config" -> "\"spark.sql.execution.maxScanBytes\""))
    }
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

  test("streaming queries are not limited") {
    withData { path =>
      withTempView("stream") {
        // Slow enough for several heartbeats to report what the task has read.
        val slow = udf((id: Long) => { Thread.sleep(200); id })
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
}
