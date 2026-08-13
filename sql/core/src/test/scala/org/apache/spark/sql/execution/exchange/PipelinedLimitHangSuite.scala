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

package org.apache.spark.sql.execution.exchange

import java.util.concurrent.{Executors, TimeUnit}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

/**
 * KNOWN-BUG reproduction (SPARK-57399 local-repartition v2). An early-stopping reader over a
 * pipelined channel shuffle hangs the writer: the channel queue is bounded (64 batches), so
 * once a LIMIT's reduce task is satisfied and stops draining while the map task is still
 * producing, the writer blocks on a full queue's put() with no drainer -- forever. Verified
 * to hang under a 90s deadline in BOTH AQE modes (2026-08-10).
 *
 * regular shuffle does not have this: it materializes to disk first, so the writer finishes
 * and leaves regardless of whether the reader drains everything (LIMIT just skips reading
 * the rest). The pipelined transport's writer/reader concurrency + backpressure assumes the
 * reader stays; early stop breaks that assumption.
 *
 * Both tests are `ignore` on purpose: they HANG until the deadline, so running them in CI
 * would waste 90s each and (sharing a process) the cancelled first would corrupt the second.
 * They document the bug and become the fix's acceptance test -- flip `ignore` to `test`, drop
 * the deadline scaffold, and assert `rows == 10` once early-stop is handled. Run one at a
 * time by hand: build/sbt "sql/testOnly *PipelinedLimitHangSuite -- -z off" after flipping.
 */
class PipelinedLimitHangSuite extends SparkFunSuite with AdaptiveSparkPlanHelper {

  private def withSession(aqe: Boolean)(body: SparkSession => Unit): Unit = {
    val spark = SparkSession.builder()
      .master("local[16]")
      .appName("pipelined-limit-hang")
      .config("spark.shuffle.manager.incremental",
        "org.apache.spark.shuffle.local.pipelined.PipelinedChannelShuffleManager")
      .config("spark.sql.adaptive.enabled", aqe.toString)
      .config("spark.sql.pipelinedShuffle.enabled", "true")
      .config("spark.speculation", "false")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
    try body(spark) finally {
      spark.stop()
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
    }
  }

  private def limitOverPipelinedCompletesWithin(seconds: Int, aqe: Boolean): Boolean = {
    val pool = Executors.newSingleThreadExecutor()
    val fut = pool.submit(new Runnable {
      override def run(): Unit = withSession(aqe) { spark =>
        import spark.implicits._
        // Many input rows per partition so the writer keeps producing well past the LIMIT.
        val rows = spark.range(0, 5000000L, 1, 4).withColumn("k", ($"id" % 100))
          .repartition($"k").limit(10).collect().length
        require(rows == 10, s"expected 10 rows, got $rows")
      }
    })
    try {
      fut.get(seconds.toLong, TimeUnit.SECONDS)
      true
    } catch {
      case _: java.util.concurrent.TimeoutException =>
        fut.cancel(true)
        false
    } finally {
      pool.shutdownNow()
    }
  }

  test("LIMIT over a pipelined shuffle completes (AQE off)") {
    assert(limitOverPipelinedCompletesWithin(90, aqe = false),
      "LIMIT over a pipelined shuffle should complete, but the writer hung on a full queue")
  }

  test("LIMIT over a pipelined shuffle completes (AQE on)") {
    assert(limitOverPipelinedCompletesWithin(90, aqe = true),
      "LIMIT over a pipelined shuffle should complete, but the writer hung on a full queue")
  }
}
