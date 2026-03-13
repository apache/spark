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

package org.apache.spark.sql.connect

import java.util.concurrent.TimeUnit

import scala.io.StdIn
import scala.sys.exit

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.ShutdownHookManager

/**
 * A simple main class method to start the spark connect server as a service for client tests
 * using spark-submit:
 * {{{
 *     bin/spark-submit --class org.apache.spark.sql.connect.SimpleSparkConnectService
 * }}}
 * The service can be stopped by receiving a stop command or until the service get killed.
 */
private[sql] object SimpleSparkConnectService {
  private val stopCommand = "q"
  private val ArrowLeakExitCode = 77

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .set("spark.plugins", "org.apache.spark.sql.connect.SparkConnectPlugin")
      .set(SQLConf.ARTIFACTS_SESSION_ISOLATION_ENABLED, true)
      .set(SQLConf.ARTIFACTS_SESSION_ISOLATION_ALWAYS_APPLY_CLASSLOADER, true)
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sparkContext = sparkSession.sparkContext // init spark context

    // Arrow leak check registered at priority 10 (below SPARK_CONTEXT_SHUTDOWN_PRIORITY = 50)
    // so it runs after SparkContext has fully stopped. Polls up to 2 minutes rather than checking
    // immediately: execution threads stop accepting new work once the gRPC server shuts down but
    // may still be flushing their last Arrow batch, and we should not force-close them.
    // halt() is used instead of exit() because exit() deadlocks inside a shutdown hook.
    ShutdownHookManager.addShutdownHook(10) { () =>
      // Synthetic leak for ArrowLeakDetectionE2ETest only. Must be injected here, not in the
      // stop handler, so SparkContext shutdown cannot release it before the check below runs.
      // Hold a strong reference through the polling loop: Arrow's AllocationManager registers a
      // Cleaner on ArrowBuf, so a discarded (unreferenced) buffer can be reclaimed by GC during
      // Thread.sleep(), decrementing getAllocatedMemory to 0 and defeating the synthetic leak.
      val syntheticLeakBuf =
        if (sys.env.contains("SPARK_TEST_ARROW_LEAK")) {
          val leakyAllocator = ArrowUtils.rootAllocator.newChildAllocator("test-leak", 0, 1024)
          leakyAllocator.buffer(64) // intentionally never closed — validates leak detection
        } else null
      try {
        val deadline = System.currentTimeMillis() + 2 * 60 * 1000L
        while (ArrowUtils.rootAllocator.getAllocatedMemory != 0 &&
          System.currentTimeMillis() < deadline) {
          Thread.sleep(100)
        }
        val leaked = ArrowUtils.rootAllocator.getAllocatedMemory
        if (leaked != 0) {
          // scalastyle:off println
          println(s"Arrow rootAllocator memory leak detected: $leaked bytes still allocated")
          // scalastyle:on println
          Runtime.getRuntime.halt(ArrowLeakExitCode)
        }
      } finally {
        // Keep syntheticLeakBuf reachable through the check above.
        java.lang.ref.Reference.reachabilityFence(syntheticLeakBuf)
      }
    }

    // scalastyle:off println
    println("Ready for client connections.")
    // scalastyle:on println
    while (true) {
      val code = StdIn.readLine()
      if (code == stopCommand) {
        // scalastyle:off println
        println("No more client connections.")
        // scalastyle:on println
        // Wait for 1 min for the server to stop
        SparkConnectService.stop(Some(1), Some(TimeUnit.MINUTES))
        sparkSession.close()
        exit(0) // triggers shutdown hooks; Arrow leak check runs in the hook registered above
      }
    }
  }
}
