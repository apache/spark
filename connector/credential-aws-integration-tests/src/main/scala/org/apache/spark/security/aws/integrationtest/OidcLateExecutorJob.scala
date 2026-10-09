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
package org.apache.spark.security.aws.integrationtest

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging

/**
 * Spark job used by the OIDC credential propagation E2E suite (OidcCredentialE2ESuite)
 * to verify that a late-registering executor receives credentials.
 *
 * With dynamic allocation enabled, the job:
 *  1. Runs a small first stage so the driver acquires credentials while few (or one)
 *     executors are active.
 *  2. Idles long enough for dynamic allocation to release idle executors.
 *  3. Runs a second, wider stage that forces new executors to be requested and
 *     registered *after* credentials were already acquired. Each task in that stage
 *     writes to S3, so a late-registering executor that did not receive credentials
 *     would fail the job.
 *
 * If every task in the second stage succeeds, late-registering executors received
 * credentials (via the SparkAppConfig response at registration time) without waiting
 * for the next renewal broadcast.
 *
 * Usage:
 *   spark-submit --class ...OidcLateExecutorJob <jar> <s3a-output-prefix> <idleMillis> <partitions>
 */
object OidcLateExecutorJob extends Logging {

  val SUCCESS_MARKER = "OidcLateExecutorJob: SUCCESS"

  def main(args: Array[String]): Unit = {
    require(args.length >= 3,
      s"Usage: ${getClass.getName.stripSuffix("$")} " +
        "<s3a-output-prefix> <idleMillis> <partitions>")

    val outputPrefix = args(0).stripSuffix("/")
    val idleMillis = args(1).toLong
    val partitions = args(2).toInt

    val conf = new SparkConf().setAppName("OidcLateExecutorJob")
    val sc = new SparkContext(conf)

    try {
      // Stage 1: small warm-up so the driver acquires credentials early.
      val warm = sc.parallelize(Seq("warm-1", "warm-2"), numSlices = 1)
      warm.saveAsTextFile(s"$outputPrefix/warmup/")
      logInfo(s"Warm-up stage complete; wrote $outputPrefix/warmup/")

      // Idle so dynamic allocation scales executors down (idle timeout is set low
      // by the test via spark.dynamicAllocation.executorIdleTimeout).
      logInfo(s"Idling for ${idleMillis}ms to let dynamic allocation scale down...")
      Thread.sleep(idleMillis)

      // Stage 2: wide stage that forces new executors to be requested/registered.
      // Each partition writes its own object to S3, so any executor lacking
      // credentials would fail its task (and thus the job).
      val wide = sc.parallelize(0 until partitions, numSlices = partitions)
      wide.map { i =>
        s"late-executor-partition-$i"
      }.saveAsTextFile(s"$outputPrefix/wide/")
      logInfo(s"Wide stage complete; wrote $outputPrefix/wide/ with $partitions partitions")

      // scalastyle:off println
      println(SUCCESS_MARKER)
      // scalastyle:on println
      logInfo(SUCCESS_MARKER)
    } finally {
      sc.stop()
    }
  }
}
