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
 * Long-running Spark job used by the OIDC credential propagation E2E suite
 * (OidcCredentialE2ESuite) to verify mid-job token rotation.
 *
 * The job writes to S3 repeatedly over a period of time. While it runs, the test
 * rotates the identity token file (simulating an externally-rotated token, e.g. a
 * Kubernetes projected ServiceAccount token). The driver's UserCredentialManager
 * re-reads the rotated token on its next renewal, exchanges it for fresh credentials
 * via STS, and propagates them to executors. Because each iteration writes with the
 * credentials current at that time, continued success across the rotation boundary
 * demonstrates that the refresh did not interrupt S3 access.
 *
 * Usage:
 *   spark-submit --class ...OidcTokenRotationJob <jar> \
 *     <s3a-output-prefix> <iterations> <sleepMillis>
 *
 * Each iteration i writes a distinct object at {@code <prefix>/iter-<i>/}. The test
 * verifies that objects for iterations spanning the rotation are all present.
 */
object OidcTokenRotationJob extends Logging {

  val SUCCESS_MARKER = "OidcTokenRotationJob: SUCCESS"

  def main(args: Array[String]): Unit = {
    require(args.length >= 3,
      s"Usage: ${getClass.getName.stripSuffix("$")} " +
        "<s3a-output-prefix> <iterations> <sleepMillis>")

    val outputPrefix = args(0).stripSuffix("/")
    val iterations = args(1).toInt
    val sleepMillis = args(2).toLong

    val conf = new SparkConf().setAppName("OidcTokenRotationJob")
    val sc = new SparkContext(conf)

    try {
      for (i <- 0 until iterations) {
        val path = s"$outputPrefix/iter-$i/"
        val data = sc.parallelize(Seq(s"oidc-rotation-line-$i-a", s"oidc-rotation-line-$i-b"),
          numSlices = 2)
        data.saveAsTextFile(path)
        // Read back to force an S3 read with the current credentials as well.
        val count = sc.textFile(path).count()
        require(count > 0, s"Expected non-empty output at $path, got $count records")
        logInfo(s"Iteration $i: wrote and read back $count records at $path")
        if (i < iterations - 1) {
          Thread.sleep(sleepMillis)
        }
      }

      // scalastyle:off println
      println(SUCCESS_MARKER)
      // scalastyle:on println
      logInfo(SUCCESS_MARKER)
    } finally {
      sc.stop()
    }
  }
}
