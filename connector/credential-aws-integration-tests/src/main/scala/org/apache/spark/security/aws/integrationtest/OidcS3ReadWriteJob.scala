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
 * to verify OIDC credential propagation.
 *
 * The job:
 *  1. Creates a SparkContext (credentials are propagated to executors by the framework).
 *  2. Creates a small RDD and writes it as text to the S3 path supplied as the first argument.
 *  3. Reads back the written data and verifies it is non-empty.
 *  4. Logs a success marker so the test suite can confirm completion via driver pod logs.
 *
 * Usage:
 *   spark-submit --class org.apache.spark.security.aws.integrationtest.OidcS3ReadWriteJob \
 *     <jar> <s3a-output-path>
 *
 * The S3A credentials provider is expected to be set (by the launching suite) to
 * [[org.apache.spark.security.aws.SparkOidcAwsCredentialsProvider]] via
 * `spark.hadoop.fs.s3a.aws.credentials.provider`, together with
 * `spark.security.oidc.enabled=true` on the driver.
 */
object OidcS3ReadWriteJob extends Logging {

  val SUCCESS_MARKER = "OidcS3ReadWriteJob: SUCCESS"

  def main(args: Array[String]): Unit = {
    require(args.length >= 1,
      s"Usage: ${getClass.getName.stripSuffix("$")} <s3a-output-path>")

    val outputPath = args(0)

    val conf = new SparkConf().setAppName("OidcS3ReadWriteJob")
    val sc = new SparkContext(conf)

    try {
      // Write phase: distribute work to executors so that each executor must use
      // the propagated OIDC-derived S3 credentials.
      val data = sc.parallelize(Seq("oidc-test-line-1", "oidc-test-line-2"), numSlices = 2)
      data.saveAsTextFile(outputPath)
      logInfo(s"Wrote data to $outputPath")

      // Read-back phase: verify the data is readable with the same credentials.
      val readBack = sc.textFile(outputPath)
      val count = readBack.count()
      require(count > 0, s"Expected non-empty output at $outputPath, got $count records")
      logInfo(s"Read back $count records from $outputPath")

      // Emit the success marker that OidcCredentialE2ESuite looks for in the driver log.
      // scalastyle:off println
      println(SUCCESS_MARKER)
      // scalastyle:on println
      logInfo(SUCCESS_MARKER)
    } finally {
      sc.stop()
    }
  }
}
