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
package org.apache.spark.deploy.kubernetes.integrationtest.jobs

import org.apache.spark.deploy.kubernetes.integrationtest.PiHelper
import org.apache.spark.sql.SparkSession

// Equivalent to SparkPi except does not stop the Spark Context
// at the end and spins forever, so other things can inspect the
// Spark UI immediately after the fact.
private[spark] object SparkPiWithInfiniteWait {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Spark Pi")
      .getOrCreate()
    val slices = if (args.length > 0) args(0).toInt else 10
    val n = math.min(100000L * slices, Int.MaxValue).toInt // avoid overflow
    val count = spark.sparkContext.parallelize(1 until n, slices).map { _ =>
        PiHelper.helpPi()
      }.reduce(_ + _)
    // scalastyle:off println
    println("Pi is roughly " + 4.0 * count / (n - 1))
    // scalastyle:on println

    // Spin forever to keep the Spark UI active, so other things can inspect the job.
    while (true) {
      Thread.sleep(600000)
    }
  }

}
