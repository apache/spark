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

package org.apache.spark.scheduler.cluster.nomad

import java.io.File
import java.nio.charset.StandardCharsets

import com.google.common.io.Files

import org.apache.spark.{SparkConf, SparkContext}

private[spark] object FileDistributionTestDriver extends TestApplication {

  def main(args: Array[String]): Unit = {
    checkArgs(args)("driver_file", "driver_result_url", "executor_file", "executor_result_url")
    val Array(driverFile, driverResultUrl, executorFile, executorResultUrl) = args

    putTestFileContents(driverFile, driverResultUrl)
    val sc = new SparkContext(new SparkConf())
    try {
      sc.parallelize(Seq(1)).foreach { x => putTestFileContents(executorFile, executorResultUrl) }
    } finally {
      sc.stop()
    }
  }

  private def putTestFileContents(path: String, outputUrl: String): Unit =
    httpPut(outputUrl) {
      Files.toString(new File(path), StandardCharsets.UTF_8)
    }

}
