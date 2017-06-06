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

import org.apache.spark.{SparkConf, SparkContext}

private[spark] object LocalDiskWritingApp extends TestApplication {

  def main(args: Array[String]): Unit = {
    checkArgs(args)("driver_result_url", "executor_result_url")
    val Array(driverResultUrl, executorResultUrl) = args

    if (driverResultUrl != "NO DRIVER WRITE") {
      writeReadAndPutFileContents("DRIVE", driverResultUrl)
    }

    val sc = new SparkContext(new SparkConf())
    try {
      sc.parallelize(Seq(1)).foreach { x => writeReadAndPutFileContents("EXEC", executorResultUrl) }
    } finally {
      sc.stop()
    }
  }

}
