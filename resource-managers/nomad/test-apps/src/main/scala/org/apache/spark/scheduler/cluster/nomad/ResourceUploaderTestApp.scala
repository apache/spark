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

import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.Resources

import org.apache.spark.SparkContext

private[spark] object ResourceUploaderTestApp extends TestApplication {

  def main(args: Array[String]): Unit = {
    checkArgs(args)("driver_result_url", "executor_result_url")
    val Array(driverResultUrl, executorResultUrl) = args

    val sc = new SparkContext
    try {
      readTestResourceAndUploadTo(driverResultUrl)
      sc.parallelize(Seq(1)).foreach { _ => readTestResourceAndUploadTo(executorResultUrl) }
    } finally {
      sc.stop()
    }
  }

  private def readTestResourceAndUploadTo(outputUrl: String): Unit = {
    httpPut(outputUrl) {
      val resource = Thread.currentThread.getContextClassLoader.getResource("test.resource")
      Resources.toString(resource, UTF_8)
    }
  }

}
