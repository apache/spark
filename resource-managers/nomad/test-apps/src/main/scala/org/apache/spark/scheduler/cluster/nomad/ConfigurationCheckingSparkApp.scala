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

import java.net.URL

import org.apache.spark.{SparkConf, SparkContext}

private[spark] object ConfigurationCheckingSparkApp extends TestApplication {

  val WAIT_TIMEOUT_MILLIS = 10000

  def main(args: Array[String]): Unit = {
    checkArgs(args)("result_url")
    val Array(resultUrl) = args

    val sc = new SparkContext(
      new SparkConf()
        .set("spark.extraListeners", classOf[SaveExecutorInfo].getName)
        .setAppName("Nomad \"test app\" 'with quotes' and \\back\\slashes and $dollarSigns")
    )
    try {
      httpPut(resultUrl) {
        val conf = sc.getConf

        val data = sc.parallelize(1 to 4, 4).collect().toSet
        sc.listenerBus.waitUntilEmpty(WAIT_TIMEOUT_MILLIS)
        assertEquals(data, Set(1, 2, 3, 4))

        // verify log URLs are present
        val Seq(listener) = sc.listenerBus.findListenersByClass[SaveExecutorInfo]()
        val executorInfos = listener.addedExecutorInfos.values
        assert(executorInfos.nonEmpty)
        executorInfos.foreach { info =>
          validateLogs("executor", conf, info.logUrlMap)
        }

        val driverLogsStatus = listener.driverLogs match {
          case Some(logs) =>
            validateLogs("driver", conf, logs)
            "with driver logs"
          case None =>
            "without driver logs"
        }

        s"SUCCESS $driverLogsStatus with ${executorInfos.size} executor(s)"
      }
    } finally sc.stop()
  }

  def validateLogs(task: String, conf: SparkConf, logs: collection.Map[String, String]): Unit = {
    val expectedLogs = Set("stdout", "stderr")
    assert(logs.keySet == expectedLogs, s"Found logs ${logs.keys}")

    val allocId = conf.getenv("NOMAD_ALLOC_ID")
    logs.foreach { case (name, urlString) =>
      val url = new URL(urlString)
      assertEquals(url.getProtocol, "http")
      assert(url.getPath.matches("/v1/client/fs/logs/[0-9a-f-]{36}"))
      assertEquals(url.getQuery, s"follow=true&plain=true&task=$task&type=$name")
    }
  }
}
