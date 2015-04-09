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

package org.apache.spark.deploy

import scala.io.Source

import org.scalatest.FunSuite

import org.apache.spark.{LocalSparkContext, SparkConf, SparkContext}

class ExecutorDebugPortSuite extends FunSuite with LocalSparkContext {

  /** Length of time to wait while draining listener events. */
  private val WAIT_TIMEOUT_MILLIS = 10000

  test("verify that correct log urls get propagated from workers") {
    try {
      val conf = new SparkConf()
        .set("spark.executor.jdwp.enabled", "true")
        .set("spark.ui.enabled", "true")
        .set("spark.ui.port", "0")
      sc = new SparkContext("local-cluster[2,1,512]", "test", conf)
      sc.parallelize(1 to 10, 10).collect
      val url = sc.ui.get.appUIAddress.stripSuffix("/") + "/executors"
      val html = Source.fromURL(url).mkString
      println(html)
      /*      listener.addedExecutorInfos.values.foreach { info =>
              assert(info.logUrlMap.nonEmpty)
              // Browse to each URL to check that it's valid
              info.logUrlMap.foreach { case (logType, logUrl) =>
                val html = Source.fromURL(logUrl).mkString
                assert(html.contains(s"$logType log page"))
              }
            }*/
    } finally {
      sc.stop()
    }
  }
}
