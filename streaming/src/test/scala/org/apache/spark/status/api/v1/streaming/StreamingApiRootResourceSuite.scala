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
package org.apache.spark.status.api.v1.streaming

import java.net.URI

import scala.collection.mutable.Queue
import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually._

import org.apache.spark.{SparkConf, SparkFunSuite, TestUtils}
import org.apache.spark.internal.config.UI.{UI_ENABLED, UI_PORT}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

/**
 * Tests that the streaming REST API, which is contributed to `/api` through
 * `StreamingApiRootResourceRegistrar`, is served by a live application UI.
 */
class StreamingApiRootResourceSuite extends SparkFunSuite {

  test("streaming REST API is served under /api/v1/applications/[app-id]/streaming") {
    val conf = new SparkConf()
      .setMaster("local[2]")
      .setAppName(getClass.getSimpleName)
      .set(UI_ENABLED, true)
      .set(UI_PORT, 0)
    val ssc = new StreamingContext(conf, Milliseconds(100))
    try {
      ssc.queueStream(Queue(ssc.sparkContext.parallelize(1 to 4, 2))).foreachRDD(_.count())
      ssc.start()
      val sc = ssc.sparkContext
      val baseUrl = s"${sc.uiWebUrl.get}/api/v1/applications/${sc.applicationId}/streaming"
      def url(path: String) = new URI(s"$baseUrl/$path").toURL

      eventually(timeout(10.seconds), interval(100.milliseconds)) {
        assert(TestUtils.httpResponseCode(url("statistics")) === 200)
      }
      assert(TestUtils.httpResponseMessage(url("statistics")).contains("\"batchDuration\" : 100"))
      assert(TestUtils.httpResponseCode(url("receivers")) === 200)
      assert(TestUtils.httpResponseCode(url("batches")) === 200)
    } finally {
      ssc.stop(stopSparkContext = true)
    }
  }
}
