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

package org.apache.spark.streaming.ui

import org.apache.spark.SparkException
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ui.{SparkUI, JettyUtils}
import org.eclipse.jetty.servlet.ServletContextHandler

class StreamingUI(val ssc: StreamingContext) {

  private val STATIC_RESOURCE_DIR = "org/apache/spark/streaming/ui/static"

  val listener = ssc.progressListener
  ssc.addStreamingListener(listener)
  ssc.sc.addSparkListener(listener)

  var staticHandler: ServletContextHandler = null
  var streamingTab: StreamingTab = null
  var streamingStorageTab: StreamingStorageTab = null

  def start() {
    val sparkUI = StreamingUI.getSparkUI(ssc)
    streamingTab = new StreamingTab(sparkUI, ssc)
    sparkUI.attachTab(streamingTab)
    streamingStorageTab = new StreamingStorageTab(sparkUI, listener)
    sparkUI.attachTab(streamingStorageTab)
    staticHandler = JettyUtils.createStaticHandler(STATIC_RESOURCE_DIR, "/static/streaming")
    StreamingUI.getSparkUI(ssc).attachHandler(staticHandler)
  }

  def stop() {
    val sparkUI = StreamingUI.getSparkUI(ssc)
    if (streamingTab != null) {
      sparkUI.detachTab(streamingTab)
      streamingTab = null
    }
    if (streamingStorageTab != null) {
      sparkUI.detachTab(streamingStorageTab)
      streamingStorageTab = null
    }
    if (staticHandler != null) {
      StreamingUI.getSparkUI(ssc).detachHandler(staticHandler)
      staticHandler = null
    }
  }
}

object StreamingUI {
  def getSparkUI(ssc: StreamingContext): SparkUI = {
    ssc.sc.ui.getOrElse {
      throw new SparkException("Parent SparkUI to attach this tab to not found!")
    }
  }
}
