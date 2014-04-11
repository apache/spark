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

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ui.{SparkUI, UITab}
import org.apache.spark.Logging
import java.util.concurrent.atomic.AtomicInteger

/** Streaming tab in the Spark web UI */
private[spark] class StreamingTab(ssc: StreamingContext)
  extends UITab(StreamingTab.streamingTabName) with Logging {

  val parent = ssc.sc.ui
  val streamingListener = new StreamingJobProgressListener(ssc)
  val basePath = parent.basePath
  val appName = parent.appName

  ssc.addStreamingListener(streamingListener)
  attachPage(new StreamingPage(this))
  parent.attachTab(this)

  def headerTabs = parent.getTabs

  def start() { }
}

object StreamingTab {
  private val atomicInteger = new AtomicInteger(0)

  /** Generate the name of the streaming tab. For the first streaming tab it will be */
  def streamingTabName: String = {
    val count = atomicInteger.getAndIncrement
    if (count == 0) "streaming" else s"streaming-$count"
  }
}
