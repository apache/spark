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

import org.apache.spark.internal.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.ui.{SparkUI, SparkUITab}

/**
 * Spark Web UI tab that shows statistics of a streaming job.
 * This assumes the given SparkContext has enabled its SparkUI.
 */
private[spark] class StreamingTab(val ssc: StreamingContext, sparkUI: SparkUI)
  extends SparkUITab(sparkUI, "streaming") with Logging {

  private val STATIC_RESOURCE_DIR = "org/apache/spark/ui/static"

  val parent = sparkUI
  val listener = ssc.progressListener

  attachPage(new StreamingPage(this))
  attachPage(new BatchPage(this))

  def attach(): Unit = {
    parent.attachTab(this)
    parent.addStaticHandler(STATIC_RESOURCE_DIR, "/static/streaming")
  }

  def detach(): Unit = {
    parent.detachTab(this)
    parent.detachHandler("/static/streaming")
  }
}
