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

package org.apache.spark.streamingtest

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, Suite}

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.testutil.StreamingTestUtils

/**
 * Manages a local `ssc` `StreamingContext` variable, correctly stopping it after each test.
 * Note that it also stops active SparkContext if `stopSparkContext` is set to true (default).
 * In most cases you may want to leave it, to isolate environment for SparkContext in each test.
 */
trait LocalStreamingContext extends BeforeAndAfterEach with BeforeAndAfterAll { self: Suite =>

  @transient var ssc: StreamingContext = _
  @transient var stopSparkContext: Boolean = true

  override def beforeAll() {
    super.beforeAll()
  }

  override def afterEach() {
    try {
      resetStreamingContext()
    } finally {
      super.afterEach()
    }
  }

  def resetStreamingContext(): Unit = {
    LocalStreamingContext.stop(ssc, stopSparkContext)
    ssc = null
  }
}

object LocalStreamingContext {
  def stop(ssc: StreamingContext, stopSparkContext: Boolean): Unit = {
    if (stopSparkContext) {
      StreamingTestUtils.ensureNoActiveSparkContext(ssc)
    } else if (ssc != null) {
      ssc.stop(stopSparkContext = false)
    }
  }
}
