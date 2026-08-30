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

package org.apache.spark.shuffle.streaming

import java.util.Properties

import org.scalatest.matchers.should.Matchers

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.shuffle.streaming.MultiShuffleManager.{isStreamingShuffleEnabled, STREAMING_SHUFFLE_ENABLED_PROPERTY}

class MultiShuffleManagerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Matchers {

  test("isStreamingShuffleEnabled reflects the per-query property") {
    val props = new Properties()
    isStreamingShuffleEnabled(props) should be(false)

    props.setProperty(STREAMING_SHUFFLE_ENABLED_PROPERTY, "true")
    isStreamingShuffleEnabled(props) should be(true)

    props.setProperty(STREAMING_SHUFFLE_ENABLED_PROPERTY, "false")
    isStreamingShuffleEnabled(props) should be(false)
  }

  private def assertRoutesToStreaming(enabled: Boolean): Unit = {
    withSpark(new SparkContext("local", "MultiShuffleManagerSuite", new SparkConf())) { sc =>
      if (enabled) {
        sc.setLocalProperty(STREAMING_SHUFFLE_ENABLED_PROPERTY, "true")
      }
      val rdd = sc.parallelize(1 to 4).map(x => (x, x))
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
      val handle = new MultiShuffleManager(sc.conf).registerShuffle(7, dep)
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]] == enabled)
    }
  }

  test("registerShuffle routes to the streaming manager when enabled for the query") {
    assertRoutesToStreaming(enabled = true)
  }

  test("registerShuffle routes to the sort manager when not enabled for the query") {
    assertRoutesToStreaming(enabled = false)
  }

  test("SparkEnv initializes the streaming shuffle tracker when MultiShuffleManager is set") {
    val conf = new SparkConf().set(SHUFFLE_MANAGER, classOf[MultiShuffleManager].getName)
    withSpark(new SparkContext("local", "MultiShuffleManagerSuite", conf)) { _ =>
      assert(SparkEnv.get.streamingShuffleOutputTracker.isDefined)
    }
  }
}
