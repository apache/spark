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

import io.netty.buffer.Unpooled
import org.mockito.Mockito.when
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import org.apache.spark._
import org.apache.spark.LocalSparkContext.withSpark
import org.apache.spark.internal.config.SHUFFLE_MANAGER
import org.apache.spark.network.shuffle.streaming.{DataMessage, TerminationAckMessage, TerminationControlMessage}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.shuffle.streaming.StreamingShuffleManager.{getQueryId, getWriterId, QUERY_ID_PROPERTY_KEY}

class StreamingShuffleManagerSuite
  extends SparkFunSuite
  with LocalSparkContext
  with Matchers
  with MockitoSugar {

  private val SQL_EXECUTION_ID_KEY = "spark.sql.execution.id"

  // ---- getWriterId ----

  test("getWriterId returns the writer id for a data message") {
    val msg = new DataMessage(7, 3, 0, Unpooled.EMPTY_BUFFER, 0L)
    getWriterId(msg) should be(7)
  }

  test("getWriterId returns the writer id for a termination control message") {
    getWriterId(new TerminationControlMessage(5, 2)) should be(5)
  }

  test("getWriterId throws on an unexpected message type") {
    val e = intercept[SparkRuntimeException] {
      getWriterId(new TerminationAckMessage(1, 1))
    }
    checkError(
      e,
      condition = "STREAMING_SHUFFLE_UNEXPECTED_MESSAGE_TYPE",
      parameters = Map("messageType" -> "TERMINATION_ACK_MESSAGE"))
  }

  // ---- getQueryId ----

  test("getQueryId returns the streaming query id when set") {
    val context = mock[TaskContext]
    when(context.getLocalProperty(QUERY_ID_PROPERTY_KEY)).thenReturn("query-123")
    getQueryId(context) should be("query-123")
  }

  test("getQueryId falls back to the SQL execution id for batch queries") {
    val context = mock[TaskContext]
    when(context.getLocalProperty(SQL_EXECUTION_ID_KEY)).thenReturn("42")
    getQueryId(context) should be("42")
  }

  test("getQueryId throws when no query id property is set") {
    val context = mock[TaskContext]
    intercept[SparkException] {
      getQueryId(context)
    }
  }

  // ---- registerShuffle ----

  test("registerShuffle returns a StreamingShuffleHandle") {
    withSpark(new SparkContext("local", "StreamingShuffleManagerSuite", new SparkConf())) { sc =>
      val rdd = sc.parallelize(1 to 4).map(x => (x, x))
      val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2))
      val handle = new StreamingShuffleManager().registerShuffle(0, dep)
      assert(handle.isInstanceOf[StreamingShuffleHandle[_, _, _]])
    }
  }

  // ---- SparkEnv tracker initialization gating ----

  test("SparkEnv initializes the streaming shuffle tracker when StreamingShuffleManager is set") {
    val conf = new SparkConf().set(SHUFFLE_MANAGER, classOf[StreamingShuffleManager].getName)
    withSpark(new SparkContext("local", "StreamingShuffleManagerSuite", conf)) { _ =>
      val tracker = SparkEnv.get.streamingShuffleOutputTracker
      assert(tracker.isDefined)
      // The driver hosts the master tracker.
      assert(tracker.get.isInstanceOf[StreamingShuffleOutputTrackerMaster])
    }
  }

  test("SparkEnv does not initialize the streaming shuffle tracker for a non-streaming manager") {
    // A non-streaming shuffle manager (the default sort manager, configured explicitly) must
    // not trigger StreamingShuffleOutputTracker initialization.
    val conf = new SparkConf().set(SHUFFLE_MANAGER, classOf[SortShuffleManager].getName)
    withSpark(new SparkContext("local", "StreamingShuffleManagerSuite", conf)) { _ =>
      assert(SparkEnv.get.streamingShuffleOutputTracker.isEmpty)
    }
  }

  test("SparkEnv does not initialize the streaming shuffle tracker for the default manager") {
    withSpark(new SparkContext("local", "StreamingShuffleManagerSuite", new SparkConf())) { _ =>
      assert(SparkEnv.get.streamingShuffleOutputTracker.isEmpty)
    }
  }
}
