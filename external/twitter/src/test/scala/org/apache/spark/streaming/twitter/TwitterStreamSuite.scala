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

package org.apache.spark.streaming.twitter

import org.apache.spark.streaming.{StreamingContext, TestSuiteBase}
import org.apache.spark.storage.StorageLevel
import twitter4j.auth.{NullAuthorization, Authorization}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import twitter4j.Status

class TwitterStreamSuite extends TestSuiteBase {

  test("twitter input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val filters = Seq("filter1", "filter2")
    // bounding box around Tokyo, Japan
    val latLons = Seq(Array(139.325823, 35.513041), Array(139.908099, 35.952258))
    val authorization: Authorization = NullAuthorization.getInstance()

    // tests the API, does not actually test data receiving
    val test1: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)
    // testing filters without Twitter OAuth
    val test2: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters)
    val test3: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2)
    // testing longitude, latitude bounding without Twitter OAuth
    val test4: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, latLons=latLons)
    val test5: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, latLons=latLons, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2)
    // testing both filters and lonLat bounding without Twitter OAuth
    val test6: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters, latLons)
    val test7: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters, latLons, StorageLevel.MEMORY_AND_DISK_SER_2)
    // testing with Twitter OAuth
    val test8: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization))
    // testing filters with Twitter OAuth
    val test9: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization), filters)
    val test10: ReceiverInputDStream[Status] = TwitterUtils.createStream(
      ssc, Some(authorization), filters, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2)
    // testing longitude, latitude bounding with Twitter OAuth
    val test11: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization), latLons=latLons)
    val test12: ReceiverInputDStream[Status] = TwitterUtils.createStream(
      ssc, Some(authorization), latLons=latLons, storageLevel=StorageLevel.MEMORY_AND_DISK_SER_2)
    // testing both filters and lonLat bounding with Twitter OAuth
    val test13: ReceiverInputDStream[Status] = TwitterUtils.createStream(
      ssc, Some(authorization), filters, latLons, StorageLevel.MEMORY_AND_DISK_SER_2)

    // Note that actually testing the data receiving is hard as authentication keys are
    // necessary for accessing Twitter live stream
    ssc.stop()
  }
}
