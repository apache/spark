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
import twitter4j.{FilterQuery, Status}

class TwitterStreamSuite extends TestSuiteBase {

  test("twitter input stream") {
    val ssc = new StreamingContext(master, framework, batchDuration)
    val filters = Seq("filter1", "filter2")
    val query = new FilterQuery().track(filters.toArray)
    val authorization: Authorization = NullAuthorization.getInstance()

    // tests the API, does not actually test data receiving

    // Create stream with empty filter restrictions
    val test1: ReceiverInputDStream[Status] = TwitterUtils.createStream(ssc, None)
    val test2: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization))

    // Create stream restricted by keywords
    val test3: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters)
    val test4: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, None, filters, StorageLevel.MEMORY_AND_DISK_SER_2)
    val test5: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, Some(authorization), filters)
    val test6: ReceiverInputDStream[Status] = TwitterUtils.createStream(
      ssc, Some(authorization), filters, StorageLevel.MEMORY_AND_DISK_SER_2)

    // Create stream from FilterQuery
    val test7: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, query)
    val test8: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc,  query, StorageLevel.MEMORY_AND_DISK_SER_2)
    val test9: ReceiverInputDStream[Status] =
      TwitterUtils.createStream(ssc, authorization, query)
    val test10: ReceiverInputDStream[Status] = TwitterUtils.createStream(
      ssc, authorization, query, StorageLevel.MEMORY_AND_DISK_SER_2)

    // Note that actually testing the data receiving is hard as authentication keys are
    // necessary for accessing Twitter live stream
    ssc.stop()
  }
}
