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

package org.apache.spark.streaming.redis

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaReceiverInputDStream, JavaStreamingContext, JavaDStream}
import scala.reflect.ClassTag
import org.apache.spark.streaming.dstream.{ReceiverInputDStream, DStream}

object RedisUtils {
  /**
   * Create an input stream that receives messages from a Redis Pub/Sub channel.
   * @param ssc           StreamingContext object
   * @param redisUrl      Url of remote Redis server
   * @param channel       Channel to subscribe to
   * @param storageLevel  RDD storage level. Defaults to StorageLevel.MEMORY_AND_DISK_SER_2.
   */
  def createStream(
      ssc: StreamingContext,
      redisUrl: String,
      channels: Seq[String],
      patterns: Seq[String],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): ReceiverInputDStream[String] = {
    new RedisInputDStream(ssc, redisUrl, channels, patterns, storageLevel)
  }

  /**
   * Create an input stream that receives messages from a Redis Pub/Sub channel.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param jssc      JavaStreamingContext object
   * @param redisUrl  Url of remote Redis server
   * @param channel   Channel name to subscribe to
   */
  def createStream(
      jssc: JavaStreamingContext,
      redisUrl: String,
      channels: Seq[String],
      patterns: Seq[String]
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, redisUrl, channels, patterns)
  }

  /**
   * Create an input stream that receives messages from a Redis Pub/Sub channel.
   * @param jssc          JavaStreamingContext object
   * @param redisUrl      Url of remote Redis server
   * @param channel       Channel to subscribe to
   * @param storageLevel  RDD storage level.
   */
  def createStream(
      jssc: JavaStreamingContext,
      redisUrl: String,
      channels: Seq[String],
      patterns: Seq[String],
      storageLevel: StorageLevel
    ): JavaReceiverInputDStream[String] = {
    implicitly[ClassTag[AnyRef]].asInstanceOf[ClassTag[String]]
    createStream(jssc.ssc, redisUrl, channels, patterns, storageLevel)
  }
}
