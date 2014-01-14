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

package org.apache.spark.streaming.flume

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.api.java.{JavaStreamingContext, JavaDStream}
import org.apache.spark.streaming.dstream.DStream

object FlumeUtils {
  /**
   * Create a input stream from a Flume source.
   * @param ssc      StreamingContext object
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def createStream (
      ssc: StreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2
    ): DStream[SparkFlumeEvent] = {
    val inputStream = new FlumeInputDStream[SparkFlumeEvent](ssc, hostname, port, storageLevel)
    inputStream
  }

  /**
   * Creates a input stream from a Flume source.
   * Storage level of the data will be the default StorageLevel.MEMORY_AND_DISK_SER_2.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   */
  def createStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int
    ): JavaDStream[SparkFlumeEvent] = {
    createStream(jssc.ssc, hostname, port)
  }

  /**
   * Creates a input stream from a Flume source.
   * @param hostname Hostname of the slave machine to which the flume data will be sent
   * @param port     Port of the slave machine to which the flume data will be sent
   * @param storageLevel  Storage level to use for storing the received objects
   */
  def createStream(
      jssc: JavaStreamingContext,
      hostname: String,
      port: Int,
      storageLevel: StorageLevel
    ): JavaDStream[SparkFlumeEvent] = {
    createStream(jssc.ssc, hostname, port, storageLevel)
  }
}
