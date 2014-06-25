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

package org.apache.spark.streaming.kinesis

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.streaming.api.java.JavaDStream

object KinesisUtils {

  def createStream(
      ssc: StreamingContext,
      accesskey:String="",
      accessSecretKey:String="",
      kinesisStream:String,
      kinesisEndpoint:String="https://kinesis.us-east-1.amazonaws.com", 
      storageLevel: StorageLevel = StorageLevel.MEMORY_ONLY_SER_2
    ): DStream[String] = {
    new KinesisInputDStream(ssc, accesskey,accessSecretKey,kinesisStream,kinesisEndpoint, storageLevel)
  }
  def createStream(
      jssc: JavaStreamingContext,
      accesskey:String,
      accessSecretKey:String,
      kinesisStream:String,
      kinesisEndpoint:String, 
      storageLevel: StorageLevel
    ): JavaDStream[String] = {
    new KinesisInputDStream(jssc.ssc, accesskey,accessSecretKey,kinesisStream,kinesisEndpoint, storageLevel)
  }
}
