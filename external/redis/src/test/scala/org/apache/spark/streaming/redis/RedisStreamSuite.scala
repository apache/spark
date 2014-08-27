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

import scala.collection.mutable.{ArrayBuffer, SynchronizedBuffer}

import org.apache.spark.streaming.{TestOutputStream, StreamingContext, TestSuiteBase}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream

import org.apache.spark.streaming.api.java.JavaReceiverInputDStream

import redis.RedisClient
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class RedisStreamSuite extends TestSuiteBase {

  // Note, this test case requires a redis-server to be running
  // We will silently fail if no such server is running
  test("redis input stream") {

    implicit val akkaSystem = akka.actor.ActorSystem()

    try {
      val redisUrl = "redis://localhost:6379"
      val channel = "def"
      val channels = Seq(channel)
      val patterns = Seq()

      val input = Seq(1, 2, 3, 4, 5)

      // We'll create a new redis topic and check for events in the stream
      val redis = new RedisClient("localhost", 6379)

      // We will fail silently if we're unable to connect to a running redis server
      try {
        Await.ready(redis.ping, 1 second)
      } catch {
        // If we cannot connect to redis, simply skip this test
        case redis.actors.NoConnectionException => return
      }

      // Create streaming context
      val ssc = new StreamingContext(master, framework, batchDuration)
      val redisStream: JavaReceiverInputDStream[String] =
        RedisUtils.createStream(ssc, redisUrl, channels, patterns, StorageLevel.MEMORY_AND_DISK)

      val outputBuffer = new ArrayBuffer[Seq[String]]
        with SynchronizedBuffer[Seq[String]]
      val outputStream = new TestOutputStream(redisStream.receiverInputDStream, outputBuffer)

      outputStream.register()

      ssc.start()

      // Wait to sync
      Thread.sleep(1000)

      // Publish test data
      input.foreach(key => {
        // publishes "key is $key" to the channel
        redis.publish(channel, s"key is $key")
      })

      // Wait to sync
      Thread.sleep(1000)

      ssc.stop()

      logInfo(s"Got $outputBuffer")
      assert(outputBuffer.flatten.toList === input.map(k => s"key is $k"))
    } finally {
      // Ensure we shut down
      akkaSystem.shutdown()  
    }
  }
}
