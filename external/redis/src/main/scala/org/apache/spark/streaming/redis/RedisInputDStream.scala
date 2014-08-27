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

import scala.collection.Map
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._
import scala.reflect.ClassTag

import java.util.Properties
import java.util.concurrent.Executors
import java.io.IOException
import java.net.URI

import redis.RedisPubSub
import redis.api.pubsub._
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

import org.apache.spark.Logging
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream._
import org.apache.spark.streaming.receiver.Receiver

/**
 * Input stream that subscribe messages from a Redis server.
 * Uses com.livestream as Redis client https://github.com/Livestream/scredis
 * @param redisUrl Url of remote redis server (e.g. redis://host:port/db)
 * @param channel channel name to subscribe to
 * @param storageLevel RDD storage level.
 */

private[streaming]
class RedisInputDStream(
    @transient ssc_ : StreamingContext,
    redisUrl: String,
    channels: Seq[String],
    patterns: Seq[String],
    storageLevel: StorageLevel
  ) extends ReceiverInputDStream[String](ssc_) with Logging {
  
  def getReceiver(): Receiver[String] = {
    new RedisReceiver(redisUrl, channels, patterns, storageLevel)
  }
}

private[streaming] 
class RedisReceiver(
    redisUrl: String,
    channels: Seq[String],
    patterns: Seq[String],
    storageLevel: StorageLevel
  ) extends Receiver[String](storageLevel) {

  def onStop() {
    // redis.quit()
  }
  
  def onStart() {
    implicit val akkaSystem = akka.actor.ActorSystem()

    // This will fail if given a bad URL
    val parsedUrl = new java.net.URI(redisUrl)

    // Initializing Redis Client specifying redisUrl
    val redisPubSub = RedisPubSub(
      host = parsedUrl.getHost,
      port = parsedUrl.getPort,
      channels = channels,
      patterns = patterns,
      onMessage = (m: Message) => {
        // info(s"got message: ${m.data}")
        store(m.data)
      }
    )

  }
}
