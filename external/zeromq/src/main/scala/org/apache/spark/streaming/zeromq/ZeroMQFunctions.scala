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

package org.apache.spark.streaming.zeromq

import scala.reflect.ClassTag

import akka.actor.{Props, SupervisorStrategy}
import akka.util.ByteString
import akka.zeromq.Subscribe

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming._
import org.apache.spark.streaming.receivers._

/**
 * Extra ZeroMQ input stream functions available on [[org.apache.spark.streaming.StreamingContext]]
 * through implicit conversions. Import org.apache.spark.streaming.zeromq._ to use these functions.
 */
class ZeroMQFunctions(ssc: StreamingContext) {
  /**
   * Create an input stream that receives messages pushed by a zeromq publisher.
   * @param publisherUrl Url of remote zeromq publisher
   * @param subscribe topic to subscribe to
   * @param bytesToObjects A zeroMQ stream publishes sequence of frames for each topic
   *                       and each frame has sequence of byte thus it needs the converter
   *                       (which might be deserializer of bytes) to translate from sequence
   *                       of sequence of bytes, where sequence refer to a frame
   *                       and sub sequence refer to its payload.
   * @param storageLevel RDD storage level. Defaults to memory-only.
   */
  def zeroMQStream[T: ClassTag](
      publisherUrl: String,
      subscribe: Subscribe,
      bytesToObjects: Seq[ByteString] ⇒ Iterator[T],
      storageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK_SER_2,
      supervisorStrategy: SupervisorStrategy = ReceiverSupervisorStrategy.defaultStrategy
    ): DStream[T] = {
    ssc.actorStream(Props(new ZeroMQReceiver(publisherUrl, subscribe, bytesToObjects)),
        "ZeroMQReceiver", storageLevel, supervisorStrategy)
  }
}
  
