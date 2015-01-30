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

import akka.actor.Props
import akka.util.ByteString
import akka.zeromq.Subscribe
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{PluggableInputDStream, InputDStream, ReflectedDStreamFactory}
import org.apache.spark.streaming.receiver.{ActorSupervisorStrategy, ActorReceiver}

/**
 * Creates [[org.apache.spark.streaming.zeromq.ZeroMQReceiver]] streams.
 *
 * This adapter class enables ZeroMQ streams to be instantiated via the
 * [[org.apache.spark.streaming.StreamingContext]] reflectedStream method.
 *
 * It expects two String arguments in streamParams:
 * 1. URL of the ZeroMQ publisher stream, e.g. "tcp://127.0.1.1:1234"
 * 2. topic to which to subscribe
 *
 * @param streamParams parameters to pass to the underlying ZeroMQReceiver
 */
class ReflectedZeroMQStreamFactory(streamParams: Seq[String])
  extends ReflectedDStreamFactory[String](streamParams) {

  /**
   * Creates a new ZeroMQ subscriber DStream[String] instance.
   *
   * @param ssc The active StreamingContext.
   * @return new InputDStream[String]
   */
  override def instantiateStream(ssc: StreamingContext): InputDStream[String] = {
    val receiver = new ActorReceiver[String](
      Props(new ZeroMQReceiver(streamParams(0),
        Subscribe(ByteString(streamParams(1))),
        (x: Seq[ByteString]) => x.map(_.utf8String).iterator )),
      "ZeroMQReceiverFromReflectedFactory",
      StorageLevel.MEMORY_AND_DISK_SER_2,
      ActorSupervisorStrategy.defaultStrategy
    )
    new PluggableInputDStream[String](ssc, receiver)
  }
}
