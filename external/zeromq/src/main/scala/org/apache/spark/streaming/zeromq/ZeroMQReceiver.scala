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

import akka.actor.Actor
import akka.util.ByteString
import akka.zeromq._

import org.apache.spark.Logging
import org.apache.spark.streaming.receiver.ActorHelper

/**
 * A receiver to subscribe to ZeroMQ stream.
 */
private[streaming] class ZeroMQReceiver[T: ClassTag](publisherUrl: String,
  subscribe: Subscribe,
  bytesToObjects: Seq[ByteString] => Iterator[T])
  extends Actor with ActorHelper with Logging {

  override def preStart() = ZeroMQExtension(context.system)
    .newSocket(SocketType.Sub, Listener(self), Connect(publisherUrl), subscribe)

  def receive: Receive = {

    case Connecting => logInfo("connecting ...")

    case m: ZMQMessage =>
      logDebug("Received message for:" + m.frame(0))

      // We ignore first frame for processing as it is the topic
      val bytes = m.frames.tail
      store(bytesToObjects(bytes))

    case Closed => logInfo("received closed ")
  }
}
