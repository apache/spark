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

package org.apache.spark.util

import akka.actor.Actor
import org.slf4j.Logger

/**
 * A trait to enable logging all Akka actor messages. Here's an example of using this:
 *
 * {{{
 *   class BlockManagerMasterActor extends Actor with ActorLogReceive with Logging {
 *     ...
 *     override def receiveWithLogging = {
 *       case GetLocations(blockId) =>
 *         sender ! getLocations(blockId)
 *       ...
 *     }
 *     ...
 *   }
 * }}}
 *
 */
private[spark] trait ActorLogReceive {
  self: Actor =>

  override def receive: Actor.Receive = new Actor.Receive {

    private val _receiveWithLogging = receiveWithLogging

    override def isDefinedAt(o: Any): Boolean = {
      val handled = _receiveWithLogging.isDefinedAt(o)
      if (!handled) {
        log.debug(s"Received unexpected actor system event: $o")
      }
      handled
    }

    override def apply(o: Any): Unit = {
      if (log.isDebugEnabled) {
        log.debug(s"[actor] received message $o from ${self.sender}")
      }
      val start = System.nanoTime
      _receiveWithLogging.apply(o)
      val timeTaken = (System.nanoTime - start).toDouble / 1000000
      if (log.isDebugEnabled) {
        log.debug(s"[actor] handled message ($timeTaken ms) $o from ${self.sender}")
      }
    }
  }

  def receiveWithLogging: Actor.Receive

  protected def log: Logger
}
