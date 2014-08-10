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

    override def isDefinedAt(o: Any): Boolean = _receiveWithLogging.isDefinedAt(o)

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
