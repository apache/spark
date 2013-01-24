package spark.storage

import akka.actor.Actor

import spark.{Logging, SparkException, Utils}


/**
 * An actor to take commands from the master to execute options. For example,
 * this is used to remove blocks from the slave's BlockManager.
 */
class BlockManagerSlaveActor(blockManager: BlockManager) extends Actor {
  override def receive = {
    case RemoveBlock(blockId) => blockManager.removeBlock(blockId)
  }
}
