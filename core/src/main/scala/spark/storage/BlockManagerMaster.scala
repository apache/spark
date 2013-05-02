package spark.storage

import java.io._
import java.util.{HashMap => JHashMap}

import scala.collection.JavaConverters._
import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.dispatch.Await
import akka.pattern.ask
import akka.util.{Duration, Timeout}
import akka.util.duration._

import spark.{Logging, SparkException, Utils}


private[spark] class BlockManagerMaster(var driverActor: ActorRef) extends Logging {

  val AKKA_RETRY_ATTEMPTS: Int = System.getProperty("spark.akka.num.retries", "3").toInt
  val AKKA_RETRY_INTERVAL_MS: Int = System.getProperty("spark.akka.retry.wait", "3000").toInt

  val DRIVER_AKKA_ACTOR_NAME = "BlockManagerMaster"

  val timeout = Duration.create(System.getProperty("spark.akka.askTimeout", "10").toLong, "seconds")

  /** Remove a dead executor from the driver actor. This is only called on the driver side. */
  def removeExecutor(execId: String) {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }

  /**
   * Send the driver actor a heart beat from the slave. Returns true if everything works out,
   * false if the driver does not know about the given block manager, which means the block
   * manager should re-register.
   */
  def sendHeartBeat(blockManagerId: BlockManagerId): Boolean = {
    askDriverWithReply[Boolean](HeartBeat(blockManagerId))
  }

  /** Register the BlockManager's id with the driver. */
  def registerBlockManager(
      blockManagerId: BlockManagerId, maxMemSize: Long, slaveActor: ActorRef) {
    logInfo("Trying to register BlockManager")
    tell(RegisterBlockManager(blockManagerId, maxMemSize, slaveActor))
    logInfo("Registered BlockManager")
  }

  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): Boolean = {
    val res = askDriverWithReply[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logInfo("Updated info of block " + blockId)
    res
  }

  /** Get locations of the blockId from the driver */
  def getLocations(blockId: String): Seq[BlockManagerId] = {
    askDriverWithReply[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /** Get locations of multiple blockIds from the driver */
  def getLocations(blockIds: Array[String]): Seq[Seq[BlockManagerId]] = {
    askDriverWithReply[Seq[Seq[BlockManagerId]]](GetLocationsMultipleBlockIds(blockIds))
  }

  /** Get ids of other nodes in the cluster from the driver */
  def getPeers(blockManagerId: BlockManagerId, numPeers: Int): Seq[BlockManagerId] = {
    val result = askDriverWithReply[Seq[BlockManagerId]](GetPeers(blockManagerId, numPeers))
    if (result.length != numPeers) {
      throw new SparkException(
        "Error getting peers, only got " + result.size + " instead of " + numPeers)
    }
    result
  }

  /**
   * Remove a block from the slaves that have it. This can only be used to remove
   * blocks that the driver knows about.
   */
  def removeBlock(blockId: String) {
    askDriverWithReply(RemoveBlock(blockId))
  }

  /**
   * Remove all blocks belonging to the given RDD.
   */
  def removeRdd(rddId: Int) {
    val rddBlockPrefix = "rdd_" + rddId + "_"
    // Get the list of blocks in block manager, and remove ones that are part of this RDD.
    // The runtime complexity is linear to the number of blocks persisted in the cluster.
    // It could be expensive if the cluster is large and has a lot of blocks persisted.
    getStorageStatus.flatMap(_.blocks).foreach { case(blockId, status) =>
      if (blockId.startsWith(rddBlockPrefix)) {
        removeBlock(blockId)
      }
    }
  }

  /**
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    askDriverWithReply[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  def getStorageStatus: Array[StorageStatus] = {
    askDriverWithReply[ArrayBuffer[StorageStatus]](GetStorageStatus).toArray
  }

  /** Stop the driver actor, called only on the Spark driver node */
  def stop() {
    if (driverActor != null) {
      tell(StopBlockManagerMaster)
      driverActor = null
      logInfo("BlockManagerMaster stopped")
    }
  }

  /** Send a one-way message to the master actor, to which we expect it to reply with true. */
  private def tell(message: Any) {
    if (!askDriverWithReply[Boolean](message)) {
      throw new SparkException("BlockManagerMasterActor returned false, expected true.")
    }
  }

  /**
   * Send a message to the driver actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  private def askDriverWithReply[T](message: Any): T = {
    // TODO: Consider removing multiple attempts
    if (driverActor == null) {
      throw new SparkException("Error sending message to BlockManager as driverActor is null " +
        "[message = " + message + "]")
    }
    var attempts = 0
    var lastException: Exception = null
    while (attempts < AKKA_RETRY_ATTEMPTS) {
      attempts += 1
      try {
        val future = driverActor.ask(message)(timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new Exception("BlockManagerMaster returned null")
        }
        return result.asInstanceOf[T]
      } catch {
        case ie: InterruptedException => throw ie
        case e: Exception =>
          lastException = e
          logWarning("Error sending message to BlockManagerMaster in " + attempts + " attempts", e)
      }
      Thread.sleep(AKKA_RETRY_INTERVAL_MS)
    }

    throw new SparkException(
      "Error sending message to BlockManagerMaster [message = " + message + "]", lastException)
  }

}
