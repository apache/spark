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


private[spark] class BlockManagerMaster(
    val actorSystem: ActorSystem,
    isMaster: Boolean,
    isLocal: Boolean,
    masterIp: String,
    masterPort: Int)
  extends Logging {

  val AKKA_RETRY_ATTEMPTS: Int = System.getProperty("spark.akka.num.retries", "3").toInt
  val AKKA_RETRY_INTERVAL_MS: Int = System.getProperty("spark.akka.retry.wait", "3000").toInt

  val MASTER_AKKA_ACTOR_NAME = "BlockMasterManager"
  val SLAVE_AKKA_ACTOR_NAME = "BlockSlaveManager"
  val DEFAULT_MANAGER_IP: String = Utils.localHostName()

  val timeout = 10.seconds
  var masterActor: ActorRef = {
    if (isMaster) {
      val masterActor = actorSystem.actorOf(Props(new BlockManagerMasterActor(isLocal)),
        name = MASTER_AKKA_ACTOR_NAME)
      logInfo("Registered BlockManagerMaster Actor")
      masterActor
    } else {
      val url = "akka://spark@%s:%s/user/%s".format(masterIp, masterPort, MASTER_AKKA_ACTOR_NAME)
      logInfo("Connecting to BlockManagerMaster: " + url)
      actorSystem.actorFor(url)
    }
  }

  /** Remove a dead executor from the master actor. This is only called on the master side. */
  def removeExecutor(execId: String) {
    tell(RemoveExecutor(execId))
    logInfo("Removed " + execId + " successfully in removeExecutor")
  }

  /**
   * Send the master actor a heart beat from the slave. Returns true if everything works out,
   * false if the master does not know about the given block manager, which means the block
   * manager should re-register.
   */
  def sendHeartBeat(blockManagerId: BlockManagerId): Boolean = {
    askMasterWithRetry[Boolean](HeartBeat(blockManagerId))
  }

  /** Register the BlockManager's id with the master. */
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
    val res = askMasterWithRetry[Boolean](
      UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logInfo("Updated info of block " + blockId)
    res
  }

  /** Get locations of the blockId from the master */
  def getLocations(blockId: String): Seq[BlockManagerId] = {
    askMasterWithRetry[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /** Get locations of multiple blockIds from the master */
  def getLocations(blockIds: Array[String]): Seq[Seq[BlockManagerId]] = {
    askMasterWithRetry[Seq[Seq[BlockManagerId]]](GetLocationsMultipleBlockIds(blockIds))
  }

  /** Get ids of other nodes in the cluster from the master */
  def getPeers(blockManagerId: BlockManagerId, numPeers: Int): Seq[BlockManagerId] = {
    val result = askMasterWithRetry[Seq[BlockManagerId]](GetPeers(blockManagerId, numPeers))
    if (result.length != numPeers) {
      throw new SparkException(
        "Error getting peers, only got " + result.size + " instead of " + numPeers)
    }
    result
  }

  /**
   * Remove a block from the slaves that have it. This can only be used to remove
   * blocks that the master knows about.
   */
  def removeBlock(blockId: String) {
    askMasterWithRetry(RemoveBlock(blockId))
  }

  /**
   * Return the memory status for each block manager, in the form of a map from
   * the block manager's id to two long values. The first value is the maximum
   * amount of memory allocated for the block manager, while the second is the
   * amount of remaining memory.
   */
  def getMemoryStatus: Map[BlockManagerId, (Long, Long)] = {
    askMasterWithRetry[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  def getStorageStatus: Array[StorageStatus] = {
    askMasterWithRetry[ArrayBuffer[StorageStatus]](GetStorageStatus).toArray
  }

  /** Stop the master actor, called only on the Spark master node */
  def stop() {
    if (masterActor != null) {
      tell(StopBlockManagerMaster)
      masterActor = null
      logInfo("BlockManagerMaster stopped")
    }
  }

  /** Send a one-way message to the master actor, to which we expect it to reply with true. */
  private def tell(message: Any) {
    if (!askMasterWithRetry[Boolean](message)) {
      throw new SparkException("BlockManagerMasterActor returned false, expected true.")
    }
  }

  /**
   * Send a message to the master actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  private def askMasterWithRetry[T](message: Any): T = {
    // TODO: Consider removing multiple attempts
    if (masterActor == null) {
      throw new SparkException("Error sending message to BlockManager as masterActor is null " +
        "[message = " + message + "]")
    }
    var attempts = 0
    var lastException: Exception = null
    while (attempts < AKKA_RETRY_ATTEMPTS) {
      attempts += 1
      try {
        val future = masterActor.ask(message)(timeout)
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
