package spark.storage

import java.util.{HashMap => JHashMap}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.collection.JavaConversions._
import scala.util.Random

import akka.actor.{Actor, ActorRef, Cancellable}
import akka.util.{Duration, Timeout}
import akka.util.duration._

import spark.{Logging, Utils}

/**
 * BlockManagerMasterActor is an actor on the master node to track statuses of
 * all slaves' block managers.
 */

private[spark]
object BlockManagerMasterActor {

  case class BlockStatus(storageLevel: StorageLevel, memSize: Long, diskSize: Long)

  class BlockManagerInfo(
      val blockManagerId: BlockManagerId,
      timeMs: Long,
      val maxMem: Long,
      val slaveActor: ActorRef)
    extends Logging {

    private var _lastSeenMs: Long = timeMs
    private var _remainingMem: Long = maxMem

    // Mapping from block id to its status.
    private val _blocks = new JHashMap[String, BlockStatus]

    logInfo("Registering block manager %s:%d with %s RAM".format(
      blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(maxMem)))

    def updateLastSeenMs() {
      _lastSeenMs = System.currentTimeMillis()
    }

    def updateBlockInfo(blockId: String, storageLevel: StorageLevel, memSize: Long, diskSize: Long)
      : Unit = synchronized {

      updateLastSeenMs()

      if (_blocks.containsKey(blockId)) {
        // The block exists on the slave already.
        val originalLevel: StorageLevel = _blocks.get(blockId).storageLevel

        if (originalLevel.useMemory) {
          _remainingMem += memSize
        }
      }

      if (storageLevel.isValid) {
        // isValid means it is either stored in-memory or on-disk.
        _blocks.put(blockId, BlockStatus(storageLevel, memSize, diskSize))
        if (storageLevel.useMemory) {
          _remainingMem -= memSize
          logInfo("Added %s in memory on %s:%d (size: %s, free: %s)".format(
            blockId, blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(memSize),
            Utils.memoryBytesToString(_remainingMem)))
        }
        if (storageLevel.useDisk) {
          logInfo("Added %s on disk on %s:%d (size: %s)".format(
            blockId, blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(diskSize)))
        }
      } else if (_blocks.containsKey(blockId)) {
        // If isValid is not true, drop the block.
        val blockStatus: BlockStatus = _blocks.get(blockId)
        _blocks.remove(blockId)
        if (blockStatus.storageLevel.useMemory) {
          _remainingMem += blockStatus.memSize
          logInfo("Removed %s on %s:%d in memory (size: %s, free: %s)".format(
            blockId, blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(memSize),
            Utils.memoryBytesToString(_remainingMem)))
        }
        if (blockStatus.storageLevel.useDisk) {
          logInfo("Removed %s on %s:%d on disk (size: %s)".format(
            blockId, blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(diskSize)))
        }
      }
    }

    def remainingMem: Long = _remainingMem

    def lastSeenMs: Long = _lastSeenMs

    def blocks: JHashMap[String, BlockStatus] = _blocks

    override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

    def clear() {
      _blocks.clear()
    }
  }
}


private[spark]
class BlockManagerMasterActor(val isLocal: Boolean) extends Actor with Logging {

  // Mapping from block manager id to the block manager's information.
  private val blockManagerInfo =
    new HashMap[BlockManagerId, BlockManagerMasterActor.BlockManagerInfo]

  // Mapping from host name to block manager id.
  private val blockManagerIdByHost = new HashMap[String, BlockManagerId]

  // Mapping from block id to the set of block managers that have the block.
  private val blockInfo = new JHashMap[String, Pair[Int, HashSet[BlockManagerId]]]

  initLogging()

  val slaveTimeout = System.getProperty("spark.storage.blockManagerSlaveTimeoutMs",
    "" + (BlockManager.getHeartBeatFrequencyFromSystemProperties * 3)).toLong

  val checkTimeoutInterval = System.getProperty("spark.storage.blockManagerTimeoutIntervalMs",
    "5000").toLong

  var timeoutCheckingTask: Cancellable = null

  override def preStart() {
    if (!BlockManager.getDisableHeartBeatsForTesting) {
      timeoutCheckingTask = context.system.scheduler.schedule(
        0.seconds, checkTimeoutInterval.milliseconds, self, ExpireDeadHosts)
    }
    super.preStart()
  }

  def removeBlockManager(blockManagerId: BlockManagerId) {
    val info = blockManagerInfo(blockManagerId)
    blockManagerIdByHost.remove(blockManagerId.ip)
    blockManagerInfo.remove(blockManagerId)
    var iterator = info.blocks.keySet.iterator
    while (iterator.hasNext) {
      val blockId = iterator.next
      val locations = blockInfo.get(blockId)._2
      locations -= blockManagerId
      if (locations.size == 0) {
        blockInfo.remove(locations)
      }
    }
  }

  def expireDeadHosts() {
    logDebug("Checking for hosts with no recent heart beats in BlockManagerMaster.")
    val now = System.currentTimeMillis()
    val minSeenTime = now - slaveTimeout
    val toRemove = new HashSet[BlockManagerId]
    for (info <- blockManagerInfo.values) {
      if (info.lastSeenMs < minSeenTime) {
        logWarning("Removing BlockManager " + info.blockManagerId + " with no recent heart beats")
        toRemove += info.blockManagerId
      }
    }
    // TODO: Remove corresponding block infos
    toRemove.foreach(removeBlockManager)
  }

  def removeHost(host: String) {
    logInfo("Trying to remove the host: " + host + " from BlockManagerMaster.")
    logInfo("Previous hosts: " + blockManagerInfo.keySet.toSeq)
    blockManagerIdByHost.get(host).foreach(removeBlockManager)
    logInfo("Current hosts: " + blockManagerInfo.keySet.toSeq)
    sender ! true
  }

  def heartBeat(blockManagerId: BlockManagerId) {
    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.ip == Utils.localHostName() && !isLocal) {
        sender ! true
      } else {
        sender ! false
      }
    } else {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      sender ! true
    }
  }

  def receive = {
    case RegisterBlockManager(blockManagerId, maxMemSize, slaveActor) =>
      register(blockManagerId, maxMemSize, slaveActor)

    case UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      blockUpdate(blockManagerId, blockId, storageLevel, deserializedSize, size)

    case GetLocations(blockId) =>
      getLocations(blockId)

    case GetLocationsMultipleBlockIds(blockIds) =>
      getLocationsMultipleBlockIds(blockIds)

    case GetPeers(blockManagerId, size) =>
      getPeersDeterministic(blockManagerId, size)
      /*getPeers(blockManagerId, size)*/

    case GetMemoryStatus =>
      getMemoryStatus

    case RemoveBlock(blockId) =>
      removeBlock(blockId)

    case RemoveHost(host) =>
      removeHost(host)
      sender ! true

    case StopBlockManagerMaster =>
      logInfo("Stopping BlockManagerMaster")
      sender ! true
      if (timeoutCheckingTask != null) {
        timeoutCheckingTask.cancel
      }
      context.stop(self)

    case ExpireDeadHosts =>
      expireDeadHosts()

    case HeartBeat(blockManagerId) =>
      heartBeat(blockManagerId)

    case other =>
      logInfo("Got unknown message: " + other)
  }

  // Remove a block from the slaves that have it. This can only be used to remove
  // blocks that the master knows about.
  private def removeBlock(blockId: String) {
    val block = blockInfo.get(blockId)
    if (block != null) {
      block._2.foreach { blockManagerId: BlockManagerId =>
        val blockManager = blockManagerInfo.get(blockManagerId)
        if (blockManager.isDefined) {
          // Remove the block from the slave's BlockManager.
          // Doesn't actually wait for a confirmation and the message might get lost.
          // If message loss becomes frequent, we should add retry logic here.
          blockManager.get.slaveActor ! RemoveBlock(blockId)
          // Remove the block from the master's BlockManagerInfo.
          blockManager.get.updateBlockInfo(blockId, StorageLevel.NONE, 0, 0)
        }
      }
      blockInfo.remove(blockId)
    }
    sender ! true
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def getMemoryStatus() {
    val res = blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
    sender ! res
  }

  private def register(blockManagerId: BlockManagerId, maxMemSize: Long, slaveActor: ActorRef) {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " "
    logDebug("Got in register 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    if (blockManagerIdByHost.contains(blockManagerId.ip) &&
        blockManagerIdByHost(blockManagerId.ip) != blockManagerId) {
      val oldId = blockManagerIdByHost(blockManagerId.ip)
      logInfo("Got second registration for host " + blockManagerId +
              "; removing old slave " + oldId)
      removeBlockManager(oldId)
    }
    if (blockManagerId.ip == Utils.localHostName() && !isLocal) {
      logInfo("Got Register Msg from master node, don't register it")
    } else {
      blockManagerInfo += (blockManagerId -> new BlockManagerMasterActor.BlockManagerInfo(
        blockManagerId, System.currentTimeMillis(), maxMemSize, slaveActor))
    }
    blockManagerIdByHost += (blockManagerId.ip -> blockManagerId)
    logDebug("Got in register 1" + tmp + Utils.getUsedTimeMs(startTimeMs))
    sender ! true
  }

  private def blockUpdate(
      blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " " + blockId + " "

    if (!blockManagerInfo.contains(blockManagerId)) {
      if (blockManagerId.ip == Utils.localHostName() && !isLocal) {
        // We intentionally do not register the master (except in local mode),
        // so we should not indicate failure.
        sender ! true
      } else {
        sender ! false
      }
      return
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      logDebug("Got in block update 1" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
      sender ! true
      return
    }

    blockManagerInfo(blockManagerId).updateBlockInfo(blockId, storageLevel, memSize, diskSize)

    var locations: HashSet[BlockManagerId] = null
    if (blockInfo.containsKey(blockId)) {
      locations = blockInfo.get(blockId)._2
    } else {
      locations = new HashSet[BlockManagerId]
      blockInfo.put(blockId, (storageLevel.replication, locations))
    }

    if (storageLevel.isValid) {
      locations += blockManagerId
    } else {
      locations.remove(blockManagerId)
    }

    if (locations.size == 0) {
      blockInfo.remove(blockId)
    }
    sender ! true
  }

  private def getLocations(blockId: String) {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockId + " "
    logDebug("Got in getLocations 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    if (blockInfo.containsKey(blockId)) {
      var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
      res.appendAll(blockInfo.get(blockId)._2)
      logDebug("Got in getLocations 1" + tmp + " as "+ res.toSeq + " at "
          + Utils.getUsedTimeMs(startTimeMs))
      sender ! res.toSeq
    } else {
      logDebug("Got in getLocations 2" + tmp + Utils.getUsedTimeMs(startTimeMs))
      var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
      sender ! res
    }
  }

  private def getLocationsMultipleBlockIds(blockIds: Array[String]) {
    def getLocations(blockId: String): Seq[BlockManagerId] = {
      val tmp = blockId
      logDebug("Got in getLocationsMultipleBlockIds Sub 0 " + tmp)
      if (blockInfo.containsKey(blockId)) {
        var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
        res.appendAll(blockInfo.get(blockId)._2)
        logDebug("Got in getLocationsMultipleBlockIds Sub 1 " + tmp + " " + res.toSeq)
        return res.toSeq
      } else {
        logDebug("Got in getLocationsMultipleBlockIds Sub 2 " + tmp)
        var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
        return res.toSeq
      }
    }

    logDebug("Got in getLocationsMultipleBlockIds " + blockIds.toSeq)
    var res: ArrayBuffer[Seq[BlockManagerId]] = new ArrayBuffer[Seq[BlockManagerId]]
    for (blockId <- blockIds) {
      res.append(getLocations(blockId))
    }
    logDebug("Got in getLocationsMultipleBlockIds " + blockIds.toSeq + " : " + res.toSeq)
    sender ! res.toSeq
  }

  private def getPeers(blockManagerId: BlockManagerId, size: Int) {
    var peers: Array[BlockManagerId] = blockManagerInfo.keySet.toArray
    var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
    res.appendAll(peers)
    res -= blockManagerId
    val rand = new Random(System.currentTimeMillis())
    while (res.length > size) {
      res.remove(rand.nextInt(res.length))
    }
    sender ! res.toSeq
  }

  private def getPeersDeterministic(blockManagerId: BlockManagerId, size: Int) {
    var peers: Array[BlockManagerId] = blockManagerInfo.keySet.toArray
    var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]

    val peersWithIndices = peers.zipWithIndex
    val selfIndex = peersWithIndices.find(_._1 == blockManagerId).map(_._2).getOrElse(-1)
    if (selfIndex == -1) {
      throw new Exception("Self index for " + blockManagerId + " not found")
    }

    var index = selfIndex
    while (res.size < size) {
      index += 1
      if (index == selfIndex) {
        throw new Exception("More peer expected than available")
      }
      res += peers(index % peers.size)
    }
    sender ! res.toSeq
  }
}
