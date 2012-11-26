package spark.storage

import java.io._
import java.util.{HashMap => JHashMap}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet}
import scala.util.Random

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.{Duration, Timeout}
import akka.util.duration._

import spark.{Logging, SparkException, Utils}


private[spark]
sealed trait ToBlockManagerMaster

private[spark]
case class RegisterBlockManager(
    blockManagerId: BlockManagerId,
    maxMemSize: Long)
  extends ToBlockManagerMaster

private[spark]
class UpdateBlockInfo(
    var blockManagerId: BlockManagerId,
    var blockId: String,
    var storageLevel: StorageLevel,
    var memSize: Long,
    var diskSize: Long)
  extends ToBlockManagerMaster
  with Externalizable {

  def this() = this(null, null, null, 0, 0)  // For deserialization only

  override def writeExternal(out: ObjectOutput) {
    blockManagerId.writeExternal(out)
    out.writeUTF(blockId)
    storageLevel.writeExternal(out)
    out.writeInt(memSize.toInt)
    out.writeInt(diskSize.toInt)
  }

  override def readExternal(in: ObjectInput) {
    blockManagerId = new BlockManagerId()
    blockManagerId.readExternal(in)
    blockId = in.readUTF()
    storageLevel = new StorageLevel()
    storageLevel.readExternal(in)
    memSize = in.readInt()
    diskSize = in.readInt()
  }
}

private[spark]
object UpdateBlockInfo {
  def apply(blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long): UpdateBlockInfo = {
    new UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize)
  }

  // For pattern-matching
  def unapply(h: UpdateBlockInfo): Option[(BlockManagerId, String, StorageLevel, Long, Long)] = {
    Some((h.blockManagerId, h.blockId, h.storageLevel, h.memSize, h.diskSize))
  }
}

private[spark]
case class GetLocations(blockId: String) extends ToBlockManagerMaster

private[spark]
case class GetLocationsMultipleBlockIds(blockIds: Array[String]) extends ToBlockManagerMaster

private[spark]
case class GetPeers(blockManagerId: BlockManagerId, size: Int) extends ToBlockManagerMaster

private[spark]
case class RemoveHost(host: String) extends ToBlockManagerMaster

private[spark]
case object StopBlockManagerMaster extends ToBlockManagerMaster

private[spark]
case object GetMemoryStatus extends ToBlockManagerMaster


private[spark] class BlockManagerMasterActor(val isLocal: Boolean) extends Actor with Logging {

  class BlockManagerInfo(
      val blockManagerId: BlockManagerId,
      timeMs: Long,
      val maxMem: Long) {
    private var _lastSeenMs = timeMs
    private var _remainingMem = maxMem
    private val _blocks = new JHashMap[String, StorageLevel]

    logInfo("Registering block manager %s:%d with %s RAM".format(
      blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(maxMem)))

    def updateLastSeenMs() {
      _lastSeenMs = System.currentTimeMillis() / 1000
    }

    def updateBlockInfo(blockId: String, storageLevel: StorageLevel, memSize: Long, diskSize: Long)
      : Unit = synchronized {

      updateLastSeenMs()

      if (_blocks.containsKey(blockId)) {
        // The block exists on the slave already.
        val originalLevel: StorageLevel = _blocks.get(blockId)

        if (originalLevel.useMemory) {
          _remainingMem += memSize
        }
      }

      if (storageLevel.isValid) {
        // isValid means it is either stored in-memory or on-disk.
        _blocks.put(blockId, storageLevel)
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
        val originalLevel: StorageLevel = _blocks.get(blockId)
        _blocks.remove(blockId)
        if (originalLevel.useMemory) {
          _remainingMem += memSize
          logInfo("Removed %s on %s:%d in memory (size: %s, free: %s)".format(
            blockId, blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(memSize),
            Utils.memoryBytesToString(_remainingMem)))
        }
        if (originalLevel.useDisk) {
          logInfo("Removed %s on %s:%d on disk (size: %s)".format(
            blockId, blockManagerId.ip, blockManagerId.port, Utils.memoryBytesToString(diskSize)))
        }
      }
    }

    def remainingMem: Long = _remainingMem

    def lastSeenMs: Long = _lastSeenMs

    override def toString: String = "BlockManagerInfo " + timeMs + " " + _remainingMem

    def clear() {
      _blocks.clear()
    }
  }

  private val blockManagerInfo = new HashMap[BlockManagerId, BlockManagerInfo]
  private val blockInfo = new JHashMap[String, Pair[Int, HashSet[BlockManagerId]]]

  initLogging()

  def removeHost(host: String) {
    logInfo("Trying to remove the host: " + host + " from BlockManagerMaster.")
    logInfo("Previous hosts: " + blockManagerInfo.keySet.toSeq)
    val ip = host.split(":")(0)
    val port = host.split(":")(1)
    blockManagerInfo.remove(new BlockManagerId(ip, port.toInt))
    logInfo("Current hosts: " + blockManagerInfo.keySet.toSeq)
    sender ! true
  }

  def receive = {
    case RegisterBlockManager(blockManagerId, maxMemSize) =>
      register(blockManagerId, maxMemSize)

    case UpdateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      updateBlockInfo(blockManagerId, blockId, storageLevel, deserializedSize, size)

    case GetLocations(blockId) =>
      getLocations(blockId)

    case GetLocationsMultipleBlockIds(blockIds) =>
      getLocationsMultipleBlockIds(blockIds)

    case GetPeers(blockManagerId, size) =>
      getPeersDeterministic(blockManagerId, size)
      /*getPeers(blockManagerId, size)*/

    case GetMemoryStatus =>
      getMemoryStatus

    case RemoveHost(host) =>
      removeHost(host)
      sender ! true

    case StopBlockManagerMaster =>
      logInfo("Stopping BlockManagerMaster")
      sender ! true
      context.stop(self)

    case other =>
      logInfo("Got unknown message: " + other)
  }

  // Return a map from the block manager id to max memory and remaining memory.
  private def getMemoryStatus() {
    val res = blockManagerInfo.map { case(blockManagerId, info) =>
      (blockManagerId, (info.maxMem, info.remainingMem))
    }.toMap
    sender ! res
  }

  private def register(blockManagerId: BlockManagerId, maxMemSize: Long) {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " "
    logDebug("Got in register 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    if (blockManagerId.ip == Utils.localHostName() && !isLocal) {
      logInfo("Got Register Msg from master node, don't register it")
    } else {
      blockManagerInfo += (blockManagerId -> new BlockManagerInfo(
        blockManagerId, System.currentTimeMillis() / 1000, maxMemSize))
    }
    logDebug("Got in register 1" + tmp + Utils.getUsedTimeMs(startTimeMs))
    sender ! true
  }

  private def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long) {

    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " " + blockId + " "

    if (!blockManagerInfo.contains(blockManagerId)) {
      // Can happen if this is from a locally cached partition on the master
      sender ! true
      return
    }

    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      logDebug("Got in updateBlockInfo 1" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
      sender ! true
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

private[spark] class BlockManagerMaster(actorSystem: ActorSystem, isMaster: Boolean, isLocal: Boolean)
  extends Logging {

  val actorName = "BlockMasterManager"
  val timeout = 10.seconds
  val maxAttempts = 5

  var masterActor = if (isMaster) {
    val actor = actorSystem.actorOf(Props(new BlockManagerMasterActor(isLocal)), name = actorName)
    logInfo("Registered BlockManagerMaster Actor")
    actor
  } else {
    val host = System.getProperty("spark.master.host", "localhost")
    val port = System.getProperty("spark.master.port", "7077").toInt
    val url = "akka://spark@%s:%s/user/%s".format(host, port, actorName)
    val actor = actorSystem.actorFor(url)
    logInfo("Connecting to BlockManagerMaster: " + url)
    actor
  }

  /**
   * Send a message to the master actor and get its result within a default timeout, or
   * throw a SparkException if this fails.
   */
  private def ask[T](message: Any): T = {
    // TODO: Consider removing multiple attempts
    if (masterActor == null) {
      throw new SparkException("Error sending message to BlockManager as masterActor is null " +
        "[message = " + message + "]")
    }
    var attempts = 0
    var lastException: Exception = null
    while (attempts < maxAttempts) {
      attempts += 1
        try {
        val future = masterActor.ask(message)(timeout)
        val result = Await.result(future, timeout)
        if (result == null) {
          throw new Exception("BlockManagerMaster returned null")
        }
        return result.asInstanceOf[T]
      } catch {
        case ie: InterruptedException =>
          throw ie
        case e: Exception =>
          lastException = e
          logWarning(
            "Error sending message to BlockManagerMaster in " + attempts + " attempts", e)
      }
      Thread.sleep(100)
    }
    throw new SparkException(
      "Error sending message to BlockManagerMaster [message = " + message + "]", lastException)
  }

  /**
   * Send a one-way message to the master actor, to which we expect it to reply with true
   */
  private def tell(message: Any) {
    if (!ask[Boolean](message)) {
      throw new SparkException("Telling master a message returned false")
    }
  }

  /**
   * Register the BlockManager's id with the master
   */
  def registerBlockManager(blockManagerId: BlockManagerId, maxMemSize: Long) {
    logInfo("Trying to register BlockManager")
    tell(RegisterBlockManager(blockManagerId, maxMemSize))
    logInfo("Registered BlockManager")
  }

  def updateBlockInfo(
      blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      memSize: Long,
      diskSize: Long
    ) {
    tell(UpdateBlockInfo(blockManagerId, blockId, storageLevel, memSize, diskSize))
    logInfo("Updated info of block " + blockId)
  }

  /** Get locations of the blockId from the master */
  def getLocations(blockId: String): Seq[BlockManagerId] = {
    ask[Seq[BlockManagerId]](GetLocations(blockId))
  }

  /** Get locations of multiple blockIds from the master */
  def getLocations(blockIds: Array[String]): Seq[Seq[BlockManagerId]] = {
    ask[Seq[Seq[BlockManagerId]]](GetLocationsMultipleBlockIds(blockIds))
  }

  /** Get ids of other nodes in the cluster from the master */
  def getPeers(blockManagerId: BlockManagerId, numPeers: Int): Seq[BlockManagerId] = {
    val result = ask[Seq[BlockManagerId]](GetPeers(blockManagerId, numPeers))
    if (result.length != numPeers) {
      throw new SparkException(
        "Error getting peers, only got " + result.size + " instead of " + numPeers)
    }
    result
  }

  /** Notify the master of a dead node */
  def notifyADeadHost(host: String) {
    tell(RemoveHost(host + ":10902"))
    logInfo("Told BlockManagerMaster to remove dead host " + host)
  }

  /** Get the memory status form the master */
  def getMemoryStatus(): Map[BlockManagerId, (Long, Long)] = {
    ask[Map[BlockManagerId, (Long, Long)]](GetMemoryStatus)
  }

  /** Stop the master actor, called only on the Spark master node */
  def stop() {
    if (masterActor != null) {
      tell(StopBlockManagerMaster)
      masterActor = null
      logInfo("BlockManagerMaster stopped")
    }
  }
}
