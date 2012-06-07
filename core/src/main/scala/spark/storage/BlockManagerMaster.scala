package spark.storage

import java.io._

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.Random

import akka.actor._
import akka.actor.Actor
import akka.actor.Actor._
import akka.util.duration._

import spark.Logging
import spark.Utils

sealed trait ToBlockManagerMaster

case class RegisterBlockManager(
    blockManagerId: BlockManagerId,
    maxMemSize: Long,
    maxDiskSize: Long)
  extends ToBlockManagerMaster
  
class HeartBeat(
    var blockManagerId: BlockManagerId,
    var blockId: String,
    var storageLevel: StorageLevel,
    var deserializedSize: Long,
    var size: Long)
  extends ToBlockManagerMaster
  with Externalizable {

  def this() = this(null, null, null, 0, 0)  // For deserialization only

  override def writeExternal(out: ObjectOutput) {
    blockManagerId.writeExternal(out)
    out.writeUTF(blockId)
    storageLevel.writeExternal(out)
    out.writeInt(deserializedSize.toInt)
    out.writeInt(size.toInt)
  }

  override def readExternal(in: ObjectInput) {
    blockManagerId = new BlockManagerId()
    blockManagerId.readExternal(in)
    blockId = in.readUTF()
    storageLevel = new StorageLevel()
    storageLevel.readExternal(in)
    deserializedSize = in.readInt()
    size = in.readInt()
  }
}

object HeartBeat {
  def apply(blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      deserializedSize: Long,
      size: Long): HeartBeat = {
    new HeartBeat(blockManagerId, blockId, storageLevel, deserializedSize, size)
  }

 
  // For pattern-matching
  def unapply(h: HeartBeat): Option[(BlockManagerId, String, StorageLevel, Long, Long)] = {
    Some((h.blockManagerId, h.blockId, h.storageLevel, h.deserializedSize, h.size))
  }
}
  
case class GetLocations(
    blockId: String)
  extends ToBlockManagerMaster

case class GetLocationsMultipleBlockIds(
    blockIds: Array[String])
  extends ToBlockManagerMaster
  
case class GetPeers(
    blockManagerId: BlockManagerId,
    size: Int)
  extends ToBlockManagerMaster
  
case class RemoveHost(
    host: String)
  extends ToBlockManagerMaster

class BlockManagerMaster(val isLocal: Boolean) extends Actor with Logging {
  class BlockManagerInfo(
      timeMs: Long,
      maxMem: Long,
      maxDisk: Long) {
    private var lastSeenMs = timeMs
    private var remainedMem = maxMem
    private var remainedDisk = maxDisk
    private val blocks = new HashMap[String, StorageLevel]
    
    def updateLastSeenMs() {
      lastSeenMs = System.currentTimeMillis() / 1000
    }
    
    def addBlock(blockId: String, storageLevel: StorageLevel, deserializedSize: Long, size: Long) =
        synchronized {
      updateLastSeenMs()
      
      if (blocks.contains(blockId)) {
        val oriLevel: StorageLevel = blocks(blockId)
        
        if (oriLevel.deserialized) {
          remainedMem += deserializedSize
        }
        if (oriLevel.useMemory) {
          remainedMem += size
        }
        if (oriLevel.useDisk) {
          remainedDisk += size
        }
      }

      blocks += (blockId -> storageLevel)

      if (storageLevel.deserialized) {
        remainedMem -= deserializedSize
      }
      if (storageLevel.useMemory) {
        remainedMem -= size
      }
      if (storageLevel.useDisk) {
        remainedDisk -= size
      }
      
      if (!(storageLevel.deserialized || storageLevel.useMemory || storageLevel.useDisk)) {
        blocks.remove(blockId)
      }
    }

    def getLastSeenMs(): Long = {
      return lastSeenMs
    }
    
    def getRemainedMem(): Long = {
      return remainedMem
    }
    
    def getRemainedDisk(): Long = {
      return remainedDisk
    }

    override def toString(): String = {
      return "BlockManagerInfo " + timeMs + " " + remainedMem + " " + remainedDisk  
    }
  }

  private val blockManagerInfo = new HashMap[BlockManagerId, BlockManagerInfo]
  private val blockIdMap = new HashMap[String, Pair[Int, HashSet[BlockManagerId]]]

  initLogging()
  
  def removeHost(host: String) {
    logInfo("Trying to remove the host: " + host + " from BlockManagerMaster.")
    logInfo("Previous hosts: " + blockManagerInfo.keySet.toSeq)
    val ip = host.split(":")(0)
    val port = host.split(":")(1)
    blockManagerInfo.remove(new BlockManagerId(ip, port.toInt))
    logInfo("Current hosts: " + blockManagerInfo.keySet.toSeq)
    self.reply(true)
  }

  def receive = {
    case RegisterBlockManager(blockManagerId, maxMemSize, maxDiskSize) =>
      register(blockManagerId, maxMemSize, maxDiskSize)

    case HeartBeat(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      heartBeat(blockManagerId, blockId, storageLevel, deserializedSize, size)

    case GetLocations(blockId) =>
      getLocations(blockId)

    case GetLocationsMultipleBlockIds(blockIds) =>
      getLocationsMultipleBlockIds(blockIds)

    case GetPeers(blockManagerId, size) =>
      getPeers_Deterministic(blockManagerId, size)
      /*getPeers(blockManagerId, size)*/
      
    case RemoveHost(host) =>
      removeHost(host)

    case msg => 
      logInfo("Got unknown msg: " + msg)
  }
  
  private def register(blockManagerId: BlockManagerId, maxMemSize: Long, maxDiskSize: Long) {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " "
    logDebug("Got in register 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    logInfo("Got Register Msg from " + blockManagerId)
    if (blockManagerId.ip == Utils.localHostName() && !isLocal) {
      logInfo("Got Register Msg from master node, don't register it")
    } else {
      blockManagerInfo += (blockManagerId -> new BlockManagerInfo(
        System.currentTimeMillis() / 1000, maxMemSize, maxDiskSize))
    }
    logDebug("Got in register 1" + tmp + Utils.getUsedTimeMs(startTimeMs))
    self.reply(true)
  }
  
  private def heartBeat(
      blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      deserializedSize: Long,
      size: Long) {
    
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " " + blockId + " "
    logDebug("Got in heartBeat 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    
    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      logDebug("Got in heartBeat 1" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
      self.reply(true)
    }
    
    blockManagerInfo(blockManagerId).addBlock(blockId, storageLevel, deserializedSize, size)
    logDebug("Got in heartBeat 2" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
    
    var locations: HashSet[BlockManagerId] = null
    if (blockIdMap.contains(blockId)) {
      locations = blockIdMap(blockId)._2
    } else {
      locations = new HashSet[BlockManagerId]
      blockIdMap += (blockId -> (storageLevel.replication, locations))
    }
    logDebug("Got in heartBeat 3" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
    
    if (storageLevel.deserialized || storageLevel.useDisk || storageLevel.useMemory) {
      locations += blockManagerId
    } else {
      locations.remove(blockManagerId)
    }
    logDebug("Got in heartBeat 4" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
    
    if (locations.size == 0) {
      blockIdMap.remove(blockId)
    }
    
    logDebug("Got in heartBeat 5" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
    self.reply(true)
  }
  
  private def getLocations(blockId: String) {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockId + " "
    logDebug("Got in getLocations 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    if (blockIdMap.contains(blockId)) {
      var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
      res.appendAll(blockIdMap(blockId)._2)
      logDebug("Got in getLocations 1" + tmp + " as "+ res.toSeq + " at " 
          + Utils.getUsedTimeMs(startTimeMs))
      self.reply(res.toSeq)
    } else {
      logDebug("Got in getLocations 2" + tmp + Utils.getUsedTimeMs(startTimeMs))
      var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
      self.reply(res)
    }
  }
  
  private def getLocationsMultipleBlockIds(blockIds: Array[String]) {
    def getLocations(blockId: String): Seq[BlockManagerId] = {
      val tmp = blockId
      logDebug("Got in getLocationsMultipleBlockIds Sub 0 " + tmp)
      if (blockIdMap.contains(blockId)) {
        var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
        res.appendAll(blockIdMap(blockId)._2)
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
    self.reply(res.toSeq)
  }

  private def getPeers(blockManagerId: BlockManagerId, size: Int) {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " "
    logDebug("Got in getPeers 0" + tmp + Utils.getUsedTimeMs(startTimeMs))
    var peers: Array[BlockManagerId] = blockManagerInfo.keySet.toArray
    var res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
    res.appendAll(peers)
    res -= blockManagerId
    val rand = new Random(System.currentTimeMillis())
    logDebug("Got in getPeers 1" + tmp + Utils.getUsedTimeMs(startTimeMs))
    while (res.length > size) {
      res.remove(rand.nextInt(res.length))
    }
    logDebug("Got in getPeers 2" + tmp + Utils.getUsedTimeMs(startTimeMs))
    self.reply(res.toSeq)
  }
  
  private def getPeers_Deterministic(blockManagerId: BlockManagerId, size: Int) {
    val startTimeMs = System.currentTimeMillis()
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
    val resStr = res.map(_.toString).reduceLeft(_ + ", " + _)
    logDebug("Got peers for " + blockManagerId + " as [" + resStr + "]")
    self.reply(res.toSeq)
  }
}

object BlockManagerMaster extends Logging {
  initLogging()

  val AKKA_ACTOR_NAME: String = "BlockMasterManager"
  val REQUEST_RETRY_INTERVAL_MS = 100
  val DEFAULT_MASTER_IP: String = System.getProperty("spark.master.host", "localhost")
  val DEFAULT_MASTER_PORT: Int = System.getProperty("spark.master.port", "7077").toInt
  val DEFAULT_MANAGER_IP: String = Utils.localHostName()
  val DEFAULT_MANAGER_PORT: String = "10902"

  implicit val TIME_OUT_SEC = Actor.Timeout(3000 millis)
  var masterActor: ActorRef = null

  def startBlockManagerMaster(isMaster: Boolean, isLocal: Boolean) {
    if (isMaster) {
      masterActor = actorOf(new BlockManagerMaster(isLocal))
      remote.register(AKKA_ACTOR_NAME, masterActor)
      logInfo("Registered BlockManagerMaster Actor: " + DEFAULT_MASTER_IP + ":" + DEFAULT_MASTER_PORT)
      masterActor.start()
    } else {
      masterActor = remote.actorFor(AKKA_ACTOR_NAME, DEFAULT_MASTER_IP, DEFAULT_MASTER_PORT)
    }
  }
  
  def notifyADeadHost(host: String) {
    (masterActor ? RemoveHost(host + ":" + DEFAULT_MANAGER_PORT)).as[Any] match {
      case Some(true) =>
        logInfo("Removed " + host + " successfully. @ notifyADeadHost")
      case Some(oops) =>
        logError("Failed @ notifyADeadHost: " + oops)
      case None =>
        logError("None @ notifyADeadHost.")
    }
  }

  def mustRegisterBlockManager(msg: RegisterBlockManager) {
    while (! syncRegisterBlockManager(msg)) {
      logWarning("Failed to register " + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
    }
  }

  def syncRegisterBlockManager(msg: RegisterBlockManager): Boolean = {
    //val masterActor = RemoteActor.select(node, name)
    val startTimeMs = System.currentTimeMillis()
    val tmp = " msg " + msg + " "
    logDebug("Got in syncRegisterBlockManager 0 " + tmp + Utils.getUsedTimeMs(startTimeMs))
    
    (masterActor ? msg).as[Any] match {
      case Some(true) => 
        logInfo("BlockManager registered successfully @ syncRegisterBlockManager.")
        logDebug("Got in syncRegisterBlockManager 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        return true
      case Some(oops) =>
        logError("Failed @ syncRegisterBlockManager: " + oops)
        return false
      case None =>
        logError("None @ syncRegisterBlockManager.")
        return false
    }
  }
  
  def mustHeartBeat(msg: HeartBeat) {
    while (! syncHeartBeat(msg)) {
      logWarning("Failed to send heartbeat" + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
    }
  }
  
  def syncHeartBeat(msg: HeartBeat): Boolean = {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " msg " + msg + " "
    logDebug("Got in syncHeartBeat " + tmp + " 0 " + Utils.getUsedTimeMs(startTimeMs))
    
    (masterActor ? msg).as[Any] match {
      case Some(true) =>
        logInfo("Heartbeat sent successfully.")
        logDebug("Got in syncHeartBeat " + tmp + " 1 " + Utils.getUsedTimeMs(startTimeMs))
        return true
      case Some(oops) =>
        logError("Failed: " + oops)
        return false
      case None => 
        logError("None.")
        return false
    }
  }
  
  def mustGetLocations(msg: GetLocations): Array[BlockManagerId] = {
    var res: Array[BlockManagerId] = syncGetLocations(msg)
    while (res == null) {
      logInfo("Failed to get locations " + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
      res = syncGetLocations(msg)
    }
    return res
  }
  
  def syncGetLocations(msg: GetLocations): Array[BlockManagerId] = {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " msg " + msg + " "
    logDebug("Got in syncGetLocations 0 " + tmp + Utils.getUsedTimeMs(startTimeMs))
    
    (masterActor ? msg).as[Seq[BlockManagerId]] match {
      case Some(arr) =>
        logDebug("GetLocations successfully.")
        logDebug("Got in syncGetLocations 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        val res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
        for (ele <- arr) {
          res += ele
        }
        logDebug("Got in syncGetLocations 2 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        return res.toArray
      case None => 
        logError("GetLocations call returned None.")
        return null
    }
  }

  def mustGetLocationsMultipleBlockIds(msg: GetLocationsMultipleBlockIds):
       Seq[Seq[BlockManagerId]] = {
    var res: Seq[Seq[BlockManagerId]] = syncGetLocationsMultipleBlockIds(msg)
    while (res == null) {
      logWarning("Failed to GetLocationsMultipleBlockIds " + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
      res = syncGetLocationsMultipleBlockIds(msg)
    }
    return res
  }
  
  def syncGetLocationsMultipleBlockIds(msg: GetLocationsMultipleBlockIds):
      Seq[Seq[BlockManagerId]] = {
    val startTimeMs = System.currentTimeMillis
    val tmp = " msg " + msg + " "
    logDebug("Got in syncGetLocationsMultipleBlockIds 0 " + tmp + Utils.getUsedTimeMs(startTimeMs))
    
    (masterActor ? msg).as[Any] match {
      case Some(arr: Seq[Seq[BlockManagerId]]) =>
        logDebug("GetLocationsMultipleBlockIds successfully: " + arr)
        logDebug("Got in syncGetLocationsMultipleBlockIds 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        return arr
      case Some(oops) =>
        logError("Failed: " + oops)
        return null
      case None => 
        logInfo("None.")
        return null
    }
  }
  
  def mustGetPeers(msg: GetPeers): Array[BlockManagerId] = {
    var res: Array[BlockManagerId] = syncGetPeers(msg)
    while ((res == null) || (res.length != msg.size)) {
      logInfo("Failed to get peers " + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
      res = syncGetPeers(msg)
    }
    
    return res
  }
  
  def syncGetPeers(msg: GetPeers): Array[BlockManagerId] = {
    val startTimeMs = System.currentTimeMillis
    val tmp = " msg " + msg + " "
    logDebug("Got in syncGetPeers 0 " + tmp + Utils.getUsedTimeMs(startTimeMs))
    
    (masterActor ? msg).as[Seq[BlockManagerId]] match {
      case Some(arr) =>
        logDebug("Got in syncGetPeers 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        val res: ArrayBuffer[BlockManagerId] = new ArrayBuffer[BlockManagerId]
        logInfo("GetPeers successfully: " + arr.length)
        res.appendAll(arr)
        logDebug("Got in syncGetPeers 2 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        return res.toArray
      case None => 
        logError("GetPeers call returned None.")
        return null
    }
  }
}
