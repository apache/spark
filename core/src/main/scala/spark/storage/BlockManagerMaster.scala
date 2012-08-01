package spark.storage

import java.io._
import java.util.{HashMap => JHashMap}

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import scala.util.Random

import akka.actor._
import akka.dispatch._
import akka.pattern.ask
import akka.remote._
import akka.util.Duration
import akka.util.Timeout
import akka.util.duration._

import spark.Logging
import spark.SparkException
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
  
case class GetLocations(blockId: String) extends ToBlockManagerMaster

case class GetLocationsMultipleBlockIds(blockIds: Array[String]) extends ToBlockManagerMaster
  
case class GetPeers(blockManagerId: BlockManagerId, size: Int) extends ToBlockManagerMaster
  
case class RemoveHost(host: String) extends ToBlockManagerMaster

case object StopBlockManagerMaster extends ToBlockManagerMaster


class BlockManagerMasterActor(val isLocal: Boolean) extends Actor with Logging {
  
  class BlockManagerInfo(
      timeMs: Long,
      maxMem: Long,
      maxDisk: Long) {
    private var lastSeenMs = timeMs
    private var remainedMem = maxMem
    private var remainedDisk = maxDisk
    private val blocks = new JHashMap[String, StorageLevel]
    
    def updateLastSeenMs() {
      lastSeenMs = System.currentTimeMillis() / 1000
    }
    
    def addBlock(blockId: String, storageLevel: StorageLevel, deserializedSize: Long, size: Long) =
        synchronized {
      updateLastSeenMs()
      
      if (blocks.containsKey(blockId)) {
        val oriLevel: StorageLevel = blocks.get(blockId)
        
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
      
      if (storageLevel.isValid) { 
        blocks.put(blockId, storageLevel)
        if (storageLevel.deserialized) {
          remainedMem -= deserializedSize
        }
        if (storageLevel.useMemory) {
          remainedMem -= size
        }
        if (storageLevel.useDisk) {
          remainedDisk -= size
        }
      } else {
        blocks.remove(blockId)
      }
    }

    def getLastSeenMs: Long = {
      return lastSeenMs
    }
    
    def getRemainedMem: Long = {
      return remainedMem
    }
    
    def getRemainedDisk: Long = {
      return remainedDisk
    }

    override def toString: String = {
      return "BlockManagerInfo " + timeMs + " " + remainedMem + " " + remainedDisk  
    }

    def clear() {
      blocks.clear()
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
    case RegisterBlockManager(blockManagerId, maxMemSize, maxDiskSize) =>
      register(blockManagerId, maxMemSize, maxDiskSize)

    case HeartBeat(blockManagerId, blockId, storageLevel, deserializedSize, size) =>
      heartBeat(blockManagerId, blockId, storageLevel, deserializedSize, size)

    case GetLocations(blockId) =>
      getLocations(blockId)

    case GetLocationsMultipleBlockIds(blockIds) =>
      getLocationsMultipleBlockIds(blockIds)

    case GetPeers(blockManagerId, size) =>
      getPeersDeterministic(blockManagerId, size)
      /*getPeers(blockManagerId, size)*/
      
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
    sender ! true
  }
  
  private def heartBeat(
      blockManagerId: BlockManagerId,
      blockId: String,
      storageLevel: StorageLevel,
      deserializedSize: Long,
      size: Long) {
    
    val startTimeMs = System.currentTimeMillis()
    val tmp = " " + blockManagerId + " " + blockId + " "
    
    if (blockId == null) {
      blockManagerInfo(blockManagerId).updateLastSeenMs()
      logDebug("Got in heartBeat 1" + tmp + " used " + Utils.getUsedTimeMs(startTimeMs))
      sender ! true
    }
    
    blockManagerInfo(blockManagerId).addBlock(blockId, storageLevel, deserializedSize, size)
    
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

class BlockManagerMaster(actorSystem: ActorSystem, isMaster: Boolean, isLocal: Boolean)
  extends Logging {

  val AKKA_ACTOR_NAME: String = "BlockMasterManager"
  val REQUEST_RETRY_INTERVAL_MS = 100
  val DEFAULT_MASTER_IP: String = System.getProperty("spark.master.host", "localhost")
  val DEFAULT_MASTER_PORT: Int = System.getProperty("spark.master.port", "7077").toInt
  val DEFAULT_MANAGER_IP: String = Utils.localHostName()
  val DEFAULT_MANAGER_PORT: String = "10902"

  val timeout = 10.seconds
  var masterActor: ActorRef = null

  if (isMaster) {
    masterActor = actorSystem.actorOf(
      Props(new BlockManagerMasterActor(isLocal)), name = AKKA_ACTOR_NAME)
    logInfo("Registered BlockManagerMaster Actor")
  } else {
    val url = "akka://spark@%s:%s/user/%s".format(
      DEFAULT_MASTER_IP, DEFAULT_MASTER_PORT, AKKA_ACTOR_NAME)
    logInfo("Connecting to BlockManagerMaster: " + url)
    masterActor = actorSystem.actorFor(url)
  }
  
  def stop() {
    if (masterActor != null) {
      communicate(StopBlockManagerMaster)
      masterActor = null
      logInfo("BlockManagerMaster stopped")
    }
  }

  // Send a message to the master actor and get its result within a default timeout, or
  // throw a SparkException if this fails.
  def askMaster(message: Any): Any = {
    try {
      val future = masterActor.ask(message)(timeout)
      return Await.result(future, timeout)
    } catch {
      case e: Exception =>
        throw new SparkException("Error communicating with BlockManagerMaster", e)
    }
  }

  // Send a one-way message to the master actor, to which we expect it to reply with true.
  def communicate(message: Any) {
    if (askMaster(message) != true) {
      throw new SparkException("Error reply received from BlockManagerMaster")
    }
  }
  
  def notifyADeadHost(host: String) {
    communicate(RemoveHost(host + ":" + DEFAULT_MANAGER_PORT))
    logInfo("Removed " + host + " successfully in notifyADeadHost")
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
    
    try {
      communicate(msg)
      logInfo("BlockManager registered successfully @ syncRegisterBlockManager")
      logDebug("Got in syncRegisterBlockManager 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
      return true
    } catch {
      case e: Exception =>
        logError("Failed in syncRegisterBlockManager", e)
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
    
    try {
      communicate(msg)
      logDebug("Heartbeat sent successfully")
      logDebug("Got in syncHeartBeat 1 " + tmp + " 1 " + Utils.getUsedTimeMs(startTimeMs))
      return true
    } catch {
      case e: Exception =>
        logError("Failed in syncHeartBeat", e)
        return false
    }
  }
  
  def mustGetLocations(msg: GetLocations): Seq[BlockManagerId] = {
    var res = syncGetLocations(msg)
    while (res == null) {
      logInfo("Failed to get locations " + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
      res = syncGetLocations(msg)
    }
    return res
  }
  
  def syncGetLocations(msg: GetLocations): Seq[BlockManagerId] = {
    val startTimeMs = System.currentTimeMillis()
    val tmp = " msg " + msg + " "
    logDebug("Got in syncGetLocations 0 " + tmp + Utils.getUsedTimeMs(startTimeMs))

    try {
      val answer = askMaster(msg).asInstanceOf[ArrayBuffer[BlockManagerId]]
      if (answer != null) {
        logDebug("GetLocations successful")
        logDebug("Got in syncGetLocations 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        return answer
      } else {
        logError("Master replied null in response to GetLocations")
        return null
      }
    } catch {
      case e: Exception =>
        logError("GetLocations failed", e)
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
    
    try {
      val answer = askMaster(msg).asInstanceOf[Seq[Seq[BlockManagerId]]]
      if (answer != null) {
        logDebug("GetLocationsMultipleBlockIds successful")
        logDebug("Got in syncGetLocationsMultipleBlockIds 1 " + tmp +
          Utils.getUsedTimeMs(startTimeMs))
        return answer
      } else {
        logError("Master replied null in response to GetLocationsMultipleBlockIds")
        return null
      }
    } catch {
      case e: Exception =>
        logError("GetLocationsMultipleBlockIds failed", e)
        return null
    }
  }
  
  def mustGetPeers(msg: GetPeers): Seq[BlockManagerId] = {
    var res = syncGetPeers(msg)
    while ((res == null) || (res.length != msg.size)) {
      logInfo("Failed to get peers " + msg)
      Thread.sleep(REQUEST_RETRY_INTERVAL_MS)
      res = syncGetPeers(msg)
    }
    
    return res
  }
  
  def syncGetPeers(msg: GetPeers): Seq[BlockManagerId] = {
    val startTimeMs = System.currentTimeMillis
    val tmp = " msg " + msg + " "
    logDebug("Got in syncGetPeers 0 " + tmp + Utils.getUsedTimeMs(startTimeMs))

    try {
      val answer = askMaster(msg).asInstanceOf[Seq[BlockManagerId]]
      if (answer != null) {
        logDebug("GetPeers successful")
        logDebug("Got in syncGetPeers 1 " + tmp + Utils.getUsedTimeMs(startTimeMs))
        return answer
      } else {
        logError("Master replied null in response to GetPeers")
        return null
      }
    } catch {
      case e: Exception =>
        logError("GetPeers failed", e)
        return null
    }
  }
}
