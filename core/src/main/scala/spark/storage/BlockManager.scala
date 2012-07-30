package spark.storage

import java.io._
import java.nio._
import java.nio.channels.FileChannel.MapMode
import java.util.{HashMap => JHashMap}
import java.util.LinkedHashMap
import java.util.concurrent.ConcurrentHashMap
import java.util.Collections

import scala.actors._
import scala.actors.Actor._
import scala.actors.Future
import scala.actors.Futures.future
import scala.actors.remote._
import scala.actors.remote.RemoteActor._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions._

import it.unimi.dsi.fastutil.io._

import spark.CacheTracker
import spark.Logging
import spark.Serializer
import spark.SizeEstimator
import spark.SparkEnv
import spark.SparkException
import spark.Utils
import spark.util.ByteBufferInputStream
import spark.network._

class BlockManagerId(var ip: String, var port: Int) extends Externalizable {
  def this() = this(null, 0)

  override def writeExternal(out: ObjectOutput) {
    out.writeUTF(ip)
    out.writeInt(port)
  }

  override def readExternal(in: ObjectInput) {
    ip = in.readUTF()
    port = in.readInt()
  }

  override def toString = "BlockManagerId(" + ip + ", " + port + ")"

  override def hashCode = ip.hashCode * 41 + port

  override def equals(that: Any) = that match {
    case id: BlockManagerId => port == id.port && ip == id.ip
    case _ => false
  }
}


case class BlockException(blockId: String, message: String, ex: Exception = null) extends Exception(message)


class BlockLocker(numLockers: Int) {
  private val hashLocker = Array.fill(numLockers)(new Object())
  
  def getLock(blockId: String): Object = {
    return hashLocker(math.abs(blockId.hashCode % numLockers))
  }
}


class BlockManager(val master: BlockManagerMaster, val serializer: Serializer, maxMemory: Long)
  extends Logging {

  case class BlockInfo(level: StorageLevel, tellMaster: Boolean)

  private val NUM_LOCKS = 337
  private val locker = new BlockLocker(NUM_LOCKS)

  private val blockInfo = new ConcurrentHashMap[String, BlockInfo]()
  private val memoryStore: BlockStore = new MemoryStore(this, maxMemory)
  private val diskStore: BlockStore = new DiskStore(this, 
    System.getProperty("spark.local.dir", System.getProperty("java.io.tmpdir")))
  
  val connectionManager = new ConnectionManager(0)
  
  val connectionManagerId = connectionManager.id
  val blockManagerId = new BlockManagerId(connectionManagerId.host, connectionManagerId.port)
  
  // TODO: This will be removed after cacheTracker is removed from the code base.
  var cacheTracker: CacheTracker = null

  initLogging()

  initialize()

  /**
   * Construct a BlockManager with a memory limit set based on system properties.
   */
  def this(master: BlockManagerMaster, serializer: Serializer) = {
    this(master, serializer, BlockManager.getMaxMemoryFromSystemProperties)
  }

  /**
   * Initialize the BlockManager. Register to the BlockManagerMaster, and start the
   * BlockManagerWorker actor.
   */
  private def initialize() {
    master.mustRegisterBlockManager(
      RegisterBlockManager(blockManagerId, maxMemory, maxMemory))
    BlockManagerWorker.startBlockManagerWorker(this)
  }

  /**
   * Get storage level of local block. If no info exists for the block, then returns null.
   */
  def getLevel(blockId: String): StorageLevel = {
    val info = blockInfo.get(blockId)
    if (info != null) info.level else null
  }

  /**
   * Change storage level for a local block and tell master is necesary. 
   * If new level is invalid, then block info (if it exists) will be silently removed.
   */
  def setLevel(blockId: String, level: StorageLevel, tellMaster: Boolean = true) {
    if (level == null) {
      throw new IllegalArgumentException("Storage level is null")
    }
    
    // If there was earlier info about the block, then use earlier tellMaster
    val oldInfo = blockInfo.get(blockId)
    val newTellMaster = if (oldInfo != null) oldInfo.tellMaster else tellMaster
    if (oldInfo != null && oldInfo.tellMaster != tellMaster) {
      logWarning("Ignoring tellMaster setting as it is different from earlier setting")
    }

    // If level is valid, store the block info, else remove the block info
    if (level.isValid) {
      blockInfo.put(blockId, new BlockInfo(level, newTellMaster))
      logDebug("Info for block " + blockId + " updated with new level as " + level) 
    } else {
      blockInfo.remove(blockId)
      logDebug("Info for block " + blockId + " removed as new level is null or invalid") 
    }
   
    // Tell master if necessary
    if (newTellMaster) {
      logDebug("Told master about block " + blockId)
      notifyMaster(HeartBeat(blockManagerId, blockId, level, 0, 0)) 
    } else {
      logDebug("Did not tell master about block " + blockId)
    }
  }

  /**
   * Get locations of the block.
   */
  def getLocations(blockId: String): Seq[String] = {
    val startTimeMs = System.currentTimeMillis
    var managers = master.mustGetLocations(GetLocations(blockId))
    val locations = managers.map(_.ip)
    logDebug("Get block locations in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * Get locations of an array of blocks.
   */
  def getLocations(blockIds: Array[String]): Array[Seq[String]] = {
    val startTimeMs = System.currentTimeMillis
    val locations = master.mustGetLocationsMultipleBlockIds(
      GetLocationsMultipleBlockIds(blockIds)).map(_.map(_.ip).toSeq).toArray
    logDebug("Get multiple block location in " + Utils.getUsedTimeMs(startTimeMs))
    return locations
  }

  /**
   * Get block from local block manager.
   */
  def getLocal(blockId: String): Option[Iterator[Any]] = {
    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    logDebug("Getting local block " + blockId)
    locker.getLock(blockId).synchronized {
    
      // Check storage level of block 
      val level = getLevel(blockId)
      if (level != null) {
        logDebug("Level for block " + blockId + " is " + level + " on local machine")
        
        // Look for the block in memory
        if (level.useMemory) {
          logDebug("Getting block " + blockId + " from memory")
          memoryStore.getValues(blockId) match {
            case Some(iterator) => {
              logDebug("Block " + blockId + " found in memory")
              return Some(iterator)
            }
            case None => {
              logDebug("Block " + blockId + " not found in memory")
            }
          }
        } else {
          logDebug("Not getting block " + blockId + " from memory")
        }

        // Look for block in disk 
        if (level.useDisk) {
          logDebug("Getting block " + blockId + " from disk")
          diskStore.getValues(blockId) match {
            case Some(iterator) => {
              logDebug("Block " + blockId + " found in disk")
              return Some(iterator)
            }
            case None => {
              throw new Exception("Block " + blockId + " not found in disk")
              return None
            }
          }
        } else {
          logDebug("Not getting block " + blockId + " from disk")
        }

      } else {
        logDebug("Level for block " + blockId + " not found")
      }
    } 
    return None 
  }

  /**
   * Get block from remote block managers.
   */
  def getRemote(blockId: String): Option[Iterator[Any]] = {
    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    logDebug("Getting remote block " + blockId)
    // Get locations of block
    val locations = master.mustGetLocations(GetLocations(blockId))

    // Get block from remote locations
    for (loc <- locations) {
      logDebug("Getting remote block " + blockId + " from " + loc)
      val data = BlockManagerWorker.syncGetBlock(
          GetBlock(blockId), ConnectionManagerId(loc.ip, loc.port))
      if (data != null) {
        logDebug("Data is not null: " + data)
        return Some(dataDeserialize(data))
      }
      logDebug("Data is null")
    }
    logDebug("Data not found")
    return None
  }

  /**
   * Get a block from the block manager (either local or remote).
   */
  def get(blockId: String): Option[Iterator[Any]] = {
    getLocal(blockId).orElse(getRemote(blockId))
  }

  /**
   * Get many blocks from local and remote block manager using their BlockManagerIds.
   */
  def get(blocksByAddress: Seq[(BlockManagerId, Seq[String])]): HashMap[String, Option[Iterator[Any]]] = {
    if (blocksByAddress == null) {
      throw new IllegalArgumentException("BlocksByAddress is null")
    }
    logDebug("Getting " + blocksByAddress.map(_._2.size).sum + " blocks")
    var startTime = System.currentTimeMillis
    val blocks = new HashMap[String,Option[Iterator[Any]]]() 
    val localBlockIds = new ArrayBuffer[String]()
    val remoteBlockIds = new ArrayBuffer[String]()
    val remoteBlockIdsPerLocation = new HashMap[BlockManagerId, Seq[String]]()

    // Split local and remote blocks
    for ((address, blockIds) <- blocksByAddress) {
      if (address == blockManagerId) {
        localBlockIds ++= blockIds
      } else {
        remoteBlockIds ++= blockIds
        remoteBlockIdsPerLocation(address) = blockIds
      }
    }
    
    // Start getting remote blocks
    val remoteBlockFutures = remoteBlockIdsPerLocation.toSeq.map { case (bmId, bIds) =>
      val cmId = ConnectionManagerId(bmId.ip, bmId.port)
      val blockMessages = bIds.map(bId => BlockMessage.fromGetBlock(GetBlock(bId)))
      val blockMessageArray = new BlockMessageArray(blockMessages)
      val future = connectionManager.sendMessageReliably(cmId, blockMessageArray.toBufferMessage)
      (cmId, future)
    }
    logDebug("Started remote gets for " + remoteBlockIds.size + " blocks in " + 
      Utils.getUsedTimeMs(startTime) + " ms")

    // Get the local blocks while remote blocks are being fetched
    startTime = System.currentTimeMillis
    localBlockIds.foreach(id => {
      get(id) match {
        case Some(block) => {
          blocks.update(id, Some(block))
          logDebug("Got local block " + id)
        }
        case None => {
          throw new BlockException(id, "Could not get block " + id + " from local machine")
        }
      }
    }) 
    logDebug("Got local blocks in " + Utils.getUsedTimeMs(startTime) + " ms")

    // wait for and gather all the remote blocks
    for ((cmId, future) <- remoteBlockFutures) {
      var count = 0
      val oneBlockId = remoteBlockIdsPerLocation(new BlockManagerId(cmId.host, cmId.port)).head
      future() match {
        case Some(message) => {
          val bufferMessage = message.asInstanceOf[BufferMessage]
          val blockMessageArray = BlockMessageArray.fromBufferMessage(bufferMessage)
          blockMessageArray.foreach(blockMessage => {
            if (blockMessage.getType != BlockMessage.TYPE_GOT_BLOCK) {
              throw new BlockException(oneBlockId, "Unexpected message received from " + cmId)
            }
            val buffer = blockMessage.getData
            val blockId = blockMessage.getId
            val block = dataDeserialize(buffer)
            blocks.update(blockId, Some(block))
            logDebug("Got remote block " + blockId + " in " + Utils.getUsedTimeMs(startTime))
            count += 1
          })
        }
        case None => {
          throw new BlockException(oneBlockId, "Could not get blocks from " + cmId)
        }
      }
      logDebug("Got remote " + count + " blocks from " + cmId.host + " in " +
        Utils.getUsedTimeMs(startTime) + " ms")
    }

    logDebug("Got all blocks in " + Utils.getUsedTimeMs(startTime) + " ms")
    return blocks
  }

  /**
   * Put a new block of values to the block manager.
   */
  def put(blockId: String, values: Iterator[Any], level: StorageLevel, tellMaster: Boolean = true) {
    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    if (values == null) {
      throw new IllegalArgumentException("Values is null")
    }
    if (level == null || !level.isValid) {
      throw new IllegalArgumentException("Storage level is null or invalid")
    }

    val startTimeMs = System.currentTimeMillis 
    var bytes: ByteBuffer = null
    
    locker.getLock(blockId).synchronized {
      logDebug("Put for block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs)
        + " to get into synchronized block")
      
      // Check and warn if block with same id already exists 
      if (getLevel(blockId) != null) {
        logWarning("Block " + blockId + " already exists in local machine")
        return
      }

      if (level.useMemory && level.useDisk) {
        // If saving to both memory and disk, then serialize only once 
        memoryStore.putValues(blockId, values, level) match {
          case Left(newValues) => 
            diskStore.putValues(blockId, newValues, level) match {
              case Right(newBytes) => bytes = newBytes
              case _ => throw new Exception("Unexpected return value")
            }
          case Right(newBytes) =>
            bytes = newBytes
            diskStore.putBytes(blockId, newBytes, level)
        }
      } else if (level.useMemory) {
        // If only save to memory 
        memoryStore.putValues(blockId, values, level) match {
          case Right(newBytes) => bytes = newBytes
          case _ => 
        }
      } else {
        // If only save to disk
        diskStore.putValues(blockId, values, level) match {
          case Right(newBytes) => bytes = newBytes
          case _ => throw new Exception("Unexpected return value")
        }
      }
        
      // Store the storage level
      setLevel(blockId, level, tellMaster)
    }
    logDebug("Put block " + blockId + " locally took " + Utils.getUsedTimeMs(startTimeMs))

    // Replicate block if required 
    if (level.replication > 1) {
      if (bytes == null) {
        bytes = dataSerialize(values) // serialize the block if not already done
      }
      replicate(blockId, bytes, level) 
    }

    // TODO: This code will be removed when CacheTracker is gone.
    if (blockId.startsWith("rdd")) {
      notifyTheCacheTracker(blockId)
    }
    logDebug("Put block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs))
  }


  /**
   * Put a new block of serialized bytes to the block manager.
   */
  def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel, tellMaster: Boolean = true) {
    if (blockId == null) {
      throw new IllegalArgumentException("Block Id is null")
    }
    if (bytes == null) {
      throw new IllegalArgumentException("Bytes is null")
    }
    if (level == null || !level.isValid) {
      throw new IllegalArgumentException("Storage level is null or invalid")
    }
    
    val startTimeMs = System.currentTimeMillis 
    
    // Initiate the replication before storing it locally. This is faster as 
    // data is already serialized and ready for sending
    val replicationFuture = if (level.replication > 1) {
      future {
        replicate(blockId, bytes, level)
      }
    } else {
      null
    }

    locker.getLock(blockId).synchronized {
      logDebug("PutBytes for block " + blockId + " took " + Utils.getUsedTimeMs(startTimeMs)
        + " to get into synchronized block")
      if (getLevel(blockId) != null) {
        logWarning("Block " + blockId + " already exists")
        return
      }

      if (level.useMemory) {
        memoryStore.putBytes(blockId, bytes, level)
      }
      if (level.useDisk) {
        diskStore.putBytes(blockId, bytes, level)
      }

      // Store the storage level
      setLevel(blockId, level, tellMaster)
    }

    // TODO: This code will be removed when CacheTracker is gone.
    if (blockId.startsWith("rdd")) {
      notifyTheCacheTracker(blockId)
    }
   
    // If replication had started, then wait for it to finish
    if (level.replication > 1) {
      if (replicationFuture == null) {
        throw new Exception("Unexpected")
      }
      replicationFuture() 
    }

    val finishTime = System.currentTimeMillis
    if (level.replication > 1) {
      logDebug("PutBytes for block " + blockId + " with replication took " + 
        Utils.getUsedTimeMs(startTimeMs))
    } else {
      logDebug("PutBytes for block " + blockId + " without replication took " + 
        Utils.getUsedTimeMs(startTimeMs))
    }
  }

  /**
   * Replicate block to another node.
   */

  private def replicate(blockId: String, data: ByteBuffer, level: StorageLevel) {
    val tLevel: StorageLevel =
      new StorageLevel(level.useDisk, level.useMemory, level.deserialized, 1)
    var peers = master.mustGetPeers(GetPeers(blockManagerId, level.replication - 1))
    for (peer: BlockManagerId <- peers) {
      val start = System.nanoTime
      logDebug("Try to replicate BlockId " + blockId + " once; The size of the data is "
        + data.array().length + " Bytes. To node: " + peer)
      if (!BlockManagerWorker.syncPutBlock(PutBlock(blockId, data, tLevel),
        new ConnectionManagerId(peer.ip, peer.port))) {
        logError("Failed to call syncPutBlock to " + peer)
      }
      logDebug("Replicated BlockId " + blockId + " once used " +
        (System.nanoTime - start) / 1e6 + " s; The size of the data is " +
        data.array().length + " bytes.")
    }
  }

  // TODO: This code will be removed when CacheTracker is gone.
  private def notifyTheCacheTracker(key: String) {
    val rddInfo = key.split(":")
    val rddId: Int = rddInfo(1).toInt
    val splitIndex: Int = rddInfo(2).toInt
    val host = System.getProperty("spark.hostname", Utils.localHostName)
    cacheTracker.notifyTheCacheTrackerFromBlockManager(spark.AddedToCache(rddId, splitIndex, host))
  }

  /**
   * Read a block consisting of a single object.
   */
  def getSingle(blockId: String): Option[Any] = {
    get(blockId).map(_.next)
  }

  /**
   * Write a block consisting of a single object.
   */
  def putSingle(blockId: String, value: Any, level: StorageLevel, tellMaster: Boolean = true) {
    put(blockId, Iterator(value), level, tellMaster)
  }

  /**
   * Drop block from memory (called when memory store has reached it limit)
   */
  def dropFromMemory(blockId: String) {
    locker.getLock(blockId).synchronized {
      val level = getLevel(blockId)
      if (level == null) {
        logWarning("Block " + blockId + " cannot be removed from memory as it does not exist")
        return
      }
      if (!level.useMemory) {
        logWarning("Block " + blockId + " cannot be removed from memory as it is not in memory")
        return
      }
      memoryStore.remove(blockId)  
      val newLevel = new StorageLevel(level.useDisk, false, level.deserialized, level.replication)
      setLevel(blockId, newLevel)
    }
  }

  def dataSerialize(values: Iterator[Any]): ByteBuffer = {
    /*serializer.newInstance().serializeMany(values)*/
    val byteStream = new FastByteArrayOutputStream(4096)
    serializer.newInstance().serializeStream(byteStream).writeAll(values).close()
    byteStream.trim()
    ByteBuffer.wrap(byteStream.array)
  }

  def dataDeserialize(bytes: ByteBuffer): Iterator[Any] = {
    /*serializer.newInstance().deserializeMany(bytes)*/
    val ser = serializer.newInstance()
    bytes.rewind()
    return ser.deserializeStream(new ByteBufferInputStream(bytes)).toIterator
  }

  private def notifyMaster(heartBeat: HeartBeat) {
    master.mustHeartBeat(heartBeat)
  }

  def stop() {
    connectionManager.stop()
    blockInfo.clear()
    memoryStore.clear()
    diskStore.clear()
    logInfo("BlockManager stopped")
  }
}

object BlockManager {
  def getMaxMemoryFromSystemProperties(): Long = {
    val memoryFraction = System.getProperty("spark.storage.memoryFraction", "0.66").toDouble
    (Runtime.getRuntime.totalMemory * memoryFraction).toLong
  }
}
