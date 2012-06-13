package spark.storage

import spark.{Utils, Logging, Serializer, SizeEstimator}

import scala.collection.mutable.ArrayBuffer

import java.io.{File, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode
import java.util.{UUID, LinkedHashMap}
import java.util.concurrent.Executors

import it.unimi.dsi.fastutil.io._

/**
 * Abstract class to store blocks
 */
abstract class BlockStore(blockManager: BlockManager) extends Logging {
  initLogging()

  def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) 

  def putValues(blockId: String, values: Iterator[Any], level: StorageLevel): Either[Iterator[Any], ByteBuffer]

  def getBytes(blockId: String): Option[ByteBuffer]

  def getValues(blockId: String): Option[Iterator[Any]]

  def remove(blockId: String)

  def dataSerialize(values: Iterator[Any]): ByteBuffer = blockManager.dataSerialize(values)

  def dataDeserialize(bytes: ByteBuffer): Iterator[Any] = blockManager.dataDeserialize(bytes)

  def clear() { }
}

/**
 * Class to store blocks in memory 
 */
class MemoryStore(blockManager: BlockManager, maxMemory: Long) 
  extends BlockStore(blockManager) {

  class Entry(var value: Any, val size: Long, val deserialized: Boolean)
  
  private val memoryStore = new LinkedHashMap[String, Entry](32, 0.75f, true)
  private var currentMemory = 0L
 
  private val blockDropper = Executors.newSingleThreadExecutor() 

  def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) {
    if (level.deserialized) {
      bytes.rewind()
      val values = dataDeserialize(bytes)
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      ensureFreeSpace(sizeEstimate)
      val entry = new Entry(elements, sizeEstimate, true)
      memoryStore.synchronized { memoryStore.put(blockId, entry) }
      currentMemory += sizeEstimate
      logDebug("Block " + blockId + " stored as values to memory")
    } else {
      val entry = new Entry(bytes, bytes.array().length, false)
      ensureFreeSpace(bytes.array.length)
      memoryStore.synchronized { memoryStore.put(blockId, entry) }
      currentMemory += bytes.array().length
      logDebug("Block " + blockId + " stored as " + bytes.array().length + " bytes to memory")
    }
  }

  def putValues(blockId: String, values: Iterator[Any], level: StorageLevel): Either[Iterator[Any], ByteBuffer] = {
    if (level.deserialized) {
      val elements = new ArrayBuffer[Any]
      elements ++= values
      val sizeEstimate = SizeEstimator.estimate(elements.asInstanceOf[AnyRef])
      ensureFreeSpace(sizeEstimate)
      val entry = new Entry(elements, sizeEstimate, true)
      memoryStore.synchronized { memoryStore.put(blockId, entry) }
      currentMemory += sizeEstimate
      logDebug("Block " + blockId + " stored as values to memory")
      return Left(elements.iterator) 
    } else {
      val bytes = dataSerialize(values)
      ensureFreeSpace(bytes.array().length)
      val entry = new Entry(bytes, bytes.array().length, false)
      memoryStore.synchronized { memoryStore.put(blockId, entry) } 
      currentMemory += bytes.array().length
      logDebug("Block " + blockId + " stored as " + bytes.array.length + " bytes to memory")
      return Right(bytes)
    }
  }

  def getBytes(blockId: String): Option[ByteBuffer] = {
    throw new UnsupportedOperationException("Not implemented") 
  }

  def getValues(blockId: String): Option[Iterator[Any]] = {
    val entry = memoryStore.synchronized { memoryStore.get(blockId) }
    if (entry == null) {
      return None 
    }
    if (entry.deserialized) {
      return Some(entry.value.asInstanceOf[ArrayBuffer[Any]].toIterator)
    } else {
      return Some(dataDeserialize(entry.value.asInstanceOf[ByteBuffer])) 
    }
  }

  def remove(blockId: String) {
    memoryStore.synchronized {
      val entry = memoryStore.get(blockId) 
      if (entry != null) {
        memoryStore.remove(blockId)
        currentMemory -= entry.size
        logDebug("Block " + blockId + " of size " + entry.size + " dropped from memory")
      } else {
        logWarning("Block " + blockId + " could not be removed as it doesnt exist")
      }
    }
  }

  override def clear() {
    memoryStore.synchronized {
      memoryStore.clear()
    }
    blockDropper.shutdown()
  }

  private def drop(blockId: String) {
    blockDropper.submit(new Runnable() {
      def run() {
        blockManager.dropFromMemory(blockId)
      }
    })
  }

  private def ensureFreeSpace(space: Long) {
    logInfo("ensureFreeSpace(%d) called with curMem=%d, maxMem=%d".format(
      space, currentMemory, maxMemory))
    
    val droppedBlockIds = new ArrayBuffer[String]()
    var droppedMemory = 0L
    
    memoryStore.synchronized {
      val iter = memoryStore.entrySet().iterator()
      while (maxMemory - (currentMemory - droppedMemory) < space && iter.hasNext) {
        val pair = iter.next()
        val blockId = pair.getKey
        droppedBlockIds += blockId
        droppedMemory += pair.getValue.size
        logDebug("Decided to drop " + blockId)
      }
    }  
    
    for (blockId <- droppedBlockIds) {
      drop(blockId)
    }
    droppedBlockIds.clear()
  }
}


/**
 * Class to store blocks in disk 
 */
class DiskStore(blockManager: BlockManager, rootDirs: String) 
  extends BlockStore(blockManager) {

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  val localDirs = createLocalDirs()
  var lastLocalDirUsed = 0

  addShutdownHook()
  
  def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) {
    logDebug("Attempting to put block " + blockId)
    val startTime = System.currentTimeMillis
    val file = createFile(blockId)
    if (file != null) {
      val channel = new RandomAccessFile(file, "rw").getChannel()
      val buffer = channel.map(MapMode.READ_WRITE, 0, bytes.array.length)
      buffer.put(bytes.array)
      channel.close()
      val finishTime = System.currentTimeMillis
      logDebug("Block " + blockId + " stored to file of " + bytes.array.length + " bytes to disk in " + (finishTime - startTime) + " ms")
    } else {
      logError("File not created for block " + blockId)
    }
  }

  def putValues(blockId: String, values: Iterator[Any], level: StorageLevel): Either[Iterator[Any], ByteBuffer] = {
    val bytes = dataSerialize(values) 
    logDebug("Converted block " + blockId + " to " + bytes.array.length + " bytes")
    putBytes(blockId, bytes, level)
    return Right(bytes)
  }

  def getBytes(blockId: String): Option[ByteBuffer] = {
    val file = getFile(blockId) 
    val length = file.length().toInt
    val channel = new RandomAccessFile(file, "r").getChannel()
    val bytes = ByteBuffer.allocate(length)
    bytes.put(channel.map(MapMode.READ_WRITE, 0, length))
    return Some(bytes)  
  }

  def getValues(blockId: String): Option[Iterator[Any]] = {
    val file = getFile(blockId) 
    val length = file.length().toInt
    val channel = new RandomAccessFile(file, "r").getChannel()
    val bytes = channel.map(MapMode.READ_ONLY, 0, length)
    val buffer = dataDeserialize(bytes)
    channel.close()
    return Some(buffer) 
  }

  def remove(blockId: String) {
    throw new UnsupportedOperationException("Not implemented") 
  }
  
  private def createFile(blockId: String): File = {
    val file = getFile(blockId) 
    if (file == null) {
      lastLocalDirUsed = (lastLocalDirUsed + 1) % localDirs.size
      val newFile = new File(localDirs(lastLocalDirUsed), blockId)
      newFile.getParentFile.mkdirs()
      return newFile 
    } else {
      logError("File for block " + blockId + " already exists on disk, " + file)
      return null
    }
  }

  private def getFile(blockId: String): File = {
    logDebug("Getting file for block " + blockId)
    // Search for the file in all the local directories, only one of them should have the file
    val files = localDirs.map(localDir => new File(localDir, blockId)).filter(_.exists)  
    if (files.size > 1) {
      throw new Exception("Multiple files for same block " + blockId + " exists: " + 
        files.map(_.toString).reduceLeft(_ + ", " + _))
      return null
    } else if (files.size == 0) {
      return null 
    } else {
      logDebug("Got file " + files(0) + " of size " + files(0).length + " bytes")
      return files(0)
    }
  }

  private def createLocalDirs(): Seq[File] = {
    logDebug("Creating local directories at root dirs '" + rootDirs + "'") 
    rootDirs.split("[;,:]").map(rootDir => {
        var foundLocalDir: Boolean = false
        var localDir: File = null
        var localDirUuid: UUID = null
        var tries = 0
        while (!foundLocalDir && tries < MAX_DIR_CREATION_ATTEMPTS) {
          tries += 1
          try {
            localDirUuid = UUID.randomUUID()
            localDir = new File(rootDir, "spark-local-" + localDirUuid)
            if (!localDir.exists) {
              localDir.mkdirs()
              foundLocalDir = true
            }
          } catch {
            case e: Exception =>
            logWarning("Attempt " + tries + " to create local dir failed", e)
          }
        }
        if (!foundLocalDir) {
          logError("Failed " + MAX_DIR_CREATION_ATTEMPTS + 
            " attempts to create local dir in " + rootDir)
          System.exit(1)
        }
        logDebug("Created local directory at " + localDir)
        localDir
    })
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dirs") {
      override def run() {
        logDebug("Shutdown hook called")
        localDirs.foreach(localDir => Utils.deleteRecursively(localDir))
      }
    })
  }
}
