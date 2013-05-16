package spark.storage

import java.io.{File, FileOutputStream, OutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.MapMode
import java.util.{Random, Date}
import java.text.SimpleDateFormat

import scala.collection.mutable.ArrayBuffer

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import spark.Utils
import spark.executor.ExecutorExitCode
import spark.serializer.{Serializer, SerializationStream}
import spark.Logging
import spark.network.netty.ShuffleSender
import spark.network.netty.PathResolver


/**
 * Stores BlockManager blocks on disk.
 */
private class DiskStore(blockManager: BlockManager, rootDirs: String)
  extends BlockStore(blockManager) with Logging {

  class DiskBlockObjectWriter(blockId: String, serializer: Serializer, bufferSize: Int)
    extends BlockObjectWriter(blockId) {

    private val f: File = createFile(blockId /*, allowAppendExisting */)

    // The file channel, used for repositioning / truncating the file.
    private var channel: FileChannel = null
    private var bs: OutputStream = null
    private var objOut: SerializationStream = null
    private var lastValidPosition = 0L

    override def open(): DiskBlockObjectWriter = {
      val fos = new FileOutputStream(f, true)
      channel = fos.getChannel()
      bs = blockManager.wrapForCompression(blockId, new FastBufferedOutputStream(fos))
      objOut = serializer.newInstance().serializeStream(bs)
      this
    }

    override def close() {
      objOut.close()
      bs.close()
      channel = null
      bs = null
      objOut = null
      // Invoke the close callback handler.
      super.close()
    }

    override def isOpen: Boolean = objOut != null

    // Flush the partial writes, and set valid length to be the length of the entire file.
    // Return the number of bytes written for this commit.
    override def commit(): Long = {
      bs.flush()
      val prevPos = lastValidPosition
      lastValidPosition = channel.position()
      lastValidPosition - prevPos
    }

    override def revertPartialWrites() {
      // Discard current writes. We do this by flushing the outstanding writes and
      // truncate the file to the last valid position.
      bs.flush()
      channel.truncate(lastValidPosition)
    }

    override def write(value: Any) {
      objOut.writeObject(value)
    }

    override def size(): Long = lastValidPosition
  }

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  val subDirsPerLocalDir = System.getProperty("spark.diskStore.subDirectories", "64").toInt

  var shuffleSender : Thread = null
  val thisInstance = this
  // Create one local directory for each path mentioned in spark.local.dir; then, inside this
  // directory, create multiple subdirectories that we will hash files into, in order to avoid
  // having really large inodes at the top level.
  val localDirs = createLocalDirs()
  val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  val useNetty = System.getProperty("spark.shuffle.use.netty", "false").toBoolean

  addShutdownHook()

  if(useNetty){
    startShuffleBlockSender()
  }

  def getBlockWriter(blockId: String, serializer: Serializer, bufferSize: Int)
    : BlockObjectWriter = {
    new DiskBlockObjectWriter(blockId, serializer, bufferSize)
  }


  override def getSize(blockId: String): Long = {
    getFile(blockId).length()
  }

  override def putBytes(blockId: String, _bytes: ByteBuffer, level: StorageLevel) {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()
    logDebug("Attempting to put block " + blockId)
    val startTime = System.currentTimeMillis
    val file = createFile(blockId)
    val channel = new RandomAccessFile(file, "rw").getChannel()
    while (bytes.remaining > 0) {
      channel.write(bytes)
    }
    channel.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      blockId, Utils.memoryBytesToString(bytes.limit), (finishTime - startTime)))
  }

  private def getFileBytes(file: File): ByteBuffer = {
    val length = file.length()
    val channel = new RandomAccessFile(file, "r").getChannel()
    val buffer = try {
      channel.map(MapMode.READ_ONLY, 0, length)
    } finally {
      channel.close()
    }

    buffer
  }

  override def putValues(
      blockId: String,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean)
    : PutResult = {

    logDebug("Attempting to write values for block " + blockId)
    val startTime = System.currentTimeMillis
    val file = createFile(blockId)
    val fileOut = blockManager.wrapForCompression(blockId,
      new FastBufferedOutputStream(new FileOutputStream(file)))
    val objOut = blockManager.defaultSerializer.newInstance().serializeStream(fileOut)
    objOut.writeAll(values.iterator)
    objOut.close()
    val length = file.length()

    val timeTaken = System.currentTimeMillis - startTime
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      blockId, Utils.memoryBytesToString(length), timeTaken))

    if (returnValues) {
      // Return a byte buffer for the contents of the file
      val buffer = getFileBytes(file)
      PutResult(length, Right(buffer))
    } else {
      PutResult(length, null)
    }
  }

  override def getBytes(blockId: String): Option[ByteBuffer] = {
    val file = getFile(blockId)
    val bytes = getFileBytes(file)
    Some(bytes)
  }

  override def getValues(blockId: String): Option[Iterator[Any]] = {
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes))
  }

  /**
   * A version of getValues that allows a custom serializer. This is used as part of the
   * shuffle short-circuit code.
   */
  def getValues(blockId: String, serializer: Serializer): Option[Iterator[Any]] = {
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes, serializer))
  }

  override def remove(blockId: String): Boolean = {
    val file = getFile(blockId)
    if (file.exists()) {
      file.delete()
    } else {
      false
    }
  }

  override def contains(blockId: String): Boolean = {
    getFile(blockId).exists()
  }

  private def createFile(blockId: String, allowAppendExisting: Boolean = false): File = {
    val file = getFile(blockId)
    if (!allowAppendExisting && file.exists()) {
      throw new Exception("File for block " + blockId + " already exists on disk: " + file)
    }
    file
  }

  private def getFile(blockId: String): File = {
    logDebug("Getting file for block " + blockId)

    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = math.abs(blockId.hashCode)
    val dirId = hash % localDirs.length
    val subDirId = (hash / localDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    var subDir = subDirs(dirId)(subDirId)
    if (subDir == null) {
      subDir = subDirs(dirId).synchronized {
        val old = subDirs(dirId)(subDirId)
        if (old != null) {
          old
        } else {
          val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
          newDir.mkdir()
          subDirs(dirId)(subDirId) = newDir
          newDir
        }
      }
    }

    new File(subDir, blockId)
  }

  private def createLocalDirs(): Array[File] = {
    logDebug("Creating local directories at root dirs '" + rootDirs + "'")
    val dateFormat = new SimpleDateFormat("yyyyMMddHHmmss")
    rootDirs.split(",").map(rootDir => {
      var foundLocalDir: Boolean = false
      var localDir: File = null
      var localDirId: String = null
      var tries = 0
      val rand = new Random()
      while (!foundLocalDir && tries < MAX_DIR_CREATION_ATTEMPTS) {
        tries += 1
        try {
          localDirId = "%s-%04x".format(dateFormat.format(new Date), rand.nextInt(65536))
          localDir = new File(rootDir, "spark-local-" + localDirId)
          if (!localDir.exists) {
            foundLocalDir = localDir.mkdirs()
          }
        } catch {
          case e: Exception =>
            logWarning("Attempt " + tries + " to create local dir failed", e)
        }
      }
      if (!foundLocalDir) {
        logError("Failed " + MAX_DIR_CREATION_ATTEMPTS +
          " attempts to create local dir in " + rootDir)
        System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
      }
      logInfo("Created local directory at " + localDir)
      localDir
    })
  }

  private def addShutdownHook() {
    localDirs.foreach(localDir => Utils.registerShutdownDeleteDir(localDir))
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dirs") {
      override def run() {
        logDebug("Shutdown hook called")
        try {
          localDirs.foreach { localDir =>
            if (!Utils.hasRootAsShutdownDeleteDir(localDir)) Utils.deleteRecursively(localDir)
          }
          if (useNetty && shuffleSender != null)
            shuffleSender.stop
        } catch {
          case t: Throwable => logError("Exception while deleting local spark dirs", t)
        }
      }
    })
  }

  private def startShuffleBlockSender() {
    try {
      val port = System.getProperty("spark.shuffle.sender.port", "6653").toInt

      val pResolver = new PathResolver {
        override def getAbsolutePath(blockId: String): String = {
          if (!blockId.startsWith("shuffle_")) {
            return null
          }
          thisInstance.getFile(blockId).getAbsolutePath()
        }
      }
      shuffleSender = new Thread {
        override def run() = {
          val sender = new ShuffleSender(port, pResolver)
          logInfo("Created ShuffleSender binding to port : "+ port)
          sender.start
        }
      }
      shuffleSender.setDaemon(true)
      shuffleSender.start

    } catch {
      case interrupted: InterruptedException =>
        logInfo("Runner thread for ShuffleBlockSender interrupted")

      case e: Exception => {
        logError("Error running ShuffleBlockSender ", e)
        if (shuffleSender != null) {
          shuffleSender.stop
          shuffleSender = null
        }
      }
    }
  }
}
