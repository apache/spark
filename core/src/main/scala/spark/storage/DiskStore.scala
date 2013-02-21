package spark.storage

import java.nio.ByteBuffer
import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel.MapMode
import java.util.{Random, Date}
import java.text.SimpleDateFormat

import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import scala.collection.mutable.ArrayBuffer

import spark.executor.ExecutorExitCode

import spark.Utils

/**
 * Stores BlockManager blocks on disk.
 */
private class DiskStore(blockManager: BlockManager, rootDirs: String)
  extends BlockStore(blockManager) {

  val MAX_DIR_CREATION_ATTEMPTS: Int = 10
  val subDirsPerLocalDir = System.getProperty("spark.diskStore.subDirectories", "64").toInt

  // Create one local directory for each path mentioned in spark.local.dir; then, inside this
  // directory, create multiple subdirectories that we will hash files into, in order to avoid
  // having really large inodes at the top level.
  val localDirs = createLocalDirs()
  val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  addShutdownHook()

  override def getSize(blockId: String): Long = {
    getFile(blockId).length()
  }

  override def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) {
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
    val objOut = blockManager.serializer.newInstance().serializeStream(fileOut)
    objOut.writeAll(values.iterator)
    objOut.close()
    val length = file.length()
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      blockId, Utils.memoryBytesToString(length), (System.currentTimeMillis - startTime)))

    if (returnValues) {
      // Return a byte buffer for the contents of the file
      val channel = new RandomAccessFile(file, "r").getChannel()
      val buffer = channel.map(MapMode.READ_ONLY, 0, length)
      channel.close()
      PutResult(length, Right(buffer))
    } else {
      PutResult(length, null)
    }
  }

  override def getBytes(blockId: String): Option[ByteBuffer] = {
    val file = getFile(blockId)
    val length = file.length().toInt
    val channel = new RandomAccessFile(file, "r").getChannel()
    val bytes = channel.map(MapMode.READ_ONLY, 0, length)
    channel.close()
    Some(bytes)
  }

  override def getValues(blockId: String): Option[Iterator[Any]] = {
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes))
  }

  override def remove(blockId: String): Boolean = {
    val file = getFile(blockId)
    if (file.exists()) {
      file.delete()
      true
    } else {
      false
    }
  }

  override def contains(blockId: String): Boolean = {
    getFile(blockId).exists()
  }

  private def createFile(blockId: String): File = {
    val file = getFile(blockId)
    if (file.exists()) {
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
        System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_CREATE_DIR)
      }
      logInfo("Created local directory at " + localDir)
      localDir
    })
  }

  private def addShutdownHook() {
    Runtime.getRuntime.addShutdownHook(new Thread("delete Spark local dirs") {
      override def run() {
        logDebug("Shutdown hook called")
        try {
          localDirs.foreach(localDir => Utils.deleteRecursively(localDir))
        } catch {
          case t: Throwable => logError("Exception while deleting local spark dirs", t)
        }
      }
    })
  }
}
