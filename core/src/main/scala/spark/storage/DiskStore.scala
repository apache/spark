package spark.storage

import java.nio.ByteBuffer
import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel.MapMode
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream
import java.util.UUID
import spark.Utils

/**
 * Stores BlockManager blocks on disk.
 */
class DiskStore(blockManager: BlockManager, rootDirs: String)
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
    getFile(blockId).length
  }

  override def putBytes(blockId: String, bytes: ByteBuffer, level: StorageLevel) {
    logDebug("Attempting to put block " + blockId)
    val startTime = System.currentTimeMillis
    val file = createFile(blockId)
    val channel = new RandomAccessFile(file, "rw").getChannel()
    val buffer = channel.map(MapMode.READ_WRITE, 0, bytes.limit)
    buffer.put(bytes)
    channel.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored to file of %d bytes to disk in %d ms".format(
      blockId, bytes.limit, (finishTime - startTime)))
  }

  override def putValues(blockId: String, values: Iterator[Any], level: StorageLevel)
    : Either[Iterator[Any], ByteBuffer] = {

    logDebug("Attempting to write values for block " + blockId)
    val file = createFile(blockId)
    val fileOut = blockManager.wrapForCompression(
      new FastBufferedOutputStream(new FileOutputStream(file)))
    val objOut = blockManager.serializer.newInstance().serializeStream(fileOut)
    objOut.writeAll(values)
    objOut.close()

    // Return a byte buffer for the contents of the file
    val channel = new RandomAccessFile(file, "rw").getChannel()
    Right(channel.map(MapMode.READ_WRITE, 0, channel.size()))
  }

  override def getBytes(blockId: String): Option[ByteBuffer] = {
    val file = getFile(blockId)
    val length = file.length().toInt
    val channel = new RandomAccessFile(file, "r").getChannel()
    Some(channel.map(MapMode.READ_WRITE, 0, length))
  }

  override def getValues(blockId: String): Option[Iterator[Any]] = {
    val file = getFile(blockId)
    val length = file.length().toInt
    val channel = new RandomAccessFile(file, "r").getChannel()
    val bytes = channel.map(MapMode.READ_ONLY, 0, length)
    val buffer = dataDeserialize(bytes)
    channel.close()
    Some(buffer)
  }

  override def remove(blockId: String) {
    throw new UnsupportedOperationException("Not implemented")
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
    rootDirs.split(",").map(rootDir => {
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
      logInfo("Created local directory at " + localDir)
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
