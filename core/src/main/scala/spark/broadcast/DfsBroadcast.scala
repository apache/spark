package spark.broadcast

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

import java.io._
import java.net._
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import spark._

class DfsBroadcast[T](@transient var value_ : T, isLocal: Boolean)
extends Broadcast[T] with Logging with Serializable {
  
  def value = value_

  DfsBroadcast.synchronized { 
    DfsBroadcast.values.put(uuid, 0, value_) 
  }

  if (!isLocal) { 
    sendBroadcast 
  }

  def sendBroadcast () {
    val out = new ObjectOutputStream (DfsBroadcast.openFileForWriting(uuid))
    out.writeObject (value_)
    out.close()
  }

  // Called by JVM when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    DfsBroadcast.synchronized {
      val cachedVal = DfsBroadcast.values.get(uuid, 0)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        logInfo( "Started reading Broadcasted variable " + uuid)
        val start = System.nanoTime
        
        val fileIn = new ObjectInputStream(DfsBroadcast.openFileForReading(uuid))
        value_ = fileIn.readObject.asInstanceOf[T]
        DfsBroadcast.values.put(uuid, 0, value_)
        fileIn.close()
        
        val time = (System.nanoTime - start) / 1e9
        logInfo( "Reading Broadcasted variable " + uuid + " took " + time + " s")
      }
    }
  }
}

class DfsBroadcastFactory 
extends BroadcastFactory {
  def initialize (isMaster: Boolean) {
    DfsBroadcast.initialize
  }
  def newBroadcast[T] (value_ : T, isLocal: Boolean) = 
    new DfsBroadcast[T] (value_, isLocal)
}

private object DfsBroadcast
extends Logging {
  val values = SparkEnv.get.cache.newKeySpace()

  private var initialized = false

  private var fileSystem: FileSystem = null
  private var workDir: String = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536

  def initialize() {
    synchronized {
      if (!initialized) {
        bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
        val dfs = System.getProperty("spark.dfs", "file:///")
        if (!dfs.startsWith("file://")) {
          val conf = new Configuration()
          conf.setInt("io.file.buffer.size", bufferSize)
          val rep = System.getProperty("spark.dfs.replication", "3").toInt
          conf.setInt("dfs.replication", rep)
          fileSystem = FileSystem.get(new URI(dfs), conf)
        }
        workDir = System.getProperty("spark.dfs.workDir", "/tmp")
        compress = System.getProperty("spark.compress", "false").toBoolean

        initialized = true
      }
    }
  }

  private def getPath(uuid: UUID) = new Path(workDir + "/broadcast-" + uuid)

  def openFileForReading(uuid: UUID): InputStream = {
    val fileStream = if (fileSystem != null) {
      fileSystem.open(getPath(uuid))
    } else {
      // Local filesystem
      new FileInputStream(getPath(uuid).toString)
    }
    
    if (compress) {
      // LZF stream does its own buffering
      new LZFInputStream(fileStream) 
    } else if (fileSystem == null) {
      new BufferedInputStream(fileStream, bufferSize)
    } else { 
      // Hadoop streams do their own buffering
      fileStream 
    }
  }

  def openFileForWriting(uuid: UUID): OutputStream = {
    val fileStream = if (fileSystem != null) {
      fileSystem.create(getPath(uuid))
    } else {
      // Local filesystem
      new FileOutputStream(getPath(uuid).toString)
    }
    
    if (compress) {
      // LZF stream does its own buffering
      new LZFOutputStream(fileStream) 
    } else if (fileSystem == null) {
      new BufferedOutputStream(fileStream, bufferSize)
    } else {
      // Hadoop streams do their own buffering
      fileStream 
    }
  }
}
