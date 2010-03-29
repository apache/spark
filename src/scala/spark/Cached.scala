package spark

import java.io._
import java.net.URI
import java.util.UUID

import com.google.common.collect.MapMaker

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, RawLocalFileSystem}

import spark.compress.lzf.{LZFInputStream, LZFOutputStream}

@serializable class Cached[T](@transient var value_ : T, local: Boolean) {
  val uuid = UUID.randomUUID()
  def value = value_

  Cache.synchronized { Cache.values.put(uuid, value_) }

  if (!local) writeCacheFile()

  private def writeCacheFile() {
    val out = new ObjectOutputStream(Cache.openFileForWriting(uuid))
    out.writeObject(value_)
    out.close()
  }

  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    Cache.synchronized {
      val cachedVal = Cache.values.get(uuid)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        val start = System.nanoTime
        val fileIn = new ObjectInputStream(Cache.openFileForReading(uuid))
        value_ = fileIn.readObject().asInstanceOf[T]
        Cache.values.put(uuid, value_)
        fileIn.close()
        val time = (System.nanoTime - start) / 1e9
        println("Reading cached variable " + uuid + " took " + time + " s")
      }
    }
  }
  
  override def toString = "spark.Cached(" + uuid + ")"
}

private object Cache {
  val values = new MapMaker().softValues().makeMap[UUID, Any]()

  private var initialized = false
  private var fileSystem: FileSystem = null
  private var workDir: String = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536

  // Will be called by SparkContext or Executor before using cache
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
        workDir = System.getProperty("spark.dfs.workdir", "/tmp")
        compress = System.getProperty("spark.compress", "false").toBoolean
        initialized = true
      }
    }
  }

  private def getPath(uuid: UUID) = new Path(workDir + "/cache-" + uuid)

  def openFileForReading(uuid: UUID): InputStream = {
    val fileStream = if (fileSystem != null) {
      fileSystem.open(getPath(uuid))
    } else {
      // Local filesystem
      new FileInputStream(getPath(uuid).toString)
    }
    if (compress)
      new LZFInputStream(fileStream) // LZF stream does its own buffering
    else if (fileSystem == null)
      new BufferedInputStream(fileStream, bufferSize)
    else
      fileStream // Hadoop streams do their own buffering
  }

  def openFileForWriting(uuid: UUID): OutputStream = {
    val fileStream = if (fileSystem != null) {
      fileSystem.create(getPath(uuid))
    } else {
      // Local filesystem
      new FileOutputStream(getPath(uuid).toString)
    }
    if (compress)
      new LZFOutputStream(fileStream) // LZF stream does its own buffering
    else if (fileSystem == null)
      new BufferedOutputStream(fileStream, bufferSize)
    else
      fileStream // Hadoop streams do their own buffering
  }
}
