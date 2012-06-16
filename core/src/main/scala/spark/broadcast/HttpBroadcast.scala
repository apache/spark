package spark.broadcast

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

import java.io._
import java.net._
import java.util.UUID

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import spark._

class HttpBroadcast[T](@transient var value_ : T, isLocal: Boolean)
extends Broadcast[T] with Logging with Serializable {
  
  def value = value_

  HttpBroadcast.synchronized { 
    HttpBroadcast.values.put(uuid, 0, value_) 
  }

  if (!isLocal) { 
    HttpBroadcast.write(uuid, value_)
  }

  // Called by JVM when deserializing an object
  private def readObject(in: ObjectInputStream): Unit = {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      val cachedVal = HttpBroadcast.values.get(uuid, 0)
      if (cachedVal != null) {
        value_ = cachedVal.asInstanceOf[T]
      } else {
        logInfo("Started reading broadcast variable " + uuid)
        val start = System.nanoTime
        value_ = HttpBroadcast.read[T](uuid)
        HttpBroadcast.values.put(uuid, 0, value_)
        val time = (System.nanoTime - start) / 1e9
        logInfo("Reading broadcast variable " + uuid + " took " + time + " s")
      }
    }
  }
}

class HttpBroadcastFactory extends BroadcastFactory {
  def initialize(isMaster: Boolean): Unit = HttpBroadcast.initialize(isMaster)
  def newBroadcast[T](value_ : T, isLocal: Boolean) = new HttpBroadcast[T](value_, isLocal)
}

private object HttpBroadcast extends Logging {
  val values = SparkEnv.get.cache.newKeySpace()

  private var initialized = false

  private var broadcastDir: File = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536
  private var serverUri: String = null
  private var server: HttpServer = null

  def initialize(isMaster: Boolean): Unit = {
    synchronized {
      if (!initialized) {
        bufferSize = System.getProperty("spark.buffer.size", "65536").toInt
        compress = System.getProperty("spark.compress", "false").toBoolean
        if (isMaster) {
          createServer()
        }
        serverUri = System.getProperty("spark.httpBroadcast.uri")
        initialized = true
      }
    }
  }

  private def createServer() {
    broadcastDir = Utils.createTempDir()
    server = new HttpServer(broadcastDir)
    server.start()
    serverUri = server.uri
    System.setProperty("spark.httpBroadcast.uri", serverUri)
    logInfo("Broadcast server started at " + serverUri)
  }

  def write(uuid: UUID, value: Any) {
    val file = new File(broadcastDir, "broadcast-" + uuid)
    val out: OutputStream = if (compress) {
      new LZFOutputStream(new FileOutputStream(file)) // Does its own buffering
    } else {
      new FastBufferedOutputStream(new FileOutputStream(file), bufferSize)
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serOut = ser.outputStream(out)
    serOut.writeObject(value)
    serOut.close()
  }

  def read[T](uuid: UUID): T = {
    val url = serverUri + "/broadcast-" + uuid
    var in = if (compress) {
      new LZFInputStream(new URL(url).openStream()) // Does its own buffering
    } else {
      new FastBufferedInputStream(new URL(url).openStream(), bufferSize)
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serIn = ser.inputStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }
}
