package spark.broadcast

import com.ning.compress.lzf.{LZFInputStream, LZFOutputStream}

import java.io._
import java.net._
import java.util.UUID

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import spark._
import spark.storage.StorageLevel

class HttpBroadcast[T](@transient var value_ : T, isLocal: Boolean)
extends Broadcast[T] with Logging with Serializable {
  
  def value = value_

  Broadcast.synchronized { 
    Broadcast.values.putSingle(uuid.toString, value_, StorageLevel.MEMORY_ONLY, false)
  }

  if (!isLocal) { 
    HttpBroadcast.write(uuid, value_)
  }

  // Called by JVM when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(uuid.toString) match {
        case Some(x) => value_ = x.asInstanceOf[T]
        case None => {
          logInfo("Started reading broadcast variable " + uuid)
          val start = System.nanoTime
          value_ = HttpBroadcast.read[T](uuid)
          Broadcast.values.putSingle(uuid.toString, value_, StorageLevel.MEMORY_ONLY, false)
          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + uuid + " took " + time + " s")
        }
      }
    }
  }
}

class HttpBroadcastFactory extends BroadcastFactory {
  def initialize(isMaster: Boolean) = HttpBroadcast.initialize(isMaster)
  def newBroadcast[T](value_ : T, isLocal: Boolean) = new HttpBroadcast[T](value_, isLocal)
  def stop() = HttpBroadcast.stop()
}

private object HttpBroadcast extends Logging {
  private var initialized = false

  private var broadcastDir: File = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536
  private var serverUri: String = null
  private var server: HttpServer = null

  def initialize(isMaster: Boolean) {
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
  
  def stop() {
    if (server != null) {
      server.stop()
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
    val serOut = ser.serializeStream(out)
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
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }
}
