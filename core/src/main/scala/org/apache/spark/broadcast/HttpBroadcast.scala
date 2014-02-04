/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.broadcast

import java.io.{File, FileOutputStream, ObjectInputStream, OutputStream}
import java.net.URL
import java.util.concurrent.TimeUnit

import it.unimi.dsi.fastutil.io.FastBufferedInputStream
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream

import org.apache.spark.{SparkConf, HttpServer, Logging, SparkEnv}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashSet, Utils}

private[spark] class HttpBroadcast[T](@transient var value_ : T, isLocal: Boolean, id: Long)
  extends Broadcast[T](id) with Logging with Serializable {

  def value = value_

  def blockId = BroadcastBlockId(id)

  HttpBroadcast.synchronized {
    SparkEnv.get.blockManager.putSingle(blockId, value_, StorageLevel.MEMORY_AND_DISK, false)
  }

  if (!isLocal) {
    HttpBroadcast.write(id, value_)
  }

  // Called by JVM when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(blockId) match {
        case Some(x) => value_ = x.asInstanceOf[T]
        case None => {
          logInfo("Started reading broadcast variable " + id)
          val start = System.nanoTime
          value_ = HttpBroadcast.read[T](id)
          SparkEnv.get.blockManager.putSingle(blockId, value_, StorageLevel.MEMORY_AND_DISK, false)
          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
        }
      }
    }
  }
}

/**
 * A [[BroadcastFactory]] implementation that uses a HTTP server as the broadcast medium.
 */
class HttpBroadcastFactory extends BroadcastFactory {
  def initialize(isDriver: Boolean, conf: SparkConf) { HttpBroadcast.initialize(isDriver, conf) }

  def newBroadcast[T](value_ : T, isLocal: Boolean, id: Long) =
    new HttpBroadcast[T](value_, isLocal, id)

  def stop() { HttpBroadcast.stop() }
}

private object HttpBroadcast extends Logging {
  private var initialized = false

  private var broadcastDir: File = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536
  private var serverUri: String = null
  private var server: HttpServer = null

  // TODO: This shouldn't be a global variable so that multiple SparkContexts can coexist
  private val files = new TimeStampedHashSet[String]
  private var cleaner: MetadataCleaner = null

  private val httpReadTimeout = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES).toInt

  private var compressionCodec: CompressionCodec = null

  def initialize(isDriver: Boolean, conf: SparkConf) {
    synchronized {
      if (!initialized) {
        bufferSize = conf.getInt("spark.buffer.size", 65536)
        compress = conf.getBoolean("spark.broadcast.compress", true)
        if (isDriver) {
          createServer(conf)
          conf.set("spark.httpBroadcast.uri",  serverUri)
        }
        serverUri = conf.get("spark.httpBroadcast.uri")
        cleaner = new MetadataCleaner(MetadataCleanerType.HTTP_BROADCAST, cleanup, conf)
        compressionCodec = CompressionCodec.createCodec(conf)
        initialized = true
      }
    }
  }

  def stop() {
    synchronized {
      if (server != null) {
        server.stop()
        server = null
      }
      if (cleaner != null) {
        cleaner.cancel()
        cleaner = null
      }
      compressionCodec = null
      initialized = false
    }
  }

  private def createServer(conf: SparkConf) {
    broadcastDir = Utils.createTempDir(Utils.getLocalDir(conf))
    server = new HttpServer(broadcastDir)
    server.start()
    serverUri = server.uri
    logInfo("Broadcast server started at " + serverUri)
  }

  def write(id: Long, value: Any) {
    val file = new File(broadcastDir, BroadcastBlockId(id).name)
    val out: OutputStream = {
      if (compress) {
        compressionCodec.compressedOutputStream(new FileOutputStream(file))
      } else {
        new FastBufferedOutputStream(new FileOutputStream(file), bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serOut = ser.serializeStream(out)
    serOut.writeObject(value)
    serOut.close()
    files += file.getAbsolutePath
  }

  def read[T](id: Long): T = {
    val url = serverUri + "/" + BroadcastBlockId(id).name
    val in = {
      val httpConnection = new URL(url).openConnection()
      httpConnection.setReadTimeout(httpReadTimeout)
      val inputStream = httpConnection.getInputStream
      if (compress) {
        compressionCodec.compressedInputStream(inputStream)
      } else {
        new FastBufferedInputStream(inputStream, bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  def cleanup(cleanupTime: Long) {
    val iterator = files.internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      val (file, time) = (entry.getKey, entry.getValue)
      if (time < cleanupTime) {
        try {
          iterator.remove()
          new File(file.toString).delete()
          logInfo("Deleted broadcast file '" + file + "'")
        } catch {
          case e: Exception => logWarning("Could not delete broadcast file '" + file + "'", e)
        }
      }
    }
  }
}
