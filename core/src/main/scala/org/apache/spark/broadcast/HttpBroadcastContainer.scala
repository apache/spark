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

import java.io.{File, FileOutputStream, ObjectInputStream, ObjectOutputStream, OutputStream}
import java.io.{BufferedInputStream, BufferedOutputStream}
import java.net.{URL, URLConnection, URI}
import java.util.concurrent.TimeUnit

import scala.reflect.ClassTag

import org.apache.spark.{HttpServer, Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashSet, Utils}



import scala.reflect.ClassTag

import org.apache.spark.{SecurityManager, SparkConf}

private class HttpBroadcastContainer extends Logging with Serializable {

  private var initialized = false
  private var broadcastDir: File = null
  private var compress: Boolean = false
  private var bufferSize: Int = 65536
  private var serverUri: String = null
  private var server: HttpServer = null
  private var securityManager: SecurityManager = null

  private val files = new TimeStampedHashSet[File]
  private val httpReadTimeout = TimeUnit.MILLISECONDS.convert(5, TimeUnit.MINUTES).toInt
  private var compressionCodec: CompressionCodec = null
  private var cleaner: MetadataCleaner = null

  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager) {
    synchronized {
      if (!initialized) {
        bufferSize = conf.getInt("spark.buffer.size", 65536)
        compress = conf.getBoolean("spark.broadcast.compress", true)
        securityManager = securityMgr
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
    val broadcastPort = conf.getInt("spark.broadcast.port", 0)
    server = new HttpServer(broadcastDir, securityManager, broadcastPort, "HTTP broadcast server")
    server.start()
    serverUri = server.uri
    logInfo("Broadcast server started at " + serverUri)
  }

  def getFile(id: Long) = new File(broadcastDir, BroadcastBlockId(id).name)

  def write(id: Long, value: Any) {
    val file = getFile(id)
    val out: OutputStream = {
      if (compress) {
        compressionCodec.compressedOutputStream(new FileOutputStream(file))
      } else {
        new BufferedOutputStream(new FileOutputStream(file), bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serOut = ser.serializeStream(out)
    serOut.writeObject(value)
    serOut.close()
    files += file
  }

  def read[T: ClassTag](id: Long): T = {
    logDebug("broadcast read server: " +  serverUri + " id: broadcast-" + id)
    val url = serverUri + "/" + BroadcastBlockId(id).name

    var uc: URLConnection = null
    if (securityManager.isAuthenticationEnabled()) {
      logDebug("broadcast security enabled")
      val newuri = Utils.constructURIForAuthentication(new URI(url), securityManager)
      uc = newuri.toURL.openConnection()
      uc.setAllowUserInteraction(false)
    } else {
      logDebug("broadcast not using security")
      uc = new URL(url).openConnection()
    }

    val in = {
      uc.setReadTimeout(httpReadTimeout)
      val inputStream = uc.getInputStream
      if (compress) {
        compressionCodec.compressedInputStream(inputStream)
      } else {
        new BufferedInputStream(inputStream, bufferSize)
      }
    }
    val ser = SparkEnv.get.serializer.newInstance()
    val serIn = ser.deserializeStream(in)
    val obj = serIn.readObject[T]()
    serIn.close()
    obj
  }

  /**
   * Remove all persisted blocks associated with this HTTP broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver
   * and delete the associated broadcast file.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean) = synchronized {
    SparkEnv.get.blockManager.master.removeBroadcast(id, removeFromDriver, blocking)
    if (removeFromDriver) {
      val file = getFile(id)
      files.remove(file)
      deleteBroadcastFile(file)
    }
  }

  /**
   * Periodically clean up old broadcasts by removing the associated map entries and
   * deleting the associated files.
   */
  private def cleanup(cleanupTime: Long) {
    val iterator = files.internalMap.entrySet().iterator()
    while(iterator.hasNext) {
      val entry = iterator.next()
      val (file, time) = (entry.getKey, entry.getValue)
      if (time < cleanupTime) {
        iterator.remove()
        deleteBroadcastFile(file)
      }
    }
  }

  private def deleteBroadcastFile(file: File) {
    try {
      if (file.exists) {
        if (file.delete()) {
          logInfo("Deleted broadcast file: %s".format(file))
        } else {
          logWarning("Could not delete broadcast file: %s".format(file))
        }
      }
    } catch {
      case e: Exception =>
        logError("Exception while deleting broadcast file: %s".format(file), e)
    }
  }
}
