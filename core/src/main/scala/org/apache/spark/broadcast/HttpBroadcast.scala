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
import java.io.IOException
import java.net.{URL, URLConnection, URI}
import java.util.concurrent.TimeUnit

import scala.collection.mutable.HashMap
import scala.reflect.ClassTag

import org.apache.spark.{HttpServer, Logging, SecurityManager, SparkConf, SparkEnv}
import org.apache.spark.io.CompressionCodec
import org.apache.spark.storage.{BroadcastBlockId, StorageLevel}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashSet, Utils}

/**
 * A [[org.apache.spark.broadcast.Broadcast]] implementation that uses HTTP server
 * as a broadcast mechanism. The first time a HTTP broadcast variable (sent as part of a
 * task) is deserialized in the executor, the broadcasted data is fetched from the driver
 * (through a HTTP server running at the driver) and stored in the BlockManager of the
 * executor to speed up future accesses.
 */
private[spark] class HttpBroadcast[T: ClassTag](
    @transient var value_ : T, isLocal: Boolean, id: Long, key: Int = 0)
  extends Broadcast[T](id) with Logging with Serializable {

  override protected def getValue() = value_

  private val blockId = BroadcastBlockId(id)

  /*
   * Broadcasted data is also stored in the BlockManager of the driver. The BlockManagerMaster
   * does not need to be told about this block as not only need to know about this data block.
   */
  HttpBroadcast.synchronized {
    SparkEnv.get.blockManager.putSingle(
      blockId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
  }

  if (!isLocal) {
    HttpBroadcast.write(id, value_, key)
  }

  /**
   * Remove all persisted state associated with this HTTP broadcast on the executors.
   */
  override protected def doUnpersist(blocking: Boolean) {
    HttpBroadcast.unpersist(id, removeFromDriver = false, blocking, key)
  }

  /**
   * Remove all persisted state associated with this HTTP broadcast on the executors and driver.
   */
  override protected def doDestroy(blocking: Boolean) {
    HttpBroadcast.unpersist(id, removeFromDriver = true, blocking, key)
  }

  /** Used by the JVM when serializing this object. */
  private def writeObject(out: ObjectOutputStream) {
    assertValid()
    out.defaultWriteObject()
  }

  /** Used by the JVM when deserializing this object. */
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject()
    HttpBroadcast.synchronized {
      SparkEnv.get.blockManager.getSingle(blockId) match {
        case Some(x) => value_ = x.asInstanceOf[T]
        case None => {
          logInfo("Started reading broadcast variable " + id)
          val start = System.nanoTime
          value_ = HttpBroadcast.read[T](id, key)
          /*
           * We cache broadcast data in the BlockManager so that subsequent tasks using it
           * do not need to re-fetch. This data is only used locally and no other node
           * needs to fetch this block, so we don't notify the master.
           */
          SparkEnv.get.blockManager.putSingle(
            blockId, value_, StorageLevel.MEMORY_AND_DISK, tellMaster = false)
          val time = (System.nanoTime - start) / 1e9
          logInfo("Reading broadcast variable " + id + " took " + time + " s")
        }
      }
    }
  }
}

private[broadcast] object HttpBroadcast extends Logging {
  
  val activeHBC = new HashMap[Int, HttpBroadcastContainer]
  
  def initialize(isDriver: Boolean, conf: SparkConf, securityMgr: SecurityManager, key: Int = 0) {
    synchronized {
      if (!activeHBC.contains(key)) {
        val hbc = new HttpBroadcastContainer()
        hbc.initialize(isDriver, conf, securityMgr)
        activeHBC.put(key, hbc)
      }
    }
  }
  
  def stop(key: Int = 0) {
    synchronized {
      if (activeHBC.contains(key) && activeHBC(key) != null) {
        activeHBC(key).stop()
        activeHBC(key) = null
        activeHBC.remove(key)
      } else {
        logWarning("Not found key in HttpBroadcast")
      }
    }
  }
  
  def getFile(id: Long, key: Int = 0): File = {
    if (activeHBC.contains(key) && activeHBC(key) != null) {
      activeHBC(key).getFile(id)
    } else {
      throw new IOException("Not found key in HttpBroadcast")
    }
  }

  private def write(id: Long, value: Any, key: Int = 0) {
    if (activeHBC.contains(key) && activeHBC(key) != null) {
      activeHBC(key).write(id, value)
    } else {
      logWarning("Not found key in HttpBroadcast")
    }
  }

  private def read[T: ClassTag](id: Long, key: Int = 0): T = {
    if (activeHBC.contains(key) && activeHBC(key) != null) {
      activeHBC(key).read[T](id)
    } else {
      throw new IOException("Not found key in HttpBroadcast")
    }
  }

  /**
   * Remove all persisted blocks associated with this HTTP broadcast on the executors.
   * If removeFromDriver is true, also remove these persisted blocks on the driver
   * and delete the associated broadcast file.
   */
  def unpersist(id: Long, removeFromDriver: Boolean, blocking: Boolean, key: Int = 0) = synchronized {
    if (activeHBC.contains(key) && activeHBC(key) != null) {
      activeHBC(key).unpersist(id, removeFromDriver, blocking)
    } else {
      logWarning("Not found key in HttpBroadcast")
    }
  }
}
