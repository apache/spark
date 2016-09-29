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

import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

import scala.reflect.ClassTag

import org.apache.hadoop.fs.Path

import org.apache.spark.{SecurityManager, SparkConf}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging
import org.apache.spark.util.ShutdownHookManager

private[spark] class BroadcastManager(
    val isDriver: Boolean,
    conf: SparkConf,
    securityManager: SecurityManager)
  extends Logging {

  private var initialized = false
  private var broadcastFactory: BroadcastFactory = null
  private val shutdownHook = addShutdownHook()
  private[spark] lazy val hdfsBackupDir =
    Option(new Path(conf.get("spark.broadcast.backup.dir", s"/tmp/spark/${conf.getAppId}_blocks")))

  initialize()

  // Called by SparkContext or Executor before using Broadcast
  private def initialize() {
    synchronized {
      if (!initialized) {
        broadcastFactory = new TorrentBroadcastFactory
        broadcastFactory.initialize(isDriver, conf, securityManager)
        initialized = true
      }
    }
  }

  def stop() {
    // Remove the shutdown hook.  It causes memory leaks if we leave it around.
    try {
      ShutdownHookManager.removeShutdownHook(shutdownHook)
    } catch {
      case e: Exception =>
        logError(s"Exception while removing shutdown hook.", e)
    }
    // only delete the path from driver when the app stop.
    if (isDriver) {
      hdfsBackupDir.foreach { dirPath =>
        try {
          val fs = dirPath.getFileSystem(SparkHadoopUtil.get.conf)
          fs.delete(dirPath, true)
        } catch {
          case e: IOException =>
            logWarning(s"Failed to delete broadcast temp dir $dirPath.", e)
        }
      }
    }

    broadcastFactory.stop()
  }

  private val nextBroadcastId = new AtomicLong(0)

  // Called from driver to create new broadcast id
  def newBroadcastId: Long = nextBroadcastId.getAndIncrement()

  def newBroadcast[T: ClassTag](value_ : T, isLocal: Boolean): Broadcast[T] = {
    broadcastFactory.newBroadcast[T](value_, isLocal, nextBroadcastId.getAndIncrement())
  }

  // Called from executor to upload broadcast data to blockmanager.
  def uploadBroadcast[T: ClassTag](
      value_ : T,
      id: Long
     ): Int = {
    broadcastFactory.uploadBroadcast[T](value_, id)
  }

  // Called from driver to create broadcast with specified id
  def newExecutorBroadcast[T: ClassTag](
      value_ : T,
      id: Long,
      nBlocks: Int): Broadcast[T] = {
    broadcastFactory.newExecutorBroadcast[T](value_, id, nBlocks)
  }

  def unbroadcast(id: Long, removeFromDriver: Boolean, blocking: Boolean) {
    broadcastFactory.unbroadcast(id, removeFromDriver, blocking)
  }

  private def addShutdownHook(): AnyRef = {
    logDebug("Adding shutdown hook") // force eager creation of logger
    ShutdownHookManager.addShutdownHook(ShutdownHookManager.TEMP_DIR_SHUTDOWN_PRIORITY) { () =>
      logInfo("Shutdown hook called")
      BroadcastManager.this.stop()
    }
  }

}

/**
 * Marker trait to identify the shape in which tuples are broadcasted. This is used for
 * executor-side broadcast, typical examples of this are identity (tuples remain unchanged)
 * or hashed (tuples are converted into some hash index).
 */
trait TransFunc[T, U] extends Serializable {
  def transform(rows: Array[T]): U
}
