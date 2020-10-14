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

package org.apache.spark.deploy.history

import java.io.File
import java.util.concurrent.atomic.AtomicLong

import scala.collection.JavaConverters._
import scala.collection.mutable.{HashMap, ListBuffer}

import org.apache.commons.io.FileUtils

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.History._
import org.apache.spark.status.KVUtils._
import org.apache.spark.util.{Clock, Utils}
import org.apache.spark.util.kvstore.KVStore

/**
 * A class used to keep track of disk usage by the SHS, allowing application data to be deleted
 * from disk when usage exceeds a configurable threshold.
 *
 * The goal of the class is not to guarantee that usage will never exceed the threshold; because of
 * how application data is written, disk usage may temporarily go higher. But, eventually, it
 * should fall back under the threshold.
 *
 * @param conf Spark configuration.
 * @param path Path where to store application data.
 * @param listing The listing store, used to persist usage data.
 * @param clock Clock instance to use.
 */
private class HistoryServerDiskManager(
    conf: SparkConf,
    path: File,
    listing: KVStore,
    clock: Clock) extends Logging {

  private val appStoreDir = new File(path, "apps")
  if (!appStoreDir.isDirectory() && !appStoreDir.mkdir()) {
    throw new IllegalArgumentException(s"Failed to create app directory ($appStoreDir).")
  }

  private val tmpStoreDir = new File(path, "temp")
  if (!tmpStoreDir.isDirectory() && !tmpStoreDir.mkdir()) {
    throw new IllegalArgumentException(s"Failed to create temp directory ($tmpStoreDir).")
  }

  private val maxUsage = conf.get(MAX_LOCAL_DISK_USAGE)
  private val currentUsage = new AtomicLong(0L)
  private val committedUsage = new AtomicLong(0L)
  private val active = new HashMap[(String, Option[String]), Long]()

  def initialize(): Unit = {
    updateUsage(sizeOf(appStoreDir), committed = true)

    // Clean up any temporary stores during start up. This assumes that they're leftover from other
    // instances and are not useful.
    tmpStoreDir.listFiles().foreach(FileUtils.deleteQuietly)

    // Go through the recorded store directories and remove any that may have been removed by
    // external code.
    val (existences, orphans) = listing
      .view(classOf[ApplicationStoreInfo])
      .asScala
      .toSeq
      .partition { info =>
        new File(info.path).exists()
      }

    orphans.foreach { info =>
      listing.delete(info.getClass(), info.path)
    }

    // Reading level db would trigger table file compaction, then it may cause size of level db
    // directory changed. When service restarts, "currentUsage" is calculated from real directory
    // size. Update "ApplicationStoreInfo.size" to ensure "currentUsage" equals
    // sum of "ApplicationStoreInfo.size".
    existences.foreach { info =>
      val fileSize = sizeOf(new File(info.path))
      if (fileSize != info.size) {
        listing.write(info.copy(size = fileSize))
      }
    }

    logInfo("Initialized disk manager: " +
      s"current usage = ${Utils.bytesToString(currentUsage.get())}, " +
      s"max usage = ${Utils.bytesToString(maxUsage)}")
  }

  /**
   * Lease some space from the store. The leased space is calculated as a fraction of the given
   * event log size; this is an approximation, and doesn't mean the application store cannot
   * outgrow the lease.
   *
   * If there's not enough space for the lease, other applications might be evicted to make room.
   * This method always returns a lease, meaning that it's possible for local disk usage to grow
   * past the configured threshold if there aren't enough idle applications to evict.
   *
   * While the lease is active, the data is written to a temporary location, so `openStore()`
   * will still return `None` for the application.
   */
  def lease(eventLogSize: Long, isCompressed: Boolean = false): Lease = {
    val needed = approximateSize(eventLogSize, isCompressed)
    makeRoom(needed)

    val tmp = Utils.createTempDir(tmpStoreDir.getPath(), "appstore")
    Utils.chmod700(tmp)

    updateUsage(needed)
    val current = currentUsage.get()
    if (current > maxUsage) {
      logInfo(s"Lease of ${Utils.bytesToString(needed)} may cause usage to exceed max " +
        s"(${Utils.bytesToString(current)} > ${Utils.bytesToString(maxUsage)})")
    }

    new Lease(tmp, needed)
  }

  /**
   * Returns the location of an application store if it's still available. Marks the store as
   * being used so that it's not evicted when running out of designated space.
   */
  def openStore(appId: String, attemptId: Option[String]): Option[File] = {
    var newSize: Long = 0
    val storePath = active.synchronized {
      val path = appStorePath(appId, attemptId)
      if (path.isDirectory()) {
        newSize = sizeOf(path)
        active(appId -> attemptId) = newSize
        Some(path)
      } else {
        None
      }
    }

    storePath.foreach { path =>
      updateApplicationStoreInfo(appId, attemptId, newSize)
    }

    storePath
  }

  /**
   * Tell the disk manager that the store for the given application is not being used anymore.
   *
   * @param delete Whether to delete the store from disk.
   */
  def release(appId: String, attemptId: Option[String], delete: Boolean = false): Unit = {
    // Because LevelDB may modify the structure of the store files even when just reading, update
    // the accounting for this application when it's closed.
    val oldSizeOpt = active.synchronized {
      active.remove(appId -> attemptId)
    }

    oldSizeOpt.foreach { oldSize =>
      val path = appStorePath(appId, attemptId)
      updateUsage(-oldSize, committed = true)
      if (path.isDirectory()) {
        if (delete) {
          deleteStore(path)
        } else {
          val newSize = sizeOf(path)
          val newInfo = listing.read(classOf[ApplicationStoreInfo], path.getAbsolutePath())
            .copy(size = newSize)
          listing.write(newInfo)
          updateUsage(newSize, committed = true)
        }
      }
    }
  }

  /**
   * A non-scientific approximation of how large an app state store will be given the size of the
   * event log.
   */
  def approximateSize(eventLogSize: Long, isCompressed: Boolean): Long = {
    if (isCompressed) {
      // For compressed logs, assume that compression reduces the log size a lot, and the disk
      // store will actually grow compared to the log size.
      eventLogSize * 2
    } else {
      // For non-compressed logs, assume the disk store will end up at approximately 50% of the
      // size of the logs. This is loosely based on empirical evidence.
      eventLogSize / 2
    }
  }

  /** Current free space. Considers space currently leased out too. */
  def free(): Long = {
    math.max(maxUsage - currentUsage.get(), 0L)
  }

  /** Current committed space. */
  def committed(): Long = committedUsage.get()

  private def deleteStore(path: File): Unit = {
    FileUtils.deleteDirectory(path)
    listing.delete(classOf[ApplicationStoreInfo], path.getAbsolutePath())
  }

  private def makeRoom(size: Long): Unit = {
    if (free() < size) {
      logDebug(s"Not enough free space, looking at candidates for deletion...")
      val evicted = new ListBuffer[ApplicationStoreInfo]()
      Utils.tryWithResource(
        listing.view(classOf[ApplicationStoreInfo]).index("lastAccess").closeableIterator()
      ) { iter =>
        var needed = size
        while (needed > 0 && iter.hasNext()) {
          val info = iter.next()
          val isActive = active.synchronized {
            active.contains(info.appId -> info.attemptId)
          }
          if (!isActive) {
            evicted += info
            needed -= info.size
          }
        }
      }

      if (evicted.nonEmpty) {
        val freed = evicted.map { info =>
          logInfo(s"Deleting store for ${info.appId}/${info.attemptId}.")
          deleteStore(new File(info.path))
          updateUsage(-info.size, committed = true)
          info.size
        }.sum

        logInfo(s"Deleted ${evicted.size} store(s) to free ${Utils.bytesToString(freed)} " +
          s"(target = ${Utils.bytesToString(size)}).")
      } else {
        logWarning(s"Unable to free any space to make room for ${Utils.bytesToString(size)}.")
      }
    }
  }

  private[history] def appStorePath(appId: String, attemptId: Option[String]): File = {
    val fileName = appId + attemptId.map("_" + _).getOrElse("") + ".ldb"
    new File(appStoreDir, fileName)
  }

  private def updateApplicationStoreInfo(
      appId: String, attemptId: Option[String], newSize: Long): Unit = {
    val path = appStorePath(appId, attemptId)
    val info = ApplicationStoreInfo(path.getAbsolutePath(), clock.getTimeMillis(), appId,
      attemptId, newSize)
    listing.write(info)
  }

  private def updateUsage(delta: Long, committed: Boolean = false): Unit = {
    val updated = currentUsage.addAndGet(delta)
    if (updated < 0) {
      throw new IllegalStateException(
        s"Disk usage tracker went negative (now = $updated, delta = $delta)")
    }
    if (committed) {
      val updatedCommitted = committedUsage.addAndGet(delta)
      if (updatedCommitted < 0) {
        throw new IllegalStateException(
          s"Disk usage tracker went negative (now = $updatedCommitted, delta = $delta)")
      }
    }
  }

  /** Visible for testing. Return the size of a directory. */
  private[history] def sizeOf(path: File): Long = FileUtils.sizeOf(path)

  private[history] class Lease(val tmpPath: File, private val leased: Long) {

    /**
     * Commits a lease to its final location, and update accounting information. This method
     * marks the application as active, so its store is not available for eviction.
     */
    def commit(appId: String, attemptId: Option[String]): File = {
      val dst = appStorePath(appId, attemptId)

      active.synchronized {
        require(!active.contains(appId -> attemptId),
          s"Cannot commit lease for active application $appId / $attemptId")

        if (dst.isDirectory()) {
          val size = sizeOf(dst)
          deleteStore(dst)
          updateUsage(-size, committed = true)
        }
      }

      updateUsage(-leased)

      val newSize = sizeOf(tmpPath)
      makeRoom(newSize)
      tmpPath.renameTo(dst)

      updateUsage(newSize, committed = true)
      if (committedUsage.get() > maxUsage) {
        val current = Utils.bytesToString(committedUsage.get())
        val max = Utils.bytesToString(maxUsage)
        logWarning(s"Commit of application $appId / $attemptId causes maximum disk usage to be " +
          s"exceeded ($current > $max)")
      }

      updateApplicationStoreInfo(appId, attemptId, newSize)

      active.synchronized {
        active(appId -> attemptId) = newSize
      }
      dst
    }

    /** Deletes the temporary directory created for the lease. */
    def rollback(): Unit = {
      updateUsage(-leased)
      FileUtils.deleteDirectory(tmpPath)
    }

  }

}

private case class ApplicationStoreInfo(
    @KVIndexParam path: String,
    @KVIndexParam("lastAccess") lastAccess: Long,
    appId: String,
    attemptId: Option[String],
    size: Long)
