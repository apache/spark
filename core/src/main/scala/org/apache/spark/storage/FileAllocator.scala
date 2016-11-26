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

package org.apache.spark.storage

import java.io.{File, IOException}

import org.apache.spark.SparkConf
import org.apache.spark.executor.ExecutorExitCode
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

private[storage] abstract class FileAllocator(
    conf: SparkConf,
    localDirs: Array[File],
    subDirsPerLocalDir: Int)
  extends Logging {
  // The content of subDirs is immutable but the content of subDirs(i) is mutable. And the content
  // of subDirs(i) is protected by the lock of subDirs(i)
  val subDirs = Array.fill(localDirs.length)(new Array[File](subDirsPerLocalDir))

  def support: Boolean = true

  def apply(filename: String): File

  protected def getFile(filename: String, storageDirs: Array[File]): File = {
    require(storageDirs.nonEmpty, "could not find file when the directories are empty")

    // Figure out which local directory it hashes to, and which subdirectory in that
    val hash = Utils.nonNegativeHash(filename)
    val dirId = localDirs.indexOf(storageDirs(hash % storageDirs.length))
    val subDirId = (hash / storageDirs.length) % subDirsPerLocalDir

    // Create the subdirectory if it doesn't already exist
    val subDir = subDirs(dirId).synchronized {
      val old = subDirs(dirId)(subDirId)
      if (old != null) {
        old
      } else {
        val newDir = new File(localDirs(dirId), "%02x".format(subDirId))
        if (!newDir.exists() && !newDir.mkdir()) {
          throw new IOException(s"Failed to create local dir in $newDir.")
        }
        subDirs(dirId)(subDirId) = newDir
        newDir
      }
    }

    new File(subDir, filename)
  }
}

/** Looks up a file by hashing it into one of our local subdirectories. */
// This method should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getFile().
private[storage] class HashAllocator(
    conf: SparkConf,
    localDirs: Array[File],
    subDirsPerLocalDir: Int)
  extends FileAllocator(
    conf,
    localDirs,
    subDirsPerLocalDir) {
  def apply(filename: String): File = getFile(filename, localDirs)
}

/** Looks up a file by tier way in different speed storage devices. */
private[storage] class TieredAllocator(
    conf: SparkConf,
    localDirs: Array[File],
    subDirsPerLocalDir: Int)
  extends FileAllocator(
    conf,
    localDirs,
    subDirsPerLocalDir) {
  val tiersEnvConf = conf.getenv("SPARK_DIRS_TIERS")
  val threshold = conf.getDouble("spark.diskStore.tiered.threshold", 0.15)

  val tiersIDs = tiersEnvConf.trim.split("")
  if (localDirs.length != tiersIDs.length) {
    logError(s"Incorrect SPARK_DIRS_TIERS setting, SPARK_DIRS_TIERS = '$tiersEnvConf'.")
    System.exit(ExecutorExitCode.DISK_STORE_FAILED_TO_BUILD_TIERED_STORAGE)
  }

  val tieredDirs: Seq[Array[File]] = (localDirs zip tiersIDs).
    groupBy(_._2).mapValues(_.map(_._1)).toSeq.sortBy(_._1).map(_._2)
  tieredDirs.zipWithIndex.foreach {
    case (dirs, index) =>
      logInfo(s"Tier $index:")
      dirs.foreach(d => logInfo(s"    $d"))
  }

  override def support: Boolean = {
    tiersEnvConf != null &&
      // TODO: tiered allocation for external shuffle service will be supported in another PR
      !conf.getBoolean("spark.shuffle.service.enabled", false)
  }

  def hasEnoughSpace(file: File): Boolean = {
    file.getParentFile.getFreeSpace * 1.0 / file.getParentFile.getTotalSpace >= threshold
  }

  def apply(filename: String): File = {
    var file: File = null
    var availableFile: File = null
    for (tier <- tieredDirs) {
      file = getFile(filename, tier)
      if (file.exists()) return file

      if (availableFile == null && hasEnoughSpace(file)) {
        availableFile = file
      }
    }
    Option(availableFile).getOrElse(file)
  }
}
