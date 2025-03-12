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

package org.apache.spark.sql.hive.execution

import java.io.IOException
import java.net.URI
import java.time.ZoneId
import java.time.format.DateTimeFormatter
import java.util.{Date, Locale, Random}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.TaskRunner

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.hive.HiveExternalCatalog

class HiveTempPath(session: SparkSession, val hadoopConf: Configuration, path: Path)
  extends Logging {
  private var stagingDirForCreating: Option[Path] = None

  lazy val externalTempPath: Path = getExternalTmpPath(path)

  private lazy val dateTimeFormatter =
    DateTimeFormatter
      .ofPattern("yyyy-MM-dd_HH-mm-ss_SSS", Locale.US)
      .withZone(ZoneId.systemDefault())

  private def getExternalTmpPath(path: Path): Path = {
    import org.apache.spark.sql.hive.client.hive._

    // Hive will creates the staging directory under the table directory, and when
    // moving staging directory to table directory, Hive will still empty the table directory, but
    // will exclude the staging directory there.

    val externalCatalog = session.sharedState.externalCatalog
    val hiveVersion = externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client.version
    val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")

    if (allSupportedHiveVersions.contains(hiveVersion)) {
      externalTempPath(path, stagingDir)
    } else {
      throw SparkException.internalError("Unsupported hive version: " + hiveVersion.fullVersion)
    }
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 1.2
  private def externalTempPath(path: Path, stagingDir: String): Path = {
    val extURI: URI = path.toUri
    if (extURI.getScheme == "viewfs") {
      val qualifiedStagingDir = getStagingDir(path, stagingDir)
      stagingDirForCreating = Some(qualifiedStagingDir)
      // Hive uses 10000
      new Path(qualifiedStagingDir, "-ext-10000")
    } else {
      val qualifiedStagingDir = getExternalScratchDir(extURI, stagingDir)
      stagingDirForCreating = Some(qualifiedStagingDir)
      new Path(qualifiedStagingDir, "-ext-10000")
    }
  }

  private def getExternalScratchDir(extURI: URI, stagingDir: String): Path = {
    getStagingDir(
      new Path(extURI.getScheme, extURI.getAuthority, extURI.getPath),
      stagingDir)
  }

  private[hive] def getStagingDir(inputPath: Path, stagingDir: String): Path = {
    val inputPathName: String = inputPath.toString
    val fs: FileSystem = inputPath.getFileSystem(hadoopConf)
    var stagingPathName: String =
      if (inputPathName.indexOf(stagingDir) == -1) {
        new Path(inputPathName, stagingDir).toString
      } else {
        inputPathName.substring(0, inputPathName.indexOf(stagingDir) + stagingDir.length)
      }

    // SPARK-20594: This is a walk-around fix to resolve a Hive bug. Hive requires that the
    // staging directory needs to avoid being deleted when users set hive.exec.stagingdir
    // under the table directory.
    if (isSubDir(new Path(stagingPathName), inputPath, fs) &&
      !stagingPathName.stripPrefix(inputPathName).stripPrefix("/").startsWith(".")) {
      logDebug(s"The staging dir '$stagingPathName' should be a child directory starts " +
        "with '.' to avoid being deleted if we set hive.exec.stagingdir under the table " +
        "directory.")
      stagingPathName = new Path(inputPathName, ".hive-staging").toString
    }

    val dir: Path =
      fs.makeQualified(
        new Path(stagingPathName + "_" + executionId + "-" + TaskRunner.getTaskRunnerID))
    logDebug("Created staging dir = " + dir + " for path = " + inputPath)
    dir
  }

  // HIVE-14259 removed FileUtils.isSubDir(). Adapted it from Hive 1.2's FileUtils.isSubDir().
  private def isSubDir(p1: Path, p2: Path, fs: FileSystem): Boolean = {
    val path1 = fs.makeQualified(p1).toString + Path.SEPARATOR
    val path2 = fs.makeQualified(p2).toString + Path.SEPARATOR
    path1.startsWith(path2)
  }

  private def executionId: String = {
    val rand: Random = new Random
    "hive_" + dateTimeFormatter.format(new Date().toInstant) + "_" + Math.abs(rand.nextLong)
  }

  def deleteTmpPath() : Unit = {
    // Attempt to delete the staging directory and the inclusive files. If failed, the files are
    // expected to be dropped at the normal termination of VM since deleteOnExit is used.
    try {
      stagingDirForCreating.foreach { stagingDir =>
        val fs = stagingDir.getFileSystem(hadoopConf)
        if (fs.delete(stagingDir, true)) {
          // If we successfully delete the staging directory, remove it from FileSystem's cache.
          fs.cancelDeleteOnExit(stagingDir)
        }
      }
    } catch {
      case NonFatal(e) =>
        val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
        logWarning(log"Unable to delete staging directory: ${MDC(PATH, stagingDir)}.", e)
    }
  }

  def createTmpPath(): Unit = {
    try {
      stagingDirForCreating.foreach { stagingDir =>
        val fs: FileSystem = stagingDir.getFileSystem(hadoopConf)
        if (!FileUtils.mkdir(fs, stagingDir, true, hadoopConf)) {
          throw SparkException.internalError(
            "Cannot create staging directory  '" + stagingDir.toString + "'")
        }
        fs.deleteOnExit(stagingDir)
      }
    } catch {
      case e: IOException =>
        throw QueryExecutionErrors.cannotCreateStagingDirError(
          s"'${stagingDirForCreating.toString}': ${e.getMessage}", e)
    }
  }

  def deleteIfNotStagingDir(path: Path, fs: FileSystem): Unit = {
    if (Option(path) != stagingDirForCreating) fs.delete(path, true)
  }

  override def toString: String = s"HiveTempPath($path)"
}
