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
import java.text.SimpleDateFormat
import java.util.{Date, Locale, Random}

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.hive.common.FileUtils
import org.apache.hadoop.hive.ql.exec.TaskRunner

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.client.HiveVersion

class HiveTempPath(session: SparkSession, val hadoopConf: Configuration, path: Path)
  extends Logging {
  private var stagingDirForCreating: Option[Path] = None

  lazy val externalTempPath: Path = getExternalTmpPath(path)

  private def getExternalTmpPath(path: Path): Path = {
    import org.apache.spark.sql.hive.client.hive._

    // Before Hive 1.1, when inserting into a table, Hive will create the staging directory under
    // a common scratch directory. After the writing is finished, Hive will simply empty the table
    // directory and move the staging directory to it.
    // After Hive 1.1, Hive will create the staging directory under the table directory, and when
    // moving staging directory to table directory, Hive will still empty the table directory, but
    // will exclude the staging directory there.
    // We have to follow the Hive behavior here, to avoid troubles. For example, if we create
    // staging directory under the table director for Hive prior to 1.1, the staging directory will
    // be removed by Hive when Hive is trying to empty the table directory.
    val hiveVersionsUsingOldExternalTempPath: Set[HiveVersion] = Set(v12, v13, v14, v1_0)
    val hiveVersionsUsingNewExternalTempPath: Set[HiveVersion] =
      Set(v1_1, v1_2, v2_0, v2_1, v2_2, v2_3, v3_0, v3_1)

    // Ensure all the supported versions are considered here.
    assert(hiveVersionsUsingNewExternalTempPath ++ hiveVersionsUsingOldExternalTempPath ==
      allSupportedHiveVersions)

    val externalCatalog = session.sharedState.externalCatalog
    val hiveVersion = externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client.version
    val stagingDir = hadoopConf.get("hive.exec.stagingdir", ".hive-staging")
    val scratchDir = hadoopConf.get("hive.exec.scratchdir", "/tmp/hive")

    if (hiveVersionsUsingOldExternalTempPath.contains(hiveVersion)) {
      oldVersionExternalTempPath(path, scratchDir)
    } else if (hiveVersionsUsingNewExternalTempPath.contains(hiveVersion)) {
      newVersionExternalTempPath(path, stagingDir)
    } else {
      throw new IllegalStateException("Unsupported hive version: " + hiveVersion.fullVersion)
    }
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 0.13
  private def oldVersionExternalTempPath(path: Path, scratchDir: String): Path = {
    val extURI: URI = path.toUri
    val scratchPath = new Path(scratchDir, executionId)
    var dirPath = new Path(
      extURI.getScheme,
      extURI.getAuthority,
      scratchPath.toUri.getPath + "-" + TaskRunner.getTaskRunnerID())

    val fs = dirPath.getFileSystem(hadoopConf)
    dirPath = new Path(fs.makeQualified(dirPath).toString())
    stagingDirForCreating = Some(dirPath)
    dirPath
  }

  // Mostly copied from Context.java#getExternalTmpPath of Hive 1.2
  private def newVersionExternalTempPath(path: Path, stagingDir: String): Path = {
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
    val format = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_SSS", Locale.US)
    "hive_" + format.format(new Date) + "_" + Math.abs(rand.nextLong)
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
        logWarning(s"Unable to delete staging directory: $stagingDir.\n" + e)
    }
  }

  def createTmpPath(): Unit = {
    try {
      stagingDirForCreating.foreach { stagingDir =>
        val fs: FileSystem = stagingDir.getFileSystem(hadoopConf)
        if (!FileUtils.mkdir(fs, stagingDir, true, hadoopConf)) {
          throw new IllegalStateException(
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
}
