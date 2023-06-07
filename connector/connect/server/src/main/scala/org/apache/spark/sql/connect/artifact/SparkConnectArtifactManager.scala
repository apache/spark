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

package org.apache.spark.sql.connect.artifact

import java.io.File
import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths, StandardCopyOption}
import java.util.concurrent.CopyOnWriteArrayList
import javax.ws.rs.core.UriBuilder

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.storage.{CacheId, StorageLevel}
import org.apache.spark.util.Utils

/**
 * The Artifact Manager for the [[SparkConnectService]].
 *
 * This class handles the storage of artifacts as well as preparing the artifacts for use.
 * Currently, jars and classfile artifacts undergo additional processing:
 *   - Jars and pyfiles are automatically added to the underlying [[SparkContext]] and are
 *     accessible by all users of the cluster.
 *   - Class files are moved into a common directory that is shared among all users of the
 *     cluster. Note: Under a multi-user setup, class file conflicts may occur between user
 *     classes as the class file directory is shared.
 */
class SparkConnectArtifactManager private[connect] {

  // The base directory where all artifacts are stored.
  // Note: If a REPL is attached to the cluster, class file artifacts are stored in the
  // REPL's output directory.
  private[connect] lazy val artifactRootPath = SparkContext.getActive match {
    case Some(sc) =>
      sc.sparkConnectArtifactDirectory.toPath
    case None =>
      throw new RuntimeException("SparkContext is uninitialized!")
  }
  private[connect] lazy val artifactRootURI = {
    val fileServer = SparkEnv.get.rpcEnv.fileServer
    fileServer.addDirectory("artifacts", artifactRootPath.toFile)
  }

  // The base directory where all class files are stored.
  // Note: If a REPL is attached to the cluster, we piggyback on the existing REPL output
  // directory to store class file artifacts.
  private[connect] lazy val classArtifactDir = SparkEnv.get.conf
    .getOption("spark.repl.class.outputDir")
    .map(p => Paths.get(p))
    .getOrElse(artifactRootPath.resolve("classes"))

  private[connect] lazy val classArtifactUri: String =
    SparkEnv.get.conf.getOption("spark.repl.class.uri") match {
      case Some(uri) => uri
      case None =>
        throw new RuntimeException("Class artifact URI had not been initialised in SparkContext!")
    }

  private val jarsList = new CopyOnWriteArrayList[Path]
  private val pythonIncludeList = new CopyOnWriteArrayList[String]

  /**
   * Get the URLs of all jar artifacts added through the [[SparkConnectService]].
   *
   * @return
   */
  def getSparkConnectAddedJars: Seq[URL] = jarsList.asScala.map(_.toUri.toURL).toSeq

  /**
   * Get the py-file names added through the [[SparkConnectService]].
   *
   * @return
   */
  def getSparkConnectPythonIncludes: Seq[String] = pythonIncludeList.asScala.toSeq

  /**
   * Add and prepare a staged artifact (i.e an artifact that has been rebuilt locally from bytes
   * over the wire) for use.
   *
   * @param session
   * @param remoteRelativePath
   * @param serverLocalStagingPath
   * @param fragment
   */
  private[connect] def addArtifact(
      sessionHolder: SessionHolder,
      remoteRelativePath: Path,
      serverLocalStagingPath: Path,
      fragment: Option[String]): Unit = {
    require(!remoteRelativePath.isAbsolute)
    if (remoteRelativePath.startsWith(s"cache${File.separator}")) {
      val tmpFile = serverLocalStagingPath.toFile
      Utils.tryWithSafeFinallyAndFailureCallbacks {
        val blockManager = sessionHolder.session.sparkContext.env.blockManager
        val blockId = CacheId(
          userId = sessionHolder.userId,
          sessionId = sessionHolder.sessionId,
          hash = remoteRelativePath.toString.stripPrefix(s"cache${File.separator}"))
        val updater = blockManager.TempFileBasedBlockStoreUpdater(
          blockId = blockId,
          level = StorageLevel.MEMORY_AND_DISK_SER,
          classTag = implicitly[ClassTag[Array[Byte]]],
          tmpFile = tmpFile,
          blockSize = tmpFile.length(),
          tellMaster = false)
        updater.save()
      }(catchBlock = { tmpFile.delete() })
    } else if (remoteRelativePath.startsWith(s"classes${File.separator}")) {
      // Move class files to common location (shared among all users)
      val target = classArtifactDir.resolve(
        remoteRelativePath.toString.stripPrefix(s"classes${File.separator}"))
      Files.createDirectories(target.getParent)
      // Allow overwriting class files to capture updates to classes.
      Files.move(serverLocalStagingPath, target, StandardCopyOption.REPLACE_EXISTING)
    } else {
      val target = artifactRootPath.resolve(remoteRelativePath)
      Files.createDirectories(target.getParent)
      // Disallow overwriting jars because spark doesn't support removing jars that were
      // previously added,
      if (Files.exists(target)) {
        throw new RuntimeException(
          s"Duplicate file: $remoteRelativePath. Files cannot be overwritten.")
      }
      Files.move(serverLocalStagingPath, target)
      if (remoteRelativePath.startsWith(s"jars${File.separator}")) {
        // Adding Jars to the underlying spark context (visible to all users)
        sessionHolder.session.sessionState.resourceLoader.addJar(target.toString)
        jarsList.add(target)
      } else if (remoteRelativePath.startsWith(s"pyfiles${File.separator}")) {
        sessionHolder.session.sparkContext.addFile(target.toString)
        val stringRemotePath = remoteRelativePath.toString
        if (stringRemotePath.endsWith(".zip") || stringRemotePath.endsWith(
            ".egg") || stringRemotePath.endsWith(".jar")) {
          pythonIncludeList.add(target.getFileName.toString)
        }
      } else if (remoteRelativePath.startsWith(s"archives${File.separator}")) {
        val canonicalUri =
          fragment.map(UriBuilder.fromUri(target.toUri).fragment).getOrElse(target.toUri)
        sessionHolder.session.sparkContext.addArchive(canonicalUri.toString)
      } else if (remoteRelativePath.startsWith(s"files${File.separator}")) {
        sessionHolder.session.sparkContext.addFile(target.toString)
      }
    }
  }
}

object SparkConnectArtifactManager {

  private var _activeArtifactManager: SparkConnectArtifactManager = _

  /**
   * Obtain the active artifact manager or create a new artifact manager.
   *
   * @return
   */
  def getOrCreateArtifactManager: SparkConnectArtifactManager = {
    if (_activeArtifactManager == null) {
      _activeArtifactManager = new SparkConnectArtifactManager
    }
    _activeArtifactManager
  }

  private lazy val artifactManager = getOrCreateArtifactManager

  /**
   * Obtain a classloader that contains jar and classfile artifacts on the classpath.
   *
   * @return
   */
  def classLoaderWithArtifacts: ClassLoader = {
    val urls = artifactManager.getSparkConnectAddedJars :+
      artifactManager.classArtifactDir.toUri.toURL
    new URLClassLoader(urls.toArray, Utils.getContextOrSparkClassLoader)
  }

  /**
   * Run a segment of code utilising a classloader that contains jar and classfile artifacts on
   * the classpath.
   *
   * @param thunk
   * @tparam T
   * @return
   */
  def withArtifactClassLoader[T](thunk: => T): T = {
    Utils.withContextClassLoader(classLoaderWithArtifacts) {
      thunk
    }
  }
}
