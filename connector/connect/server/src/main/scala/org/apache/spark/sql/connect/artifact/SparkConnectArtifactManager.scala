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

import java.net.{URL, URLClassLoader}
import java.nio.file.{Files, Path, Paths}
import java.util.concurrent.CopyOnWriteArrayList

import scala.collection.JavaConverters._

import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * The Artifact Manager for the [[SparkConnectService]].
 *
 * This class handles the storage of artifacts as well as preparing the artifacts for use.
 * Currently, jars and classfile artifacts undergo additional processing:
 *   - Jars are automatically added to the underlying [[SparkContext]] and are accessible by all
 *     users of the cluster.
 *   - Class files are moved into a common directory that is shared among all users of the
 *     cluster. Note: Under a multi-user setup, class file conflicts may occur between user
 *     classes as the class file directory is shared.
 */
class SparkConnectArtifactManager private[connect] {

  // The base directory where all artifacts are stored.
  // Note: If a REPL is attached to the cluster, class file artifacts are stored in the
  // REPL's output directory.
  private[connect] val artifactRootPath = Utils.createTempDir("artifacts").toPath
  private[connect] val artifactRootURI = {
    val fileServer = SparkEnv.get.rpcEnv.fileServer
    fileServer.addDirectory("artifacts", artifactRootPath.toFile)
  }

  // The base directory where all class files are stored.
  // Note: If a REPL is attached to the cluster, we piggyback on the existing REPL output
  // directory to store class file artifacts.
  private[connect] val classArtifactDir = {
    val dir = SparkEnv.get.conf
      .getOption("spark.repl.class.outputDir")
      .map(p => Paths.get(p))
      .getOrElse(artifactRootPath.resolve("classes"))
    Files.createDirectories(dir)
    dir
  }
  private[connect] val classArtifactUri: String = {
    val conf = SparkEnv.get.conf
    // If set, piggyback on the existing repl class uri functionality that the executor uses
    // to load class files.
    conf.getOption("spark.repl.class.uri").getOrElse {
      val fileServer = SparkEnv.get.rpcEnv.fileServer
      fileServer.addDirectory(artifactRootURI + "/classes", classArtifactDir.toFile)
    }
  }

  private val jarsList = new CopyOnWriteArrayList[Path]

  /**
   * Get the URLs of all jar artifacts added through the [[SparkConnectService]].
   *
   * @return
   */
  def getSparkConnectAddedJars: Seq[URL] = jarsList.asScala.map(_.toUri.toURL).toSeq

  /**
   * Add and prepare a staged artifact (i.e an artifact that has been rebuilt locally from bytes
   * over the wire) for use.
   *
   * @param session
   * @param remoteRelativePath
   * @param serverLocalStagingPath
   */
  private[connect] def addArtifact(
      session: SparkSession,
      remoteRelativePath: Path,
      serverLocalStagingPath: Path): Unit = {
    require(!remoteRelativePath.isAbsolute)
    if (remoteRelativePath.startsWith("classes/")) {
      // Move class files to common location (shared among all users)
      val target = classArtifactDir.resolve(remoteRelativePath.toString.stripPrefix("classes/"))
      Files.createDirectories(target.getParent)
      Files.move(serverLocalStagingPath, target)
    } else {
      val target = artifactRootPath.resolve(remoteRelativePath)
      Files.createDirectories(target.getParent)
      Files.move(serverLocalStagingPath, target)
      if (remoteRelativePath.startsWith("jars")) {
        // Adding Jars to the underlying spark context (visible to all users)
        session.sessionState.resourceLoader.addJar(target.toString)
        jarsList.add(target)
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
