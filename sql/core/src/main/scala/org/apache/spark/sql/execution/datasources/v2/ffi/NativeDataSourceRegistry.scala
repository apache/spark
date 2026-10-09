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
package org.apache.spark.sql.execution.datasources.v2.ffi

import java.io.File
import java.nio.file.Paths

import org.apache.spark.{SparkContext, SparkEnv, SparkFiles}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.datasource.NativeBridge
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.Utils

/**
 * Finds the native data source packages on the driver. Like the Python data sources installed in
 * the Python path, the packages installed in `$SPARK_HOME/native-datasources` are found
 * automatically. The active session can also add packages with `spark.addArtifact`, or with
 * `spark.sql.dataSource.native.paths`, which take precedence over the installed ones.
 */
object NativeDataSourceRegistry extends Logging {

  /** The directory of `SPARK_HOME` that contains the installed native data source packages. */
  val INSTALLED_PACKAGES_DIR = "native-datasources"

  /** Returns whether a package provides the native data source with the given name. */
  def exists(name: String, conf: SQLConf): Boolean = lookup(name, conf).isDefined

  /**
   * Finds the package that provides the native data source with the given name, which is
   * case-insensitive. Returns the name of the data source as listed in the manifest, and the
   * package.
   */
  def lookup(name: String, conf: SQLConf): Option[(String, NativeDataSourcePackage)] = {
    if (!conf.getConf(StaticSQLConf.NATIVE_DATA_SOURCE_ENABLED)) {
      return None
    }
    lookup(name, configuredPackageFiles(conf) ++ artifactPackageFiles())
      .orElse(lookup(name, installedPackageFiles()))
  }

  private def lookup(name: String, files: Seq[File]): Option[(String, NativeDataSourcePackage)] = {
    val found = files.map(_.getCanonicalFile).distinct.map(NativeDataSourcePackage.read)
      .flatMap(pkg => pkg.manifest.dataSources.find(_.equalsIgnoreCase(name)).map(_ -> pkg))
      // The same package can be found more than once, for example when it is in a configured
      // directory and was also added as an artifact.
      .groupBy(_._2.sha256).values.map(_.head).toSeq
    found match {
      case Seq() => None
      case Seq(result @ (_, pkg)) =>
        if (pkg.manifest.abiVersion != NativeBridge.ABI_VERSION) {
          throw QueryExecutionErrors.invalidNativeDataSourcePackageError(
            pkg.path,
            "UNSUPPORTED_ABI_VERSION",
            Map(
              "version" -> pkg.manifest.abiVersion.toString,
              "supported" -> NativeBridge.ABI_VERSION.toString))
        }
        Some(result)
      case _ =>
        throw QueryCompilationErrors.nativeDataSourcePackageConflictError(
          name, found.map(_._2.path).sorted)
    }
  }

  /**
   * Makes the package available to the executors of the active session. Returns the package with
   * the location of the copies of the executors.
   */
  def distribute(pkg: NativeDataSourcePackage): NativeDataSourcePackage = {
    SparkSession.getActiveSession match {
      // In local mode, the executors run in the driver and read the package where it is.
      case Some(session) if !session.sparkContext.isLocal => addToArtifacts(session, pkg)
      case _ => pkg
    }
  }

  /** Adds the package as an artifact of the session, unless it already is one. */
  private[ffi] def addToArtifacts(
      session: SparkSession,
      pkg: NativeDataSourcePackage): NativeDataSourcePackage = {
    val artifactManager = session.artifactManager
    val (added, artifactUUID) = artifactManager.getNativeDataSourcePackages
    if (!added.exists(_.getName == pkg.fileName)) {
      artifactManager.addLocalArtifacts(Artifact.newFileArtifact(
        Paths.get(pkg.fileName), new Artifact.LocalFile(Paths.get(pkg.path))) :: Nil)
    }
    pkg.copy(artifactUUID = artifactUUID)
  }

  /**
   * The local copies, on an executor, of a package distributed with [[distribute]]. The files of
   * an isolated session are stored in a directory named after the UUID of the session.
   */
  def distributedCopies(pkg: NativeDataSourcePackage): Seq[File] = {
    if (SparkEnv.get == null) {
      Nil
    } else {
      val root = new File(SparkFiles.getRootDirectory())
      pkg.artifactUUID.map(uuid => new File(new File(root, uuid), pkg.fileName)).toSeq :+
        new File(root, pkg.fileName)
    }
  }

  /** The packages installed in `$SPARK_HOME/native-datasources`, if the directory exists. */
  private[ffi] def installedPackageFiles(): Seq[File] = {
    SparkContext.getActive.flatMap(_.getSparkHome()).toSeq
      .map(home => new File(home, INSTALLED_PACKAGES_DIR))
      .filter(_.isDirectory)
      .flatMap(packageFilesIn)
  }

  private def configuredPackageFiles(conf: SQLConf): Seq[File] = {
    conf.getConf(SQLConf.NATIVE_DATA_SOURCE_PATHS).flatMap { path =>
      val uri = Utils.resolveURI(path)
      val file = new File(uri.getPath)
      if (uri.getScheme != null && uri.getScheme != "file") {
        logWarning(log"Ignoring the native data source path ${MDC(PATH, path)}: only local " +
          log"paths are supported. Add remote packages with spark.addArtifact instead.")
        Nil
      } else if (file.isDirectory) {
        packageFilesIn(file)
      } else if (file.isFile) {
        Seq(file)
      } else {
        logWarning(log"The native data source path ${MDC(PATH, path)} does not exist.")
        Nil
      }
    }
  }

  private def packageFilesIn(dir: File): Seq[File] = {
    dir.listFiles()
      .filter(f => f.isFile && f.getName.endsWith(NativeDataSourcePackage.FILE_EXTENSION))
      .sortBy(_.getName)
      .toSeq
  }

  private def artifactPackageFiles(): Seq[File] = {
    SparkSession.getActiveSession.toSeq.flatMap(_.artifactManager.getNativeDataSourcePackages._1)
  }
}
