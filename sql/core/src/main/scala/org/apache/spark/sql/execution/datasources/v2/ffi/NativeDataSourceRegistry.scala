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
import java.util.Locale

import org.apache.spark.{SparkEnv, SparkFiles}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.datasource.NativeBridge
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.Utils

/**
 * Finds the native data sources on the driver. Like the Python data sources installed in the
 * Python path, the native data source libraries installed in the native library path are found
 * automatically, by their file name: the library `spark_datasource_<name>`, such as
 * `libspark_datasource_my_source.so` on Linux, implements the data source `<name>`. The active
 * session can also add packages with `spark.addArtifact`, or with
 * `spark.sql.dataSource.native.paths`, which take precedence over the installed libraries.
 */
object NativeDataSourceRegistry extends Logging {

  /** The prefix of the names of the installed native data source libraries. */
  val LIBRARY_NAME_PREFIX = "spark_datasource_"

  /** Returns whether the native data source with the given name exists. */
  def exists(name: String, conf: SQLConf): Boolean = lookup(name, conf).isDefined

  /**
   * Finds the package or the installed library that provides the native data source with the
   * given name, which is case-insensitive. Returns the name of the data source, as listed in the
   * manifest of the package or in lower case for an installed library, and where its library is.
   */
  def lookup(name: String, conf: SQLConf): Option[(String, NativeLibraryLocation)] = {
    if (!conf.getConf(StaticSQLConf.NATIVE_DATA_SOURCE_ENABLED)) {
      return None
    }
    lookupPackage(name, configuredPackageFiles(conf) ++ artifactPackageFiles())
      .orElse(lookupInstalledLibrary(name))
  }

  private def lookupPackage(
      name: String,
      files: Seq[File]): Option[(String, NativeDataSourcePackage)] = {
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

  private def lookupInstalledLibrary(name: String): Option[(String, InstalledNativeLibrary)] = {
    // Other names, such as class names, are not the names of libraries.
    if (!name.matches("[A-Za-z0-9_]+")) {
      return None
    }
    val dataSource = name.toLowerCase(Locale.ROOT)
    findInstalledLibrary(libraryFileName(dataSource))
      .map(file => dataSource -> InstalledNativeLibrary(file.getPath))
  }

  /** Finds the library with the given file name in the native library path of this node. */
  private[ffi] def findInstalledLibrary(fileName: String): Option[File] = {
    librarySearchPath(System.getProperty("java.library.path"), System.getenv("PATH"))
      .map(new File(_, fileName))
      .find(_.isFile)
  }

  /**
   * The file name of the installed library of the native data source with the given name, such
   * as `libspark_datasource_my_source.so` on Linux, `libspark_datasource_my_source.dylib` on
   * macOS and `spark_datasource_my_source.dll` on Windows.
   */
  def libraryFileName(name: String): String = {
    System.mapLibraryName(LIBRARY_NAME_PREFIX + name.toLowerCase(Locale.ROOT))
  }

  /**
   * The native library path: the directories where native data source libraries are installed,
   * in the order they are searched. They are the directories of `java.library.path`, where the
   * JVM finds native libraries, which include `LD_LIBRARY_PATH` on Linux, `DYLD_LIBRARY_PATH` on
   * macOS and `PATH` on Windows, and then the `lib` directory of each installation prefix in
   * `PATH`, such as `/usr/local/lib` for `/usr/local/bin`. Relative directories are ignored.
   */
  private[ffi] def librarySearchPath(javaLibraryPath: String, path: String): Seq[File] = {
    def directories(value: String): Seq[File] = {
      Option(value).toSeq.flatMap(_.split(File.pathSeparator)).map(new File(_)).filter(_.isAbsolute)
    }
    val prefixLibraries = directories(path)
      .filter(_.getName == "bin")
      .flatMap(bin => Option(bin.getParentFile))
      .map(new File(_, "lib"))
    (directories(javaLibraryPath) ++ prefixLibraries).distinct
  }

  /**
   * Makes the library available to the executors of the active session. Returns where the
   * executors find it. An installed library is already installed on every node.
   */
  def distribute(location: NativeLibraryLocation): NativeLibraryLocation = location match {
    case pkg: NativeDataSourcePackage =>
      SparkSession.getActiveSession match {
        // In local mode, the executors run in the driver and read the package where it is.
        case Some(session) if !session.sparkContext.isLocal => addToArtifacts(session, pkg)
        case _ => pkg
      }
    case installed: InstalledNativeLibrary => installed
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
