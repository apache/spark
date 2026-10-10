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

import java.io.{File, FileInputStream, IOException}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, StandardCopyOption}
import java.security.MessageDigest
import java.util.{HexFormat, Locale}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.ZipFile

import scala.jdk.CollectionConverters._
import scala.util.Try

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.PATH
import org.apache.spark.sql.Artifact
import org.apache.spark.sql.datasource.NativeBridge
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.util.Utils

/**
 * The manifest of a native data source package, `spark-native-datasource.json`:
 * {{{
 *   {
 *     "abiVersion": 1,
 *     "dataSources": ["my_source"],
 *     "libraries": {
 *       "linux-x86_64": "linux-x86_64/libmy_source.so",
 *       "osx-aarch64": "osx-aarch64/libmy_source.dylib"
 *     }
 *   }
 * }}}
 *
 * @param abiVersion the version of [[NativeBridge]] that the libraries implement
 * @param dataSources the names of the data sources that the libraries implement
 * @param libraries the path of the library in the package for each platform, as returned by
 *                  [[NativePlatform.current]]
 */
case class NativeDataSourceManifest(
    abiVersion: Int,
    dataSources: Seq[String],
    libraries: Map[String, String])

/**
 * Where the library of a native data source is: in a [[NativeDataSourcePackage]], or installed in
 * the native library path, as an [[InstalledNativeLibrary]]. It is sent to the executors, which
 * load the library with [[NativeLibraries.get]].
 */
sealed trait NativeLibraryLocation extends Serializable

/**
 * A native data source package: a zip file that contains a [[NativeDataSourceManifest]] and the
 * libraries it lists. The SHA-256 checksum of the file identifies the package, so that an
 * executor can check that it loads the same package as the driver.
 *
 * @param path the path of the package file on the driver
 * @param artifactUUID the UUID of the session whose artifacts contain the package, if the session
 *                     is isolated. Executors store the artifacts of such a session separately.
 */
case class NativeDataSourcePackage(
    path: String,
    sha256: String,
    manifest: NativeDataSourceManifest,
    artifactUUID: Option[String] = None) extends NativeLibraryLocation {
  def fileName: String = new File(path).getName
}

/**
 * A package file that is not a valid native data source package.
 *
 * @param error the error that explains why the package is invalid
 * @param dataSources the data sources listed in the manifest of the package, if it can be read
 */
case class InvalidNativeDataSourcePackage(error: Throwable, dataSources: Seq[String])

object NativeDataSourcePackage extends Logging {
  val FILE_EXTENSION: String = Artifact.NATIVE_DATA_SOURCE_PACKAGE_EXTENSION
  val MANIFEST_NAME = "spark-native-datasource.json"

  private case class CacheKey(path: String, length: Long, lastModified: Long)

  private[ffi] type ReadResult = Either[InvalidNativeDataSourcePackage, NativeDataSourcePackage]

  // Bounded, because each session stores its artifacts in a directory of its own, so a
  // long-running server where many sessions add packages reads many different files.
  private val MAX_CACHED_PACKAGES = 1000

  private val packages: Cache[CacheKey, ReadResult] =
    CacheBuilder.newBuilder().maximumSize(MAX_CACHED_PACKAGES).build[CacheKey, ReadResult]()
  private val mapper = new ObjectMapper()

  /** Reads the package in the given file. The result is cached until the file changes. */
  def read(file: File): NativeDataSourcePackage = tryRead(file).fold(p => throw p.error, identity)

  /**
   * Reads the package in the given file, or returns why it is invalid. The result is cached until
   * the file changes, and an invalid package is logged when it is read.
   */
  private[ffi] def tryRead(file: File): ReadResult = {
    val canonical = try {
      file.getCanonicalFile
    } catch {
      case e: IOException =>
        return Left(InvalidNativeDataSourcePackage(
          invalidManifestError(file, s"The package cannot be read: $e", e), Nil))
    }
    val key = CacheKey(canonical.getPath, canonical.length(), canonical.lastModified())
    Option(packages.getIfPresent(key)).getOrElse {
      val result = parse(canonical)
      result.swap.foreach { invalid =>
        logWarning(log"Invalid native data source package ${MDC(PATH, canonical)}.",
          invalid.error)
      }
      packages.put(key, result)
      result
    }
  }

  /**
   * Forgets the packages read from the files under the given directory, such as the artifacts of
   * a session that is cleaned up.
   */
  def forgetPackagesIn(dir: File): Unit = {
    val prefix = dir.getCanonicalPath + File.separator
    packages.asMap().keySet().removeIf(_.path.startsWith(prefix))
  }

  /** Returns whether a package read from the file at the given canonical path is cached. */
  private[ffi] def isCached(canonicalPath: String): Boolean = {
    packages.asMap().keySet().asScala.exists(_.path == canonicalPath)
  }

  private def invalidManifestError(file: File, reason: String, cause: Throwable = null) = {
    QueryExecutionErrors.invalidNativeDataSourcePackageError(
      file.getPath, "INVALID_MANIFEST", Map("reason" -> reason), cause)
  }

  private def parse(file: File): ReadResult = {
    def invalid(reason: String, dataSources: Seq[String] = Nil, cause: Throwable = null) = {
      Left(InvalidNativeDataSourcePackage(invalidManifestError(file, reason, cause), dataSources))
    }
    try {
      val manifest = Utils.tryWithResource(new ZipFile(file)) { zip =>
        Option(zip.getEntry(MANIFEST_NAME)) match {
          case None => invalid(s"The package does not contain the manifest $MANIFEST_NAME.")
          case Some(entry) =>
            val json = Utils.tryWithResource(zip.getInputStream(entry)) { in =>
              new String(in.readAllBytes(), StandardCharsets.UTF_8)
            }
            parseManifest(json) match {
              case Left(reason) => invalid(reason)
              case Right(manifest) =>
                manifest.libraries.values.find(zip.getEntry(_) == null) match {
                  case Some(library) => invalid(
                    s"The package does not contain the library $library.", manifest.dataSources)
                  case None => Right(manifest)
                }
            }
        }
      }
      manifest.map(NativeDataSourcePackage(file.getPath, sha256(file), _))
    } catch {
      case e: IOException => invalid(s"The package is not a valid zip file: $e", cause = e)
    }
  }

  private[ffi] def parseManifest(json: String): Either[String, NativeDataSourceManifest] = {
    val root: JsonNode = try {
      mapper.readTree(json)
    } catch {
      case e: IOException => return Left(s"The manifest is not valid JSON: ${e.getMessage}")
    }
    if (root == null || !root.isObject) {
      return Left("The manifest must be a JSON object.")
    }
    val abiVersion = root.get("abiVersion")
    if (abiVersion == null || !abiVersion.isInt) {
      return Left("The manifest must have an integer field 'abiVersion'.")
    }
    val dataSources = root.get("dataSources")
    if (dataSources == null || !dataSources.isArray || dataSources.isEmpty ||
        !dataSources.elements().asScala.forall(n => n.isTextual && n.asText().nonEmpty)) {
      return Left("The manifest must have a field 'dataSources' with a non-empty array of " +
        "data source names.")
    }
    val libraries = root.get("libraries")
    if (libraries == null || !libraries.isObject || libraries.isEmpty ||
        !libraries.properties().asScala.forall(e => e.getValue.isTextual)) {
      return Left("The manifest must have a field 'libraries' with an object that maps " +
        "platforms to the paths of the libraries in the package.")
    }
    Right(NativeDataSourceManifest(
      abiVersion.asInt(),
      dataSources.elements().asScala.map(_.asText()).toSeq,
      libraries.properties().asScala.map(e => e.getKey -> e.getValue.asText()).toMap))
  }

  private def sha256(file: File): String = {
    val digest = MessageDigest.getInstance("SHA-256")
    Utils.tryWithResource(new FileInputStream(file)) { in =>
      val buffer = new Array[Byte](64 * 1024)
      var read = in.read(buffer)
      while (read != -1) {
        digest.update(buffer, 0, read)
        read = in.read(buffer)
      }
    }
    HexFormat.of().formatHex(digest.digest())
  }
}

/**
 * A native data source library installed in the native library path, where
 * [[NativeDataSourceRegistry]] finds it by its file name. It must be installed on every node.
 *
 * @param path the path of the library on the driver
 */
case class InstalledNativeLibrary(path: String) extends NativeLibraryLocation

/** The platform names used in [[NativeDataSourceManifest.libraries]]. */
object NativePlatform {
  /** The platform of this JVM, such as `linux-x86_64`, `linux-aarch64` or `osx-aarch64`. */
  lazy val current: String = name(System.getProperty("os.name"), System.getProperty("os.arch"))

  private[ffi] def name(osName: String, osArch: String): String = {
    val os = osName.toLowerCase(Locale.ROOT) match {
      case n if n.startsWith("linux") => "linux"
      case n if n.startsWith("mac") || n.startsWith("darwin") => "osx"
      case n if n.startsWith("windows") => "windows"
      case n => n.replaceAll("[^a-z0-9]", "")
    }
    val arch = osArch.toLowerCase(Locale.ROOT) match {
      case "amd64" | "x86_64" | "x64" => "x86_64"
      case "aarch64" | "arm64" => "aarch64"
      case a => a
    }
    s"$os-$arch"
  }
}

/**
 * The native data source libraries loaded in this JVM. A library is loaded once, on first use,
 * after it is extracted from its package if needed, and stays loaded until the JVM exits.
 */
object NativeLibraries extends Logging {
  // The libraries of the packages, by the checksum of the package, and the installed libraries,
  // by their canonical path: the JVM loads a library file in a single class loader. A library
  // that is loaded but rejected stays loaded, so its error is kept: it cannot be loaded again.
  private val packagedLibraries = new ConcurrentHashMap[String, Try[NativeLibrary]]()
  private val installedLibraries = new ConcurrentHashMap[String, Try[NativeLibrary]]()

  private lazy val extractionDir: File = Utils.createTempDir(namePrefix = "native-datasources")

  /** Returns the library at the given location, and loads it if needed. */
  private[ffi] def get(location: NativeLibraryLocation): NativeLibrary = location match {
    case pkg: NativeDataSourcePackage =>
      getOrLoad(packagedLibraries, pkg.sha256)(loadPackaged(pkg))
    case installed: InstalledNativeLibrary =>
      val file = locate(installed).getCanonicalFile
      getOrLoad(installedLibraries, file.getPath)(loadInstalled(file))
  }

  private def getOrLoad(libraries: ConcurrentHashMap[String, Try[NativeLibrary]], key: String)(
      load: => Try[NativeLibrary]): NativeLibrary = {
    val library = libraries.get(key)
    (if (library != null) library else libraries.computeIfAbsent(key, _ => load)).get
  }

  private def loadPackaged(pkg: NativeDataSourcePackage): Try[NativeLibrary] = {
    val platform = NativePlatform.current
    val libraryPath = pkg.manifest.libraries.getOrElse(platform, {
      throw QueryExecutionErrors.invalidNativeDataSourcePackageError(
        pkg.path,
        "UNSUPPORTED_PLATFORM",
        Map(
          "platform" -> platform,
          "platforms" -> pkg.manifest.libraries.keys.toSeq.sorted.mkString(", ")))
    })
    val packageFile = locate(pkg)
    val libraryFile = new File(new File(extractionDir, pkg.sha256), new File(libraryPath).getName)
    if (!libraryFile.isFile) {
      extract(packageFile, libraryPath, libraryFile)
    }
    load(
      libraryFile,
      (reason, cause) => QueryExecutionErrors.invalidNativeDataSourcePackageError(
        pkg.path, "INVALID_LIBRARY", Map("library" -> libraryPath, "reason" -> reason), cause),
      parameters => QueryExecutionErrors.invalidNativeDataSourcePackageError(
        pkg.path, "UNSUPPORTED_ABI_VERSION", parameters))
  }

  private def loadInstalled(file: File): Try[NativeLibrary] = {
    load(
      file,
      (reason, cause) => QueryExecutionErrors.invalidNativeDataSourceLibraryError(
        file.getPath, "CANNOT_LOAD", Map("reason" -> reason), cause),
      parameters => QueryExecutionErrors.invalidNativeDataSourceLibraryError(
        file.getPath, "UNSUPPORTED_ABI_VERSION", parameters))
  }

  /**
   * Loads a library, and checks the version of the interface that it implements. Throws the error
   * if the library cannot be loaded, and returns it if the library is loaded but rejected.
   *
   * @param invalidLibrary returns the error for an invalid library, with the reason
   * @param unsupportedAbiVersion returns the error for an unsupported version of the interface,
   *                              with the parameters `version` and `supported`
   */
  private def load(
      file: File,
      invalidLibrary: (String, Throwable) => Throwable,
      unsupportedAbiVersion: Map[String, String] => Throwable): Try[NativeLibrary] = {
    logInfo(log"Loading the native data source library ${MDC(PATH, file)}.")
    val library = try {
      NativeLibrary.load(file.getPath)
    } catch {
      case e: UnsatisfiedLinkError => throw invalidLibrary(e.getMessage, e)
    }
    Try {
      val abiVersion = try {
        library.abiVersion()
      } catch {
        case e: UnsatisfiedLinkError =>
          throw invalidLibrary("It does not implement NativeBridge.abiVersion.", e)
      }
      if (abiVersion != NativeBridge.ABI_VERSION) {
        throw unsupportedAbiVersion(
          Map("version" -> abiVersion.toString, "supported" -> NativeBridge.ABI_VERSION.toString))
      }
      library
    }
  }

  /** Finds a copy of the package on this node: the one on the driver, or a distributed one. */
  private def locate(pkg: NativeDataSourcePackage): File = {
    (new File(pkg.path) +: NativeDataSourceRegistry.distributedCopies(pkg))
      .find { file =>
        file.isFile && NativeDataSourcePackage.tryRead(file).exists(_.sha256 == pkg.sha256)
      }
      .getOrElse {
        throw QueryExecutionErrors.nativeDataSourcePackageNotFoundError(pkg.fileName, pkg.sha256)
      }
  }

  /**
   * Finds an installed library on this node: where the driver found it, or else in the native
   * library path of this node.
   */
  private def locate(installed: InstalledNativeLibrary): File = {
    val file = new File(installed.path)
    if (file.isFile) {
      file
    } else {
      NativeDataSourceRegistry.findInstalledLibrary(file.getName).getOrElse {
        throw QueryExecutionErrors.nativeDataSourceLibraryNotFoundError(
          file.getName, installed.path)
      }
    }
  }

  private def extract(packageFile: File, libraryPath: String, target: File): Unit = {
    target.getParentFile.mkdirs()
    val temp = File.createTempFile(target.getName, ".tmp", target.getParentFile)
    try {
      Utils.tryWithResource(new ZipFile(packageFile)) { zip =>
        Utils.tryWithResource(zip.getInputStream(zip.getEntry(libraryPath))) { in =>
          Files.copy(in, temp.toPath, StandardCopyOption.REPLACE_EXISTING)
        }
      }
      Files.move(temp.toPath, target.toPath, StandardCopyOption.REPLACE_EXISTING)
    } finally {
      temp.delete()
    }
  }
}
