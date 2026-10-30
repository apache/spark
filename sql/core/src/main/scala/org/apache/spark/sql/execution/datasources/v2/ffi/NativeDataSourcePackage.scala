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
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.nio.file.attribute.BasicFileAttributes
import java.security.MessageDigest
import java.util.{HexFormat, Locale}
import java.util.concurrent.ConcurrentHashMap
import java.util.zip.ZipFile

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.common.cache.{Cache, CacheBuilder}
import org.apache.arrow.c.jni.JniLoader

import org.apache.spark.SPARK_VERSION
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{PATH, VERSION_NUM}
import org.apache.spark.sql.Artifact
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
 * @param abiVersion the oldest version of the native data source interface that the libraries
 *                   implement. Spark uses a package whose version it supports, or that is older
 *                   than the newest version that it supports, and `spark_ds_init` chooses the
 *                   version of the table. Spark ignores the fields that it does not know.
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
 * @param distributedFileName the name of the copy of the package in the artifacts of the session,
 *                            if the package is not itself an artifact of the session
 */
case class NativeDataSourcePackage(
    path: String,
    sha256: String,
    manifest: NativeDataSourceManifest,
    artifactUUID: Option[String] = None,
    distributedFileName: Option[String] = None) extends NativeLibraryLocation {
  def fileName: String = new File(path).getName
}

/**
 * A package file that is not a valid native data source package.
 *
 * @param error the error that explains why the package is invalid
 * @param dataSources the data sources listed in the manifest of the package, if it can be read
 */
case class InvalidNativeDataSourcePackage(error: Throwable, dataSources: Seq[String])

/**
 * A version of a file, to cache what is read from it until it changes: the file is identified by
 * its canonical path, and its version by its length, its modification time and its file key, such
 * as the inode on Linux, which changes when another file replaces it, as `make install` does.
 */
private[ffi] case class FileVersion(path: String, length: Long, lastModified: Long, key: AnyRef)

private[ffi] object FileVersion {
  def apply(canonical: File): FileVersion = {
    val attributes = Files.readAttributes(canonical.toPath, classOf[BasicFileAttributes])
    FileVersion(canonical.getPath, attributes.size(), attributes.lastModifiedTime().toMillis,
      attributes.fileKey())
  }
}

object NativeDataSourcePackage extends Logging {
  val FILE_EXTENSION: String = Artifact.NATIVE_DATA_SOURCE_PACKAGE_EXTENSION
  val MANIFEST_NAME = "spark-native-datasource.json"

  private[ffi] type ReadResult = Either[InvalidNativeDataSourcePackage, NativeDataSourcePackage]

  // Bounded, because each session stores its artifacts in a directory of its own, so a
  // long-running server where many sessions add packages reads many different files.
  private val MAX_CACHED_PACKAGES = 1000

  private val packages: Cache[FileVersion, ReadResult] =
    CacheBuilder.newBuilder().maximumSize(MAX_CACHED_PACKAGES).build[FileVersion, ReadResult]()
  private val mapper = new ObjectMapper()

  /** Reads the package in the given file. The result is cached until the file changes. */
  def read(file: File): NativeDataSourcePackage = tryRead(file).fold(p => throw p.error, identity)

  /**
   * Checks a package that a session adds with `spark.addArtifact`, in the given file, before the
   * session gets it: fails if Spark cannot read the package, does not support its version of the
   * interface, or if it has no library for the platform of the driver, which loads the library to
   * plan a query. The package that a session added last takes precedence, and a session cannot
   * remove an artifact, so Spark rejects such a package rather than ignore it, as it ignores the
   * configured ones, which would keep the session on the package or library that it used before,
   * or add a package that fails every query on its data sources. The library is not loaded, so a
   * library that fails to load fails the queries that use it. The file is not the artifact
   * itself, so the result is not cached, and errors call it `path`.
   */
  def checkAdded(file: File, path: String): Unit = {
    val manifest = readManifest(file, path).fold(invalid => throw invalid.error, identity)
    if (!isSupported(manifest)) {
      throw unsupportedAbiVersionError(path, manifest)
    }
    if (!manifest.libraries.contains(NativePlatform.current)) {
      throw unsupportedPlatformError(path, manifest)
    }
  }

  /** Whether Spark supports the version of the interface of a package. */
  private[ffi] def isSupported(manifest: NativeDataSourceManifest): Boolean = {
    // The version of a package is the oldest that its library implements.
    manifest.abiVersion <= NativeLibrary.ABI_VERSION
  }

  /** The error for a package whose version of the interface Spark does not support. */
  private[ffi] def unsupportedAbiVersionError(
      path: String,
      manifest: NativeDataSourceManifest): Throwable = {
    QueryExecutionErrors.invalidNativeDataSourcePackageError(
      path,
      "UNSUPPORTED_ABI_VERSION",
      Map(
        "version" -> manifest.abiVersion.toString,
        "supported" -> NativeLibrary.ABI_VERSION.toString))
  }

  /** The error for a package without a library for the platform of this JVM. */
  private[ffi] def unsupportedPlatformError(
      path: String,
      manifest: NativeDataSourceManifest): Throwable = {
    QueryExecutionErrors.invalidNativeDataSourcePackageError(
      path,
      "UNSUPPORTED_PLATFORM",
      Map(
        "platform" -> NativePlatform.current,
        "platforms" -> manifest.libraries.keys.toSeq.sorted.mkString(", ")))
  }

  /**
   * Reads the package in the given file, or returns why it is invalid. The result is cached until
   * the file changes, and an invalid package, or a package of a version of the interface that
   * Spark does not support, is logged when it is read.
   */
  private[ffi] def tryRead(file: File): ReadResult = {
    val version = try {
      FileVersion(file.getCanonicalFile)
    } catch {
      case e: IOException =>
        return Left(InvalidNativeDataSourcePackage(
          invalidManifestError(file.getPath, s"The package cannot be read: $e", e), Nil))
    }
    Option(packages.getIfPresent(version)).getOrElse {
      val canonical = new File(version.path)
      val result = parse(canonical, canonical.getPath)
      result match {
        case Left(invalid) =>
          logWarning(log"Invalid native data source package ${MDC(PATH, canonical)}.",
            invalid.error)
        case Right(pkg) if !isSupported(pkg.manifest) =>
          logWarning(log"The native data source package ${MDC(PATH, canonical)} implements " +
            log"version ${MDC(VERSION_NUM, pkg.manifest.abiVersion)} of the native data source " +
            log"interface, which this version of Spark does not support, so it is not used.")
        case _ =>
      }
      packages.put(version, result)
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

  private def invalidManifestError(path: String, reason: String, cause: Throwable = null) = {
    QueryExecutionErrors.invalidNativeDataSourcePackageError(
      path, "INVALID_MANIFEST", Map("reason" -> reason), cause)
  }

  /** Reads the package in the given file, which errors call `path`. */
  private def parse(file: File, path: String): ReadResult = {
    readManifest(file, path).flatMap { manifest =>
      try {
        Right(NativeDataSourcePackage(path, sha256(file), manifest))
      } catch {
        case e: IOException => Left(InvalidNativeDataSourcePackage(
          invalidManifestError(path, s"The package cannot be read: $e", e), manifest.dataSources))
      }
    }
  }

  /**
   * Reads the manifest of the package in the given file, which errors call `path`, and checks
   * that the package contains the libraries that it lists. Only reads the central directory of
   * the zip file and the manifest.
   */
  private def readManifest(
      file: File,
      path: String): Either[InvalidNativeDataSourcePackage, NativeDataSourceManifest] = {
    def invalid(reason: String, dataSources: Seq[String] = Nil, cause: Throwable = null) = {
      Left(InvalidNativeDataSourcePackage(invalidManifestError(path, reason, cause), dataSources))
    }
    if (file.isDirectory) {
      // The error of ZipFile would name the file, which can be a copy of the one at `path`.
      return invalid("The package is a directory.")
    }
    try {
      Utils.tryWithResource(new ZipFile(file)) { zip =>
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
    if (abiVersion == null || !abiVersion.isInt || abiVersion.asInt() < 1) {
      return Left("The manifest must have a positive integer field 'abiVersion'.")
    }
    val dataSources = root.get("dataSources")
    if (dataSources == null || !dataSources.isArray || dataSources.isEmpty ||
        !dataSources.elements().asScala.forall { n =>
          n.isTextual && n.asText().nonEmpty && !n.asText().contains('\u0000')
        }) {
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

  private[ffi] def sha256(file: File): String = {
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
 * [[NativeDataSourceRegistry]] finds it by its file name. It must be installed on every node. The
 * SHA-256 checksum of the file identifies the build of the library, so that an executor can check
 * that it loads the same build as the driver: the state that the driver serializes is only
 * meant for the same build.
 *
 * @param path the path of the library on the driver
 * @param sha256 the SHA-256 checksum of the library on the driver
 */
case class InstalledNativeLibrary(path: String, sha256: String) extends NativeLibraryLocation

object InstalledNativeLibrary {
  // Bounded, like the cache of the packages, although few libraries are installed.
  private val checksums: Cache[FileVersion, String] =
    CacheBuilder.newBuilder().maximumSize(1000).build[FileVersion, String]()

  /** The installed library in the given file. */
  def apply(file: File): InstalledNativeLibrary = InstalledNativeLibrary(file.getPath, sha256(file))

  /** The SHA-256 checksum of the given file. The result is cached until the file changes. */
  private[ffi] def sha256(file: File): String = {
    val version = FileVersion(file.getCanonicalFile)
    Option(checksums.getIfPresent(version)).getOrElse {
      val checksum = NativeDataSourcePackage.sha256(new File(version.path))
      checksums.put(version, checksum)
      checksum
    }
  }
}

/** The platform names used in [[NativeDataSourceManifest.libraries]]. */
object NativePlatform {
  /**
   * The platform of this JVM, such as `linux-x86_64`, `linux-aarch64` or `osx-aarch64`. `linux` is
   * Linux with glibc: with musl, as on Alpine Linux, the platform ends with `-musl`, such as
   * `linux-x86_64-musl`, since a library built for one does not load with the other.
   */
  lazy val current: String = {
    val osName = System.getProperty("os.name")
    val linux = osName.toLowerCase(Locale.ROOT).startsWith("linux")
    name(osName, System.getProperty("os.arch"), musl = linux && usesMusl(processMaps()))
  }

  /** The libraries mapped in this process, as listed in `/proc/self/maps`, if it can be read. */
  private def processMaps(): Seq[String] = {
    try {
      Files.readAllLines(Paths.get("/proc/self/maps")).asScala.toSeq
    } catch {
      case _: IOException => Nil
    }
  }

  /** Whether the C library of a process is musl, which it maps as `ld-musl-<arch>.so.1`. */
  private[ffi] def usesMusl(maps: Seq[String]): Boolean = maps.exists(_.contains("/ld-musl-"))

  private[ffi] def name(osName: String, osArch: String, musl: Boolean = false): String = {
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
    if (os == "linux" && musl) s"$os-$arch-musl" else s"$os-$arch"
  }
}

/**
 * The native data source libraries loaded in this JVM. A library is loaded once, on first use,
 * after it is extracted from its package if needed, and stays loaded until the JVM exits.
 */
object NativeLibraries extends Logging {
  // The libraries of the packages, by the checksum of the package, and the installed libraries,
  // by their canonical path, with the checksum of the build that was loaded, so that each library
  // file is loaded once. A library that is loaded but rejected stays loaded, so its error is kept:
  // it is not loaded again.
  private val packagedLibraries = new ConcurrentHashMap[String, Try[NativeLibrary]]()
  private val installedLibraries = new ConcurrentHashMap[String, (String, Try[NativeLibrary])]()

  private lazy val extractionDir: File = Utils.createTempDir(namePrefix = "native-datasources")

  /** Returns the library at the given location, and loads it if needed. */
  private[ffi] def get(location: NativeLibraryLocation): NativeLibrary = {
    // The libraries are called with the FFM API, which is final only from Java 22.
    if (!NativeLibrary.isSupported) {
      throw QueryExecutionErrors.nativeDataSourceUnsupportedJavaVersionError(
        NativeLibrary.MINIMUM_JAVA_VERSION, Runtime.version().feature())
    }
    checkCDataInterface()
    getSupported(location)
  }

  /**
   * Fails unless the JNI library of the C data interface of Arrow Java, with which Spark exchanges
   * the data, loads on this platform. Arrow Java ships it for Linux with glibc and macOS on x86_64
   * and aarch64, and for Windows on x86_64; elsewhere, its own error does not say what it is for.
   */
  private[ffi] def checkCDataInterface(load: => Unit = JniLoader.get().ensureLoaded()): Unit = {
    try {
      load
    } catch {
      // Arrow Java does not have the library for the architecture, or the library does not link,
      // as with musl.
      case e @ (_: IllegalStateException | _: LinkageError) =>
        throw QueryExecutionErrors.nativeDataSourceUnsupportedPlatformError(
          NativePlatform.current, e)
    }
  }

  private def getSupported(location: NativeLibraryLocation): NativeLibrary = location match {
    case pkg: NativeDataSourcePackage =>
      getOrLoad(packagedLibraries, pkg.sha256)(loadPackaged(pkg)).get
    case installed: InstalledNativeLibrary =>
      // The file has the checksum of the build of the driver.
      val file = locate(installed).getCanonicalFile
      val (loadedChecksum, library) =
        getOrLoad(installedLibraries, file.getPath)(installed.sha256 -> loadInstalled(file))
      // The JVM cannot load another build from the same path: the library was replaced since.
      if (loadedChecksum != installed.sha256) {
        throw QueryExecutionErrors.invalidNativeDataSourceLibraryError(
          file.getPath,
          "REPLACED",
          Map("loadedChecksum" -> loadedChecksum, "checksum" -> installed.sha256))
      }
      library.get
  }

  private def getOrLoad[T](libraries: ConcurrentHashMap[String, T], key: String)(
      load: => T): T = {
    val library = libraries.get(key)
    if (library != null) library else libraries.computeIfAbsent(key, _ => load)
  }

  private def loadPackaged(pkg: NativeDataSourcePackage): Try[NativeLibrary] = {
    val platform = NativePlatform.current
    val libraryPath = pkg.manifest.libraries.getOrElse(platform, {
      throw NativeDataSourcePackage.unsupportedPlatformError(pkg.path, pkg.manifest)
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
   * if the library cannot be loaded, and returns it if the library is loaded but rejected, so that
   * it is kept: the library stays loaded, and Spark only initializes it once.
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
      NativeLibrary.load(file.getPath, SPARK_VERSION)
    } catch {
      // A library that was loaded stays loaded, and Spark must not initialize it again.
      case e: NativeLibrary.LoadException if e.loaded =>
        return Failure(invalidLibrary(e.getMessage, e))
      case e: NativeLibrary.LoadException => throw invalidLibrary(e.getMessage, e)
    }
    Try {
      if (library.abiVersion() != NativeLibrary.ABI_VERSION) {
        throw unsupportedAbiVersion(Map(
          "version" -> library.abiVersion().toString,
          "supported" -> NativeLibrary.ABI_VERSION.toString))
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
   * Finds an installed library on this node with the same checksum as on the driver: where the
   * driver found it, or else in the native library path of this node.
   */
  private def locate(installed: InstalledNativeLibrary): File = {
    val file = new File(installed.path)
    (file +: NativeDataSourceRegistry.findInstalledLibraries(file.getName))
      .find(f => f.isFile && InstalledNativeLibrary.sha256(f) == installed.sha256)
      .getOrElse {
        throw QueryExecutionErrors.nativeDataSourceLibraryNotFoundError(
          file.getName, installed.sha256, installed.path)
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
