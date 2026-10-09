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

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

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
    artifactUUID: Option[String] = None) {
  def fileName: String = new File(path).getName
}

object NativeDataSourcePackage {
  val FILE_EXTENSION: String = Artifact.NATIVE_DATA_SOURCE_PACKAGE_EXTENSION
  val MANIFEST_NAME = "spark-native-datasource.json"

  private case class CacheKey(path: String, length: Long, lastModified: Long)

  private val packages = new ConcurrentHashMap[CacheKey, NativeDataSourcePackage]()
  private val mapper = new ObjectMapper()

  /** Reads the package in the given file. The result is cached until the file changes. */
  def read(file: File): NativeDataSourcePackage = {
    val canonical = file.getCanonicalFile
    val key = CacheKey(canonical.getPath, canonical.length(), canonical.lastModified())
    packages.computeIfAbsent(key, _ => parse(canonical))
  }

  private def parse(file: File): NativeDataSourcePackage = {
    def invalid(reason: String, cause: Throwable = null): Throwable = {
      QueryExecutionErrors.invalidNativeDataSourcePackageError(
        file.getPath, "INVALID_MANIFEST", Map("reason" -> reason), cause)
    }
    val manifest = try {
      Utils.tryWithResource(new ZipFile(file)) { zip =>
        val entry = Option(zip.getEntry(MANIFEST_NAME)).getOrElse {
          throw invalid(s"The package does not contain the manifest $MANIFEST_NAME.")
        }
        val json = Utils.tryWithResource(zip.getInputStream(entry)) { in =>
          new String(in.readAllBytes(), StandardCharsets.UTF_8)
        }
        val manifest = parseManifest(json).fold(reason => throw invalid(reason), identity)
        manifest.libraries.values.find(zip.getEntry(_) == null).foreach { library =>
          throw invalid(s"The package does not contain the library $library.")
        }
        manifest
      }
    } catch {
      case e: IOException => throw invalid(s"The package is not a valid zip file: $e", e)
    }
    NativeDataSourcePackage(file.getPath, sha256(file), manifest)
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
 * The native data source libraries loaded in this JVM. A library is extracted from its package
 * and loaded once, on first use, and stays loaded until the JVM exits.
 */
object NativeLibraries extends Logging {
  private val libraries = new ConcurrentHashMap[String, NativeLibrary]()

  private lazy val extractionDir: File = Utils.createTempDir(namePrefix = "native-datasources")

  /** Returns the library of the given package, and loads it if needed. */
  private[ffi] def get(pkg: NativeDataSourcePackage): NativeLibrary = {
    val library = libraries.get(pkg.sha256)
    if (library != null) library else libraries.computeIfAbsent(pkg.sha256, _ => load(pkg))
  }

  private def load(pkg: NativeDataSourcePackage): NativeLibrary = {
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
    logInfo(log"Loading the native data source library ${MDC(PATH, libraryFile)}.")

    def invalidLibrary(reason: String, cause: Throwable): Throwable = {
      QueryExecutionErrors.invalidNativeDataSourcePackageError(
        pkg.path, "INVALID_LIBRARY", Map("library" -> libraryPath, "reason" -> reason), cause)
    }
    val library = try {
      NativeLibrary.load(libraryFile.getPath)
    } catch {
      case e: UnsatisfiedLinkError => throw invalidLibrary(e.getMessage, e)
    }
    val abiVersion = try {
      library.abiVersion()
    } catch {
      case e: UnsatisfiedLinkError =>
        throw invalidLibrary("It does not implement NativeBridge.abiVersion.", e)
    }
    if (abiVersion != NativeBridge.ABI_VERSION) {
      throw QueryExecutionErrors.invalidNativeDataSourcePackageError(
        pkg.path,
        "UNSUPPORTED_ABI_VERSION",
        Map("version" -> abiVersion.toString, "supported" -> NativeBridge.ABI_VERSION.toString))
    }
    library
  }

  /** Finds a copy of the package on this node: the one on the driver, or a distributed one. */
  private def locate(pkg: NativeDataSourcePackage): File = {
    (new File(pkg.path) +: NativeDataSourceRegistry.distributedCopies(pkg))
      .find(file => file.isFile && NativeDataSourcePackage.read(file).sha256 == pkg.sha256)
      .getOrElse {
        throw QueryExecutionErrors.nativeDataSourcePackageNotFoundError(pkg.fileName, pkg.sha256)
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
