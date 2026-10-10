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

import java.io.{File, FileOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.zip.{ZipEntry, ZipOutputStream}

import org.apache.spark.util.Utils

/**
 * Builds the native data source library of the tests,
 * `native-datasource/test_native_datasource.cc`, with the C++ compiler of the machine, and
 * packages or installs it. The tests that load the library are skipped when [[toolchain]] is not
 * defined.
 */
object NativeDataSourceTestUtils {

  private lazy val buildDir = Utils.createTempDir(namePrefix = "native-datasource-test")

  private val libraries = new ConcurrentHashMap[String, File]()

  // The executors store the files of the sessions that are not isolated in the same directory, so
  // each package has its own name.
  private val packageCount = new AtomicInteger()

  /** The C++ compiler and the JNI include directories, if available. */
  lazy val toolchain: Option[(String, Seq[String])] = {
    val include = new File(System.getProperty("java.home"), "include")
    val platformInclude = Option(include.listFiles()).toSeq.flatten
      .find(dir => new File(dir, "jni_md.h").isFile)
    val compiler = Seq("c++", "g++", "clang++").find(Utils.checkCommandAvailable)
    for {
      compiler <- compiler
      platformInclude <- platformInclude
      if new File(include, "jni.h").isFile
    } yield (compiler, Seq(include.getPath, platformInclude.getPath))
  }

  /**
   * Compiles the test library with the given preprocessor definitions, once for each name. The
   * library implements the data sources [[dataSources]], with the prefix `NAME_PREFIX`.
   */
  def compile(name: String, defines: String*): File = {
    libraries.computeIfAbsent(name, _ => build(name, defines))
  }

  private def build(name: String, defines: Seq[String]): File = {
    val (compiler, includes) = toolchain.get
    val source = new File(buildDir, "test_native_datasource.cc")
    if (!source.exists()) {
      val in = getClass.getClassLoader
        .getResourceAsStream("native-datasource/test_native_datasource.cc")
      Utils.tryWithResource(in)(Files.copy(_, source.toPath))
    }
    val library = new File(buildDir, System.mapLibraryName(name))
    val command = Seq(compiler, "-std=c++17", "-O1", "-shared", "-fPIC") ++
      includes.map("-I" + _) ++ defines ++ Seq("-o", library.getPath, source.getPath)
    val process = new ProcessBuilder(command: _*).redirectErrorStream(true).start()
    val output = new String(process.getInputStream.readAllBytes(), UTF_8)
    assert(process.waitFor() == 0, s"Failed to compile the test library: $output")
    library
  }

  /** The data sources of the test library compiled with the given `NAME_PREFIX`. */
  def dataSources(prefix: String): Seq[String] =
    Seq("native_range", "native_sink", "native_counter").map(prefix + _)

  def manifestJson(
      dataSources: Seq[String],
      libraries: Map[String, String],
      abiVersion: Int = 1): String = {
    val names = dataSources.map(n => "\"" + n + "\"").mkString(", ")
    val paths = libraries.map { case (p, path) => "\"" + p + "\": \"" + path + "\"" }
      .mkString(", ")
    s"""{"abiVersion": $abiVersion, "dataSources": [$names], "libraries": {$paths}}"""
  }

  def createZip(dir: File, fileName: String, entries: (String, Array[Byte])*): File = {
    dir.mkdirs()
    val file = new File(dir, fileName)
    Utils.tryWithResource(new ZipOutputStream(new FileOutputStream(file))) { zip =>
      entries.foreach { case (path, bytes) =>
        zip.putNextEntry(new ZipEntry(path))
        zip.write(bytes)
        zip.closeEntry()
      }
    }
    file
  }

  /** Creates a package with the library built for this platform. */
  def createPackage(
      library: File,
      dataSources: Seq[String],
      dir: File = Utils.createTempDir(),
      fileName: String = s"test-${packageCount.incrementAndGet()}.sparkpkg",
      abiVersion: Int = 1,
      platform: String = NativePlatform.current): File = {
    val path = s"$platform/${library.getName}"
    createZip(
      dir,
      fileName,
      NativeDataSourcePackage.MANIFEST_NAME ->
        manifestJson(dataSources, Map(platform -> path), abiVersion).getBytes(UTF_8),
      path -> Files.readAllBytes(library.toPath))
  }

  /** Installs a library as the library of the given native data source, in a directory. */
  def installLibrary(library: File, dataSource: String, dir: File): File = {
    val file = new File(dir, NativeDataSourceRegistry.libraryFileName(dataSource))
    Files.copy(library.toPath, file.toPath)
    file
  }

  /** Runs `f` with a directory added to `java.library.path`, in the native library path. */
  def withLibraryPath[T](dir: File)(f: => T): T = {
    val original = System.getProperty("java.library.path")
    System.setProperty(
      "java.library.path", (dir.getPath +: Option(original).toSeq).mkString(File.pathSeparator))
    try {
      f
    } finally {
      if (original == null) {
        System.clearProperty("java.library.path")
      } else {
        System.setProperty("java.library.path", original)
      }
    }
  }
}
