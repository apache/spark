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
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogFunction, FunctionResource, PackageResource}
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.util.Utils

/**
 * Builds the native function library of the tests, `native-datasource/test_native_udf.cc`, with
 * the C++ compiler of the machine, as [[NativeDataSourceTestUtils]] builds the native data source
 * library, and packages it. The test library appends the calls that the header rules out to the
 * file of [[NativeDataSourceTestUtils.checkingOrderOfCalls]].
 */
object NativeUDFTestUtils {

  private lazy val buildDir = Utils.createTempDir(namePrefix = "native-udf-test")

  private val libraries = new ConcurrentHashMap[String, File]()

  private val packageCount = new AtomicInteger()

  /** The C++ compiler, and the include directories with both headers. */
  lazy val compiler: Option[(String, Seq[String])] = {
    NativeDataSourceTestUtils.compiler.map { case (compiler, includes) =>
      Seq("spark_udf.h", "test_native_udf.cc", "test_native_datasource.cc").foreach { name =>
        val resource = if (name.endsWith(".h")) s"org/apache/spark/sql/datasource/$name" else {
          s"native-datasource/$name"
        }
        Utils.tryWithResource(getClass.getClassLoader.getResourceAsStream(resource)) { in =>
          Files.copy(in, new File(buildDir, name).toPath)
        }
      }
      (compiler, includes :+ buildDir.getPath)
    }
  }

  /** [[compiler]], if available on a version of Java that can load native functions. */
  lazy val toolchain: Option[(String, Seq[String])] =
    compiler.filter(_ => NativeLibrary.isSupported)

  /**
   * Compiles `test_native_udf.cc` alone into a library, with the given preprocessor definitions,
   * once for each name. [[compileWithDataSources]] adds the native data source library of the
   * tests to it.
   */
  def compile(name: String, defines: String*): File = build(name, defines, withDataSources = false)

  def compileWithDataSources(name: String): File = build(name, Nil, withDataSources = true)

  private def build(name: String, defines: Seq[String], withDataSources: Boolean): File = {
    libraries.computeIfAbsent(name, _ => {
      val (compiler, includes) = toolchain.get
      val sources = Seq("test_native_udf.cc") ++
        (if (withDataSources) Seq("test_native_datasource.cc") else Nil)
      val library = new File(buildDir, System.mapLibraryName(name))
      run(Seq(compiler, "-std=c++17", "-O1", "-shared", "-fPIC", "-fvisibility=hidden") ++
        includes.map("-I" + _) ++ defines ++
        Seq(s"-DORDER_VIOLATIONS_PATH=\"${NativeDataSourceTestUtils.orderViolations.getPath}\"",
          "-o", library.getPath) ++
        sources.map(new File(buildDir, _).getPath))
      library
    })
  }

  /** Compiles a C++ program that includes `spark_udf.h`, runs it and returns its output. */
  def runProgram(name: String, source: String): String = {
    val (compiler, includes) = this.compiler.get
    val sourceFile = new File(buildDir, s"$name.cc")
    Files.writeString(sourceFile.toPath, source)
    val program = new File(buildDir, name)
    run(Seq(compiler, "-std=c++17") ++ includes.map("-I" + _) ++
      Seq("-o", program.getPath, sourceFile.getPath))
    run(Seq(program.getPath))
  }

  private def run(command: Seq[String]): String = {
    val process = new ProcessBuilder(command: _*).redirectErrorStream(true).start()
    val output = new String(process.getInputStream.readAllBytes(), UTF_8)
    assert(process.waitFor() == 0, s"Failed to run ${command.mkString(" ")}: $output")
    output
  }

  /** A function of a manifest, as JSON. */
  def function(
      name: String,
      arguments: Seq[String],
      returns: String,
      deterministic: Option[Boolean] = None): String = {
    val types = arguments.map(t => "\"" + t + "\"").mkString(", ")
    val flag = deterministic.map(d => s""", "deterministic": $d""").getOrElse("")
    s"""{"name": "$name", "kind": "scalar", "arguments": [$types], "returns": "$returns"$flag}"""
  }

  /** The functions of the test library, with their types, but for identity and describe. */
  val functions: Seq[String] = Seq(
    function("add_one", Seq("bigint"), "bigint"),
    function("half_if_even", Seq("bigint"), "bigint"),
    function("shout", Seq("string"), "string"),
    function("edit_distance", Seq("string", "string"), "int"),
    function("offset", Seq("bigint"), "bigint"),
    function("rows_before", Seq("bigint"), "bigint"),
    function("task_attempt_id", Nil, "bigint", deterministic = Some(false)),
    function("fail_on", Seq("bigint", "bigint"), "bigint"),
    function("fail_create", Seq("bigint"), "bigint"),
    function("null_state", Seq("bigint"), "bigint"),
    function("wait_for_interrupt", Seq("string"), "bigint"),
    function("bad_result", Seq("bigint", "string"), "bigint"),
    function("bad_nested_result", Seq("bigint", "string"), "array<bigint>"),
    function("bad_map_result", Seq("bigint", "string"), "map<bigint,bigint>"),
    function("keep_input", Seq("bigint"), "bigint"))

  /** A manifest, as JSON, without data sources unless `dataSources` is given. */
  def manifestJson(
      functions: Seq[String],
      libraries: Map[String, String],
      udfAbiVersion: Int = 1,
      dataSources: Option[Seq[String]] = None,
      abiVersion: Option[Int] = Some(1)): String = {
    val paths = libraries.map { case (p, path) => "\"" + p + "\": \"" + path + "\"" }
      .mkString(", ")
    val sources = dataSources.map { names =>
      s""""dataSources": [${names.map(n => "\"" + n + "\"").mkString(", ")}], """
    }.getOrElse("")
    val version = abiVersion.map(v => s""""abiVersion": $v, """).getOrElse("")
    s"""{$version$sources"udfAbiVersion": $udfAbiVersion, """ +
      s""""functions": [${functions.mkString(", ")}], "libraries": {$paths}}"""
  }

  /**
   * Registers add_one in the session, as CREATE TEMPORARY FUNCTION does, from a package whose
   * library Spark never loads, for the tests of plans: they need neither a C++ compiler nor Java
   * 22, since only the executors load a library, to evaluate a call.
   */
  def registerForPlans(session: SparkSession): Unit = {
    val file = NativeDataSourceTestUtils.createZip(
      Utils.createTempDir(),
      "plans.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(Seq(function("add_one",
        Seq("bigint"), "bigint")), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8),
      "lib.so" -> Array.emptyByteArray)
    val pkg = NativeDataSourcePackage.read(file)
    val addOne = pkg.manifest.functions.head.asInstanceOf[NativeFunctionSpec]
    session.sessionState.catalog.registerFunction(
      CatalogFunction(FunctionIdentifier(addOne.name), addOne.name,
        Seq(FunctionResource(PackageResource, file.getPath))),
      overrideIfExists = true,
      functionBuilder = Some(arguments => NativeUDF(addOne.name, addOne, pkg, arguments)))
  }

  /** Creates a package with the library built for this platform. */
  def createPackage(
      library: File,
      functions: Seq[String] = this.functions,
      dir: File = Utils.createTempDir(),
      fileName: String = s"udf-test-${packageCount.incrementAndGet()}.sparkpkg",
      udfAbiVersion: Int = 1,
      dataSources: Option[Seq[String]] = None,
      platform: String = NativePlatform.current,
      abiVersion: Option[Int] = Some(1)): File = {
    val path = s"$platform/${library.getName}"
    NativeDataSourceTestUtils.createZip(
      dir,
      fileName,
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        functions, Map(platform -> path), udfAbiVersion, dataSources, abiVersion).getBytes(UTF_8),
      path -> Files.readAllBytes(library.toPath))
  }
}
