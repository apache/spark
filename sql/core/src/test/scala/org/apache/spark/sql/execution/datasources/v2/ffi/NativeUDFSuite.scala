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
import java.math.{BigDecimal => JBigDecimal}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files
import java.sql.{Date, Timestamp}
import java.time.LocalDateTime
import java.util.{Locale, Properties, UUID}
import java.util.concurrent.{Callable, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.arrow.c.{ArrowArray, ArrowSchema, Data}
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.apache.logging.log4j.Level
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SPARK_DOC_ROOT, SparkConf, SparkRuntimeException, SparkThrowable, SparkUnsupportedOperationException, TaskContext, TaskContextImpl, TestUtils}
import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.internal.config.BUFFER_PAGESIZE
import org.apache.spark.memory.{TaskMemoryManager, TestMemoryConsumer, TestMemoryManager}
import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, InternalRow}
import org.apache.spark.sql.catalyst.expressions.{Alias, ArrayTransform, BloomFilterMightContain, LambdaFunction, Literal, NamedLambdaVariable}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, EvalNativeUDFs, Join, OneRowRelation, Project}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.FakeV2ProviderWithCustomSchema
import org.apache.spark.sql.connector.catalog.{InMemoryEnhancedPartitionFilterTable, InMemoryTableEnhancedPartitionFilterCatalog}
import org.apache.spark.sql.execution.LocalLimitExec
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.python.{ArrowEvalPythonExec, BatchEvalPythonExec}
import org.apache.spark.sql.functions.{call_function, col}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Tests native functions with the library in `native-datasource/test_native_udf.cc`, compiled
 * with the C++ compiler of the machine by [[NativeUDFTestUtils]]. The tests that load a library
 * are skipped when there is no C++ compiler, or before Java 22.
 */
class NativeUDFSuite extends QueryTest with SharedSparkSession {
  import NativeDataSourceTestUtils.{checkingOrderOfCalls, createZip}
  import NativeUDFTestUtils._

  // Jobs outside of SQL executions get the files of all the sessions, so the sessions are kept
  // until the end of the suite, as in NativeDataSourceSuite.
  private val sessions = ArrayBuffer.empty[SparkSession]

  private lazy val library = compile("test_native_udf")

  private lazy val defaultPackage = createPackage(library)

  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and Java 22 or later are required.")
    try checkingOrderOfCalls(f) finally SparkSession.setActiveSession(spark)
  }

  private def nativeGridTest[A](name: String)(params: Seq[A])(f: A => Unit): Unit = {
    gridTest(name)(params) { param =>
      assume(toolchain.isDefined, "A C++ compiler and Java 22 or later are required.")
      try checkingOrderOfCalls(f(param)) finally SparkSession.setActiveSession(spark)
    }
  }

  /** A new active session, with the given packages added as artifacts. */
  private def newSession(packages: File*): SparkSession = {
    val session = spark.newSession()
    SparkSession.setActiveSession(session)
    sessions += session
    packages.foreach(pkg => session.addArtifact(pkg.getPath))
    session
  }

  /** Registers functions of the test library under their names in the manifest. */
  private def register(session: SparkSession, pkg: String, names: String*): Unit = {
    names.foreach { name =>
      session.sql(s"CREATE TEMPORARY FUNCTION $name AS '$name' USING PACKAGE '$pkg'")
    }
  }

  // Checks the answer without converting the DataFrame to an RDD: a job outside of a SQL
  // execution gets the files of all the sessions.
  private def checkRows(df: => DataFrame, expected: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expected, checkToRDD = false)
  }

  private def findError(e: Throwable, condition: String): SparkThrowable = {
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null)
      .collectFirst { case t: SparkThrowable if t.getCondition == condition => t }
      .getOrElse(throw e)
  }

  private def evaluations(df: DataFrame): Seq[EvalNativeUDFsExec] = {
    df.queryExecution.executedPlan.collect { case e: EvalNativeUDFsExec => e }
  }

  test("the bindings match the layout of spark_udf.h") {
    assume(compiler.isDefined, "A C++ compiler is required.")
    val layout = NativeUdfLibrary.layout().asScala.map { case (key, value) => key -> value.toLong }
    val constants = Map(
      "SPARK_UDF_ABI_VERSION" -> NativeUdfLibrary.ABI_VERSION.toLong,
      "SPARK_UDF_SCALAR" -> NativeUdfLibrary.SCALAR.toLong)
    val prints = layout.keys.map { key =>
      val value = key.split('.') match {
        case Array(struct) => s"sizeof($struct)"
        case Array(struct, field) => s"offsetof($struct, $field)"
      }
      s"""  std::printf("%s %zu\\n", "$key", $value);"""
    } ++ constants.keys.map { name =>
      s"""  std::printf("%s %lld\\n", "$name", static_cast<long long>($name));"""
    }
    val output = runProgram("udf_layout",
      s"""#include <cstddef>
         |#include <cstdio>
         |#include "spark_udf.h"
         |
         |int main() {
         |${prints.mkString("\n")}
         |  return 0;
         |}
         |""".stripMargin)
    val actual = output.linesIterator.map { line =>
      val Array(key, value) = line.split(" ")
      key -> value.toLong
    }.toMap
    assert(actual == layout.toMap ++ constants)
  }

  test("native functions require Java 22 or later") {
    assume(!NativeLibrary.isSupported, "The Java version supports native functions.")
    // Not loaded: Spark fails before it loads the library.
    val pkg = createZip(Utils.createTempDir(), "old_java.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        Seq(function("f", Nil, "int")), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8),
      "lib.so" -> "unused".getBytes(UTF_8))
    val parameters = Map("required" -> "22", "current" -> Runtime.version().feature().toString)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        newSession().sql(s"CREATE TEMPORARY FUNCTION f AS 'f' USING PACKAGE '${pkg.getPath}'")
      },
      condition = "NATIVE_UDF_UNSUPPORTED_JAVA_VERSION",
      parameters = parameters)
    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        NativeLibraries.getFunctionLibrary(NativeDataSourcePackage.read(pkg))
      },
      condition = "NATIVE_UDF_UNSUPPORTED_JAVA_VERSION",
      parameters = parameters)
  }

  test("fail clearly where the C data interface of Arrow Java does not load, for native " +
      "functions") {
    assume(NativeLibrary.isSupported, "Native functions require Java 22 or later.")
    val pkg = NativeDataSourcePackage(
      "unused.sparkpkg", "", NativeDataSourceManifest(Some(1), Nil, Map.empty))
    val cause = new UnsatisfiedLinkError("libarrow_cdata_jni.so: symbol not found")
    val e = intercept[SparkUnsupportedOperationException] {
      NativeLibraries.getFunctionLibrary(pkg, loadCDataInterface = throw cause)
    }
    checkError(
      exception = e,
      condition = "NATIVE_UDF_UNSUPPORTED_PLATFORM",
      parameters = Map("platform" -> NativePlatform.current, "reason" -> cause.toString))
    assert(e.getCause eq cause)
  }

  test("read the functions of a manifest") {
    val json = manifestJson(
      Seq(
        function("Add_One", Seq("bigint"), "bigint"),
        function("pairs", Seq("array<string>", "struct<a: int NOT NULL, b: binary>"),
          "map<string,decimal(10,2)>", deterministic = Some(false)),
        """{"name": "sum_all", "kind": "aggregate", "arguments": ["a type of later"]}"""),
      Map("linux-x86_64" -> "lib.so"))
    assert(NativeDataSourcePackage.parseManifest(json) == Right(NativeDataSourceManifest(
      Some(1), Nil, Map("linux-x86_64" -> "lib.so"), Some(1), Seq(
        NativeFunctionSpec("Add_One", "scalar", Seq(LongType), LongType, deterministic = true),
        NativeFunctionSpec("pairs", "scalar",
          Seq(ArrayType(StringType), StructType(Seq(
            StructField("a", IntegerType, nullable = false), StructField("b", BinaryType)))),
          MapType(StringType, DecimalType(10, 2)), deterministic = false),
        UnsupportedNativeFunction("sum_all", "aggregate")))))
  }

  test("read the types of a manifest the same way in every session") {
    val file = createZip(Utils.createTempDir(), "timestamps.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        Seq(function("ts", Seq("timestamp", "timestamp_ntz"), "timestamp")),
        Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8),
      "lib.so" -> Array.emptyByteArray)
    def types(session: SparkSession): (Seq[DataType], DataType) = session.withActive {
      val ts = NativeDataSourcePackage.read(file).manifest.functions.head
        .asInstanceOf[NativeFunctionSpec]
      (ts.arguments, ts.returnType)
    }
    val ntz = spark.newSession()
    ntz.conf.set(SQLConf.TIMESTAMP_TYPE.key, "TIMESTAMP_NTZ")
    val expected = (Seq(TimestampType, TimestampNTZType), TimestampType)
    assert(types(ntz) == expected)
    assert(types(spark.newSession()) == expected)
  }

  private val libraries = """"libraries": {"linux-x86_64": "lib.so"}"""
  private val addOne = function("add_one", Seq("bigint"), "bigint")

  gridTest("accept a manifest with")(Seq(
    Case("functions and no data sources",
      s""""dataSources": [], "udfAbiVersion": 1, "functions": [$addOne]"""),
    Case("functions and no field of data sources",
      s""""udfAbiVersion": 1, "functions": [$addOne]"""),
    Case("functions and data sources",
      s""""dataSources": ["a"], "udfAbiVersion": 1, "functions": [$addOne]"""),
    Case("data sources and no functions", s""""dataSources": ["a"], "functions": []"""),
    // The version of the functions of a manifest without functions is not used.
    Case("data sources and a version of no functions",
      s""""dataSources": ["a"], "udfAbiVersion": 0"""),
    Case("struct fields whose names differ only in case",
      s""""udfAbiVersion": 1, "functions": [${function("f", Nil, "struct<a: int, A: int>")}]"""))) {
    case Case(_, fields) =>
    assert(NativeDataSourcePackage.parseManifest(
      s"""{"abiVersion": 1, $fields, $libraries}""").isRight)
  }

  private val noDataSources = "The manifest must have a field 'dataSources' with a non-empty " +
    "array of data source names."

  /** A case of a test, named by its label. */
  private case class Case[T](label: String, value: T) {
    override def toString: String = label
  }

  gridTest("reject a manifest whose functions are invalid")(Seq(
    Case("no data sources", "\"dataSources\": []" -> noDataSources),
    Case("no data sources nor functions",
      "\"dataSources\": [], \"functions\": []" -> noDataSources),
    Case("functions not an array", s""""udfAbiVersion": 1, "functions": {}""" ->
      "The field 'functions' of the manifest must be an array of functions."),
    Case("no version", s""""functions": [$addOne]""" ->
      "A manifest with functions must have a positive integer field 'udfAbiVersion'."),
    Case("version 0", s""""udfAbiVersion": 0, "functions": [$addOne]""" ->
      "A manifest with functions must have a positive integer field 'udfAbiVersion'."),
    Case("function not an object", """"udfAbiVersion": 1, "functions": ["add_one"]""" ->
      "Each function of the manifest must be a JSON object."),
    Case("no name", """"udfAbiVersion": 1, "functions": [{"kind": "scalar"}]""" ->
      ("Each function of the manifest must have a field 'name' with a non-empty name without " +
        "the character U+0000.")),
    Case("U+0000 in a name",
      s""""udfAbiVersion": 1, "functions": [${function("a\\u0000b", Nil, "int")}]""" ->
      ("Each function of the manifest must have a field 'name' with a non-empty name without " +
        "the character U+0000.")),
    Case("duplicate names",
      s""""udfAbiVersion": 1, "functions": [$addOne, ${function("ADD_ONE", Nil, "int")}]""" ->
      "The manifest lists the function add_one more than once."),
    Case("no kind",
      """"udfAbiVersion": 1, "functions": [{"name": "f", "arguments": [], "returns": "int"}]""" ->
      "The function f of the manifest must have a field 'kind'."),
    Case("duplicate names of different kinds", """"udfAbiVersion": 1, "functions": [
      |{"name": "f", "kind": "scalar", "arguments": [], "returns": "int"},
      |{"name": "F", "kind": "aggregate"}]""".stripMargin ->
      "The manifest lists the function f more than once."),
    Case("arguments not strings", """"udfAbiVersion": 1, "functions": [{"name": "f",
      |"kind": "scalar", "arguments": [1], "returns": "int"}]""".stripMargin ->
      ("The function f of the manifest must have a field 'arguments' with an array of the " +
        "types of its arguments.")),
    Case("no result", """"udfAbiVersion": 1, "functions": [{"name": "f", "kind": "scalar",
      |"arguments": []}]""".stripMargin ->
      "The function f of the manifest must have a field 'returns' with the type of its result."),
    Case("deterministic not a boolean", """"udfAbiVersion": 1, "functions": [{"name": "f",
      |"kind": "scalar", "arguments": [], "returns": "int", "deterministic": "no"}]"""
      .stripMargin ->
      "The function f of the manifest must have a boolean in its field 'deterministic'."),
    Case("interval argument",
      s""""udfAbiVersion": 1, "functions": [${function("f", Seq("interval day"), "int")}]""" ->
      ("The function f of the manifest has INTERVAL DAY as the type of its argument 1, which " +
        "contains the type INTERVAL DAY: native functions do not support it.")),
    Case("variant in the result",
      s""""udfAbiVersion": 1, "functions": [${function("f", Nil, "array<variant>")}]""" ->
      ("The function f of the manifest has ARRAY<VARIANT> as the type of its result, which " +
        "contains the type VARIANT: native functions do not support it.")),
    Case("char argument",
      s""""udfAbiVersion": 1, "functions": [${function("f", Seq("char(3)"), "int")}]""" ->
      ("The function f of the manifest has CHAR(3) as the type of its argument 1, which " +
        "contains the type CHAR(3): native functions do not support it.")),
    Case("U+0000 in a field name", s""""udfAbiVersion": 1,
      |"functions": [${function("f", Nil, "struct<`a\\u0000`: int>")}]""".stripMargin ->
      ("The function f of the manifest has STRUCT<`a\\u0000`: INT> as the type of its result, " +
        "with a field whose name contains the character U+0000, which Spark cannot pass to a " +
        "library.")),
    Case("duplicate field names", s""""udfAbiVersion": 1,
      |"functions": [${function("f", Nil, "struct<a: int, a: string>")}]""".stripMargin ->
      ("The function f of the manifest has STRUCT<a: INT, a: STRING> as the type of its result, " +
        "with more than one field named a in a struct, which Spark cannot pass to a library.")),
    Case("duplicate field names in a nested struct", s""""udfAbiVersion": 1, "functions": [
      |${function("f", Seq("array<map<string,struct<s: struct<b: int, b: int>>>>"), "int")}]"""
      .stripMargin ->
      ("The function f of the manifest has ARRAY<MAP<STRING, STRUCT<s: STRUCT<b: INT, b: INT>>>> " +
        "as the type of its argument 1, with more than one field named b in a struct, which " +
        "Spark cannot pass to a library.")))) { case Case(_, (fields, reason)) =>
    assert(NativeDataSourcePackage.parseManifest(
      s"""{"abiVersion": 1, $fields, $libraries}""") == Left(reason))
  }

  private val noVersion = "The manifest must have a positive integer field 'abiVersion'."

  gridTest("require the field abiVersion unless a manifest lists only functions")(Seq(
    Case("only functions, no field of data sources",
      s""""udfAbiVersion": 1, "functions": [$addOne]""" -> Right(None)),
    Case("only functions, no data sources",
      s""""dataSources": [], "udfAbiVersion": 1, "functions": [$addOne]""" -> Right(None)),
    Case("only functions, a null version",
      s""""abiVersion": null, "udfAbiVersion": 1, "functions": [$addOne]""" -> Left(noVersion)),
    Case("functions and data sources",
      s""""dataSources": ["a"], "udfAbiVersion": 1, "functions": [$addOne]""" -> Left(noVersion)),
    Case("an empty field of functions", """"functions": []""" -> Left(noVersion)),
    Case("no field of functions", """"dataSources": []""" -> Left(noVersion)),
    Case("only functions, an invalid one", """"udfAbiVersion": 1, "functions": ["f"]""" ->
      Left("Each function of the manifest must be a JSON object.")))) {
    case Case(_, (fields, expected)) =>
      assert(NativeDataSourcePackage.parseManifest(s"""{$fields, $libraries}""")
        .map(_.abiVersion) == expected)
  }

  gridTest("reject an invalid type in a manifest")(Seq("no_such_type", "decimal(40,2)")) { ddl =>
    val reason = NativeDataSourcePackage.parseManifest(
      s"""{"abiVersion": 1, "udfAbiVersion": 1,
         |"functions": [${function("f", Seq(ddl), "int")}], $libraries}"""
        .stripMargin).swap.toOption.get
    assert(reason.startsWith(s"The function f of the manifest has $ddl as the type of its " +
      "argument 1, which is not a valid type: "), reason)
  }

  gridTest("report the version of the functions of a later version when a session adds their " +
      "package, whatever they declare")(Seq(
    Case("a type of that version", Seq(function("f", Seq("time"), "int"))),
    Case("no kind", Seq("""{"name": "f", "arguments": "time"}""")),
    Case("duplicate names",
      Seq(function("f", Seq("time"), "int"), """{"name": "F", "kind": "table"}""")),
    Case("U+0000 in a name", Seq(function("a\\u0000b", Nil, "int"))),
    Case("a function that is not an object", Seq("\"f\"")))) { case Case(_, functions) =>
    val pkg = createZip(Utils.createTempDir(), "later.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(functions,
        Map(NativePlatform.current -> "lib.so"), udfAbiVersion = 2).getBytes(UTF_8),
      "lib.so" -> "unused".getBytes(UTF_8))
    checkError(
      exception = intercept[SparkRuntimeException](spark.newSession().addArtifact(pkg.getPath)),
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_UDF_ABI_VERSION",
      parameters = Map("path" -> pkg.getPath, "version" -> "2", "supported" -> "1"))
  }

  nativeTest("check a package with functions when a session adds it") {
    // A package with functions only, which Spark accepts.
    newSession(defaultPackage)
    Seq(None, Some(Nil)).foreach { dataSources =>
      val unversioned = createPackage(library, dataSources = dataSources, abiVersion = None)
      val accepted = newSession(unversioned)
      register(accepted, unversioned.getName, "add_one")
      checkRows(accepted.sql("SELECT add_one(1)"), Seq(Row(2L)))
    }
    val session = newSession()
    Seq(
      createPackage(library, dataSources = Some(Seq("unversioned")), abiVersion = None),
      createPackage(library, abiVersion = Some(0))).foreach { pkg =>
      checkError(
        exception = intercept[SparkRuntimeException](session.addArtifact(pkg.getPath)),
        condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.INVALID_MANIFEST",
        parameters = Map("path" -> pkg.getPath,
          "reason" -> "The manifest must have a positive integer field 'abiVersion'."))
    }
    val newerVersion = createPackage(library, abiVersion = Some(2))
    checkError(
      exception = intercept[SparkRuntimeException](session.addArtifact(newerVersion.getPath)),
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_ABI_VERSION",
      parameters = Map("path" -> newerVersion.getPath, "version" -> "2", "supported" -> "1"))
    val newer = createPackage(library, udfAbiVersion = 2)
    checkError(
      exception = intercept[SparkRuntimeException](session.addArtifact(newer.getPath)),
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_UDF_ABI_VERSION",
      parameters = Map("path" -> newer.getPath, "version" -> "2", "supported" -> "1"))
    val invalid = createPackage(library, functions = Seq(function("f", Nil, "interval day")))
    checkError(
      exception = intercept[SparkRuntimeException](session.addArtifact(invalid.getPath)),
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.INVALID_MANIFEST",
      parameters = Map("path" -> invalid.getPath, "reason" -> ("The function f of the manifest " +
        "has INTERVAL DAY as the type of its result, which contains the type INTERVAL DAY: " +
        "native functions do not support it.")))
  }

  nativeTest("serve the data sources of a configured package of a later version of functions") {
    val both = compileWithDataSources("test_native_udf_with_data_sources")
    val pkg = createPackage(both, functions = Seq("""{"name": "f", "arguments": ["time"]}"""),
      udfAbiVersion = 2, dataSources = Some(NativeDataSourceTestUtils.dataSources("")))
    val session = newSession()
    session.conf.set(SQLConf.NATIVE_DATA_SOURCE_PATHS.key, pkg.getParent)
    checkRows(session.read.format("native_range").option("end", "3").load().select("id"),
      Seq(Row(0L), Row(1L), Row(2L)))
    checkError(
      exception = intercept[SparkRuntimeException](register(session, pkg.getName, "f")),
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_UDF_ABI_VERSION",
      parameters = Map("path" -> pkg.getCanonicalPath, "version" -> "2", "supported" -> "1"))
  }

  nativeTest("serve the data sources of a configured package next to one with an invalid type") {
    val dir = Utils.createTempDir()
    createPackage(compileWithDataSources("test_native_udf_with_data_sources"), dir = dir,
      dataSources = Some(NativeDataSourceTestUtils.dataSources("")))
    createPackage(library, functions = Seq(function("f", Seq("decimal(40,2)"), "int")), dir = dir)
    val session = newSession()
    session.conf.set(SQLConf.NATIVE_DATA_SOURCE_PATHS.key, dir.getPath)
    checkRows(session.read.format("native_range").option("end", "3").load().select("id"),
      Seq(Row(0L), Row(1L), Row(2L)))
  }

  nativeGridTest("register a function of a package given by")(
      Seq("its artifact", "its configured file", "its path", "its URI", "its HTTP URL")) { how =>
    val pkg = createPackage(library)
    val session = newSession()
    // The name of the function in the manifest is case-insensitive.
    def create(reference: String): Unit = {
      session.sql(s"CREATE TEMPORARY FUNCTION plus_one AS 'ADD_one' USING PACKAGE '$reference'")
    }
    how match {
      case "its artifact" =>
        session.addArtifact(pkg.getPath)
        create(pkg.getName)
      case "its configured file" =>
        session.conf.set(SQLConf.NATIVE_DATA_SOURCE_PATHS.key, pkg.getParent)
        create(pkg.getName)
      case "its path" => create(pkg.getPath)
      case "its URI" => create(pkg.toURI.toString)
      // Fetched when the function is registered.
      case "its HTTP URL" =>
        TestUtils.withHttpServer(pkg.getParent)(url => create(s"$url${pkg.getName}"))
    }
    checkRows(session.sql("SELECT plus_one(id) FROM range(3)"), Seq(Row(1L), Row(2L), Row(3L)))
  }

  nativeTest("reject the registrations of functions that Spark cannot run") {
    val session = newSession(defaultPackage)
    def create(sql: String): Throwable = intercept[Throwable](session.sql(sql))
    checkError(
      exception = create("CREATE TEMPORARY FUNCTION f AS 'add_one' USING PACKAGE 'none.sparkpkg'")
        .asInstanceOf[AnalysisException],
      condition = "NATIVE_UDF_PACKAGE_NOT_FOUND",
      parameters = Map("package" -> "none.sparkpkg", "functionName" -> "`f`"))
    checkError(
      exception = create(
        s"CREATE TEMPORARY FUNCTION f AS 'subtract' USING PACKAGE '${defaultPackage.getName}'")
        .asInstanceOf[AnalysisException],
      condition = "NATIVE_UDF_NOT_FOUND",
      parameters = Map("package" -> defaultPackage.getName, "name" -> "subtract",
        "functions" -> functions.map(f => f.split('"')(3)).mkString(", ")))
    val elsewhere = createPackage(library, platform = "other-platform")
    checkError(
      exception = create(
        s"CREATE TEMPORARY FUNCTION f AS 'add_one' USING PACKAGE '${elsewhere.getPath}'")
        .asInstanceOf[SparkThrowable],
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_PLATFORM",
      parameters = Map("path" -> elsewhere.getCanonicalPath, "platform" -> NativePlatform.current,
        "platforms" -> "other-platform"))
    val newer = createPackage(library, udfAbiVersion = 2)
    checkError(
      exception = create(
        s"CREATE TEMPORARY FUNCTION f AS 'add_one' USING PACKAGE '${newer.getPath}'")
        .asInstanceOf[SparkThrowable],
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_UDF_ABI_VERSION",
      parameters = Map("path" -> newer.getCanonicalPath, "version" -> "2", "supported" -> "1"))
    checkError(
      exception = create(
        s"CREATE FUNCTION f AS 'add_one' USING PACKAGE '${defaultPackage.getName}'")
        .asInstanceOf[AnalysisException],
      condition = "UNSUPPORTED_FEATURE.PERSISTENT_NATIVE_UDF",
      parameters = Map("functionName" -> "`spark_catalog`.`default`.`f`"))
    val withJar = "CREATE TEMPORARY FUNCTION f AS 'add_one' USING PACKAGE 'p.sparkpkg', JAR 'f.jar'"
    checkError(
      exception = create(withJar).asInstanceOf[ParseException],
      condition = "_LEGACY_ERROR_TEMP_0035",
      parameters = Map("message" -> "CREATE FUNCTION with a package and other resources"),
      context = ExpectedContext(withJar, 0, withJar.length - 1))
  }

  nativeTest("register the functions of a package that lists a function of another kind") {
    val aggregate =
      """{"name": "sum_all", "kind": "aggregate", "arguments": ["a type of later"], "merge": 1}"""
    val pkg = createPackage(library, functions = Seq(addOne, aggregate))
    val session = newSession(pkg)
    register(session, pkg.getName, "add_one")
    checkRows(session.sql("SELECT add_one(1)"), Seq(Row(2L)))
    checkError(
      exception = intercept[AnalysisException](register(session, pkg.getName, "sum_all")),
      condition = "NATIVE_UDF_UNSUPPORTED_KIND",
      parameters = Map("package" -> pkg.getName, "name" -> "sum_all", "kind" -> "aggregate",
        "kinds" -> "scalar"))
  }

  nativeTest("reject the registrations when native functions are disabled") {
    val session = newSession(defaultPackage)
    // A static configuration, which a session cannot set otherwise.
    session.sessionState.conf.setConf(StaticSQLConf.NATIVE_UDF_ENABLED, false)
    checkError(
      exception = intercept[AnalysisException] {
        session.sql(
          s"CREATE TEMPORARY FUNCTION f AS 'add_one' USING PACKAGE '${defaultPackage.getName}'")
      },
      condition = "NATIVE_UDF_DISABLED",
      parameters = Map("functionName" -> "`f`", "config" -> "\"spark.sql.udf.native.enabled\""))
  }

  nativeTest("replace a native function") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one")
    checkError(
      exception = intercept[AnalysisException] {
        register(session, defaultPackage.getName, "add_one")
      },
      condition = "ROUTINE_ALREADY_EXISTS",
      parameters = Map("routineName" -> "`add_one`", "newRoutineType" -> "routine",
        "existingRoutineType" -> "routine"))
    session.sql("CREATE OR REPLACE TEMPORARY FUNCTION add_one AS 'half_if_even' USING PACKAGE " +
      s"'${defaultPackage.getName}'")
    checkRows(session.sql("SELECT add_one(4)"), Seq(Row(2L)))
    session.sql("DROP TEMPORARY FUNCTION add_one")
    assert(!session.catalog.functionExists("add_one"))
  }

  nativeTest("check the arguments of a call against the types of the manifest") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one", "edit_distance")
    checkRows(session.sql("SELECT add_one(CAST(1 AS INT)), edit_distance('ab', 'b')"),
      Seq(Row(2L, 1)))
    checkError(
      exception = intercept[AnalysisException](session.sql("SELECT add_one(1, 2)")),
      condition = "WRONG_NUM_ARGS.WITHOUT_SUGGESTION",
      parameters = Map("functionName" -> "`add_one`", "expectedNum" -> "1", "actualNum" -> "2",
        "docroot" -> SPARK_DOC_ROOT),
      context = ExpectedContext("add_one(1, 2)", 7, 19))
    checkError(
      exception = intercept[AnalysisException](session.sql("SELECT add_one(x => 1)")),
      condition = "NAMED_PARAMETERS_NOT_SUPPORTED",
      parameters = Map("functionName" -> "`add_one`"),
      context = ExpectedContext("add_one(x => 1)", 7, 21))
    checkError(
      exception = intercept[AnalysisException](session.sql("SELECT add_one(array(1))")),
      condition = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
      parameters = Map("sqlExpr" -> "\"add_one(array(1))\"", "paramIndex" -> "first",
        "inputSql" -> "\"array(1)\"", "inputType" -> "\"ARRAY<INT>\"",
        "requiredType" -> "\"BIGINT\""),
      context = ExpectedContext("add_one(array(1))", 7, 23))
  }

  nativeTest("call native functions from SQL and with call_function") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one", "half_if_even", "shout", "edit_distance")
    // scalastyle:off nonascii
    val df = session.sql(
      "SELECT * FROM VALUES (1L, 'kitten', 'sitting'), (4L, '\u00e9lan', 'elan'), " +
        "(NULL, NULL, 'x') AS t(id, a, b)")
    val expected = Seq(
      Row(2L, null, "KITTEN!", 3), Row(5L, 2L, "\u00e9LAN!", 1), Row(null, null, null, null))
    // scalastyle:on nonascii
    checkRows(
      df.selectExpr("add_one(id)", "half_if_even(id)", "shout(a)", "edit_distance(a, b)"),
      expected)
    checkRows(
      df.select(call_function("add_one", col("id")), call_function("half_if_even", col("id")),
        call_function("shout", col("a")), call_function("edit_distance", col("a"), col("b"))),
      expected)
  }

  nativeTest("read back the columns of the child when the calls take them in another order") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one", "edit_distance")
    // scalastyle:off nonascii
    val df = session.sql(
      "SELECT * FROM VALUES (1L, 'kitten', 'sitting'), (4L, '\u00e9lan', 'elan'), " +
        "(NULL, NULL, 'x') AS t(id, a, b)")
    val expected = Seq(
      Row(1L, "kitten", "sitting", 3, 2L), Row(4L, "\u00e9lan", "elan", 1, 5L),
      Row(null, null, "x", null, null))
    // scalastyle:on nonascii
    checkRows(df.selectExpr("id", "a", "b", "edit_distance(b, a)", "add_one(id)"), expected)
  }

  private val typedValues: Seq[(String, Seq[Any])] = Seq(
    "boolean" -> Seq(true, false, null),
    "tinyint" -> Seq(Byte.MinValue, 1.toByte, null),
    "smallint" -> Seq(Short.MaxValue, -2.toShort, null),
    "int" -> Seq(Int.MinValue, 3, null),
    "bigint" -> Seq(Long.MaxValue, -4L, null),
    "float" -> Seq(1.5f, Float.MinPositiveValue, null),
    "double" -> Seq(-2.25, Double.MaxValue, null),
    "decimal(38,10)" -> Seq(new JBigDecimal("1234567890123456789012345678.0123456789"),
      new JBigDecimal("-0.0000000001"), null),
    // scalastyle:off nonascii
    "string" -> Seq("", "\u00fcber", null),
    // scalastyle:on nonascii
    "binary" -> Seq(Array[Byte](), Array[Byte](0, -1, 2), null),
    "date" -> Seq(Date.valueOf("1970-01-01"), Date.valueOf("2026-11-11"), null),
    "timestamp" -> Seq(Timestamp.valueOf("2026-11-11 01:02:03.456789"), null),
    "timestamp_ntz" -> Seq(LocalDateTime.parse("1999-12-31T23:59:59.999999"), null),
    "array<int>" -> Seq(Seq(1, null, 3), Seq(), null),
    "map<string,bigint>" -> Seq(Map("a" -> 1L, "b" -> null), Map(), null),
    "struct<a: int, b: string NOT NULL>" -> Seq(Row(null, "x"), Row(1, ""), null),
    "array<struct<a: array<string>, b: map<int,binary>>>" -> Seq(
      Seq(Row(Seq("x", null), Map(1 -> Array[Byte](1))), null, Row(null, Map())), null))

  nativeGridTest("return the values of each type of version 1")(typedValues.map(_._1)) { ddl =>
      val values = typedValues.toMap.apply(ddl)
      val pkg = createPackage(library, functions = Seq(function("identity", Seq(ddl), ddl)))
      val session = newSession(pkg)
      register(session, pkg.getName, "identity")
      val df = session.createDataFrame(
        values.map(Row(_)).asJava, StructType(Seq(StructField("v", DataType.fromDDL(ddl)))))
      checkRows(df.selectExpr("v", "identity(v)"), values.map(v => Row(v, v)))
  }

  nativeGridTest("pass the types of the manifest to create, with useLargeVarTypes")(
      Seq(false, true)) { largeVarTypes =>
    val types = Seq("bigint", "string", "binary", "timestamp", "timestamp_ntz", "decimal(10,2)",
      "date", "array<string>", "map<string,int>", "struct<a: int, b: string NOT NULL>")
    val pkg = createPackage(library, functions = Seq(function("describe", types, "string"),
      function("shout", Seq("string"), "string"), function("identity", Seq("binary"), "binary")))
    val session = newSession(pkg)
    session.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "America/Los_Angeles")
    session.conf.set(SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key, largeVarTypes.toString)
    register(session, pkg.getName, "describe", "shout", "identity")
    val schema = StructType(types.zipWithIndex.map { case (ddl, i) =>
      StructField(s"c$i", DataType.fromDDL(ddl), nullable = i % 2 == 1)
    })
    val row = Row(1L, null, Array[Byte](), Timestamp.valueOf("2026-11-11 00:00:00"), null,
      new JBigDecimal("1.00"), Date.valueOf("2026-11-11"), Seq("a"), Map("a" -> 1), Row(1, "b"))
    val df = session.createDataFrame(Seq(row).asJava, schema)
    checkRows(df.selectExpr(s"describe(${types.indices.map(i => s"c$i").mkString(", ")})"),
      Seq(Row("l,u?,z,tsu:America/Los_Angeles?,tsu:,d:10,2?,tdD,+l?(u?),+m(+s(u,i?)),+s?(i?,u)" +
        "->u?;America/Los_Angeles")))
    // A reader of 32-bit offsets gets other values from large strings and binaries.
    val values = session.createDataFrame(
      Seq(Row("ab", Array[Byte](0, 1, 2)), Row("cde", Array[Byte]()), Row(null, null)).asJava,
      StructType(Seq(StructField("s", StringType), StructField("b", BinaryType))))
    checkRows(values.selectExpr("shout(s)", "identity(b)"),
      Seq(Row("AB!", Array[Byte](0, 1, 2)), Row("CDE!", Array[Byte]()), Row(null, null)))
  }

  nativeTest("read input arrays with offsets") {
    // Spark exports batches with an offset of 0, but the header allows offsets at every level,
    // as in slices of other batches. The test library reads them as the C data interface says.
    val library = NativeLibraries.getFunctionLibrary(NativeDataSourcePackage.read(defaultPackage))
    val vector = new BigIntVector("x", ArrowUtils.rootAllocator)
    val root = VectorSchemaRoot.of(vector)
    Seq[java.lang.Long](10L, 11L, 12L, null, 14L).zipWithIndex.foreach {
      case (null, i) => vector.setNull(i)
      case (value, i) => vector.setSafe(i, value)
    }
    root.setRowCount(5)
    def eval(name: String): Seq[Any] = {
      val task = NativeTask.create()
      val input = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
      val out = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
      val result = new BigIntVector("result", ArrowUtils.rootAllocator)
      try {
        val schema = StructType(Seq(StructField("arg0", LongType)))
        val state = NativeArrow.exportSchema(schema, "UTC") { arguments =>
          val resultSchema = ArrowSchema.allocateNew(ArrowUtils.rootAllocator)
          try {
            Data.exportField(ArrowUtils.rootAllocator, result.getField, null, resultSchema)
            library.create(name, NativeUdfLibrary.SCALAR, arguments,
              resultSchema.memoryAddress(), "UTC", task)
          } finally {
            resultSchema.release()
            resultSchema.close()
          }
        }
        try {
          Data.exportVectorSchemaRoot(ArrowUtils.rootAllocator, root, null, input)
          // The rows 1 to 3 of the struct, whose column starts at the value 1 of the vector.
          val batch = input.memoryAddress()
          val column = Platform.getLong(null, Platform.getLong(null, batch + 48))
          Platform.putLong(null, batch, 3)
          Platform.putLong(null, batch + 16, 1)
          Platform.putLong(null, column, 4)
          Platform.putLong(null, column + 8, -1)
          Platform.putLong(null, column + 16, 1)
          library.eval(state, batch, out.memoryAddress())
          Data.importIntoVector(ArrowUtils.rootAllocator, out, result, null, false)
          (0 until result.getValueCount).map(i => result.getObject(i): Any)
        } finally {
          // The results are released before the state is closed.
          result.close()
          library.close(state)
        }
      } finally {
        input.release()
        input.close()
        out.release()
        out.close()
        result.close()
        task.close()
      }
    }
    try {
      assert(eval("identity") == Seq(12L, null, 14L))
      assert(eval("offset") == Seq(2L, 2L, 2L))
    } finally {
      root.close()
    }
  }

  nativeTest("give each call of a function in each task a state of its own") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "7")
    register(session, defaultPackage.getName, "rows_before")
    // 8 tasks of 125 rows, which count the rows of their states across their batches.
    val df = session.range(0, 1000, 1, 8)
      .selectExpr("id", "rows_before(id) AS a", "rows_before(id + 1) AS b", "rows_before(id) AS c")
    checkRows(df, (0L until 1000L).map(id => Row(id, id % 125, id % 125, id % 125)))
    assert(evaluations(df).map(_.udfs.size) == Seq(2))
  }

  nativeTest("give create the context of the task") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "task_attempt_id")
    session.udf.register("scala_task_attempt_id", () => TaskContext.get().taskAttemptId())
    checkRows(
      session.range(0, 10, 1, 3).selectExpr("task_attempt_id() = scala_task_attempt_id()"),
      Seq.fill(10)(Row(true)))
  }

  nativeTest("end the batches at maxRecordsPerBatch rows and maxBytesPerBatch bytes") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    register(session, defaultPackage.getName, "add_one", "shout")
    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "10")
    val counted = session.range(0, 95, 1, 1).selectExpr("add_one(id)")
    checkRows(counted, (1L to 95L).map(Row(_)))
    assert(evaluations(counted).head.metrics("numBatches").value == 10)

    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "1000")
    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_BATCH.key, "1000")
    // About 10 strings of 100 bytes in each batch.
    val sized = session.range(0, 100, 1, 1).selectExpr("shout(repeat('a', 100))")
    checkRows(sized, Seq.fill(100)(Row("A" * 100 + "!")))
    val batches = evaluations(sized).head.metrics("numBatches").value
    assert(batches >= 9 && batches <= 11, batches)
  }

  nativeTest("end a batch once the rows that it keeps hold maxBytesPerBatch bytes") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_BYTES_PER_BATCH.key, "100000")
    register(session, defaultPackage.getName, "add_one")
    // The payload is not an argument, so each batch keeps its rows of 10 KB.
    val payload = "x" * 10000
    val df = session.createDataFrame((0L until 100L).map(Row(_, payload)).asJava,
      StructType(Seq(StructField("id", LongType), StructField("payload", StringType))))
      .selectExpr("add_one(id)", "payload")
    checkRows(df, (1L to 100L).map(Row(_, payload)))
    val batches = evaluations(df).head.metrics("numBatches").value
    assert(batches >= 9, batches)
  }

  nativeTest("release the rows that a batch keeps when its task stops early") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one")
    val payload = "x" * 10000
    val df = session.createDataFrame((0L until 100L).map(Row(_, payload)).asJava,
      StructType(Seq(StructField("id", LongType), StructField("payload", StringType))))
      .where("add_one(id) > 3").selectExpr("add_one(id)", "payload").limit(2)
    checkRows(df, Seq(Row(4L, payload), Row(5L, payload)))
  }

  nativeGridTest("report a failure of the library as NATIVE_UDF_ERROR")(Seq(
    Case("failed eval", ("fail_on", "fail_on(id, 3)", false,
      "fail_on: got 3 in row 3 of the batch")),
    Case("failed eval without last_error", ("fail_on", "fail_on(id, 3)", true,
      "eval failed with the error code 1.")),
    Case("failed create", ("fail_create", "fail_create(id)", false,
      "fail_create: injected failure")),
    Case("NULL state", ("null_state", "null_state(id)", false, "create returned a NULL state.")),
    Case("types that the library does not implement", ("add_one", "add_one(CAST(id AS INT))",
      false, "add_one: unsupported types (i) -> i")),
    Case("function that the library does not implement", ("subtract", "subtract(id)", false,
      "unknown function subtract")))) {
    case Case(_, (name, call, noLastError, message)) =>
      val testLibrary = if (noLastError) compile("test_native_udf_ne", "-DNO_LAST_ERROR") else {
        library
      }
      // The manifest disagrees with the library for the last two functions.
      val declared = Seq(addOne.replace("add_one", "subtract")) ++ functions
        .filterNot(_.contains("\"add_one\"")) :+ function("add_one", Seq("int"), "int")
      val pkg = createPackage(testLibrary, functions = declared)
      val session = newSession(pkg)
      session.sql(s"CREATE TEMPORARY FUNCTION $name AS '$name' USING PACKAGE '${pkg.getName}'")
      val contexts = NativeLibrary.openTaskContexts()
      val e = intercept[Exception](session.range(0, 5, 1, 1).selectExpr(call).collect())
      checkError(
        exception = findError(e, "NATIVE_UDF_ERROR"),
        condition = "NATIVE_UDF_ERROR",
        parameters = Map("function" -> s"`$name`", "name" -> name, "package" -> pkg.getName,
          "msg" -> message))
      eventually(timeout(1.minute))(assert(NativeLibrary.openTaskContexts() <= contexts))
  }

  nativeGridTest("reject a result that breaks the rules of the header")(Seq(
    "offset" -> ("The result has an offset of 1, but Spark only reads arrays with an offset of " +
      "0. Copy sliced arrays before returning them."),
    "short" -> "eval returned 2 values for a batch of 3 rows.",
    "long" -> "eval returned 4 values for a batch of 3 rows.",
    "null_count" -> ("The result has a null_count of 1 but no validity buffer, which the C data " +
      "interface only allows with a null_count of 0."),
    "negative_null_count" -> "The result has an invalid null_count of -2.",
    "children" -> "The result has 1 child arrays, but its type has 0.",
    "buffers" -> "The result has 2 buffers, but a NULL pointer to them.",
    "unset" -> "eval succeeded without setting its result.").map { case (kind, message) =>
    Case(kind, ("bad_result", message))
  } ++ Seq(
    "child_offset" -> ("A child array of the result has an offset of 1, but Spark only reads " +
      "arrays with an offset of 0. Copy sliced arrays before returning them."),
    "short_child" ->
      "The result has a child array with a length of 2, but it refers to 3 values of it.",
    "null_child" -> "The result has a NULL child array.",
    "child_dictionary" ->
      "A child array of the result is dictionary-encoded, which Spark does not read.").map {
    case (kind, message) => Case(kind, ("bad_nested_result", message))
  }) { case Case(kind, (function, message)) =>
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, function)
    val e = intercept[Exception] {
      session.range(0, 3, 1, 1).selectExpr(s"$function(id, '$kind')").collect()
    }
    checkError(
      exception = findError(e, "NATIVE_UDF_ERROR"),
      condition = "NATIVE_UDF_ERROR",
      parameters = Map("function" -> s"`$function`", "name" -> function,
        "package" -> defaultPackage.getName, "msg" -> message))
  }

  nativeGridTest("reject a null that the type of a result does not allow as")(Seq(
    // The library returns its input, whose field a is null in the second row.
    Case("a field declared NOT NULL", ("identity", Seq("struct<a: int, b: string>"),
      "struct<a: int NOT NULL, b: string>",
      "SELECT identity(named_struct('a', id, 'b', 'x')) FROM " +
        "(SELECT IF(id = 1, NULL, CAST(id AS INT)) AS id FROM range(3))",
      "value in the field a, which the type of the result does not allow: " +
        "STRUCT<a: INT NOT NULL, b: STRING>.")),
    Case("a key of a map", ("bad_map_result", Seq("bigint", "string"), "map<bigint,bigint>",
      "SELECT bad_map_result(id, 'null_key') FROM range(3)",
      "key of a map, which the type of the result does not allow: MAP<BIGINT, BIGINT>.")))) {
    case Case(_, (name, arguments, returns, query, returned)) =>
      val pkg = createPackage(library, functions = Seq(function(name, arguments, returns)))
      val session = newSession(pkg)
      register(session, pkg.getName, name)
      val e = intercept[Exception](session.sql(query).collect())
      checkError(
        exception = findError(e, "NATIVE_UDF_ERROR"),
        condition = "NATIVE_UDF_ERROR",
        parameters = Map("function" -> s"`$name`", "name" -> name, "package" -> pkg.getName,
          "msg" -> s"eval returned a null $returned"))
  }

  nativeTest("let a library keep its input until the state is closed") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "4")
    register(session, defaultPackage.getName, "keep_input", "add_one")
    // The library returns the buffers of the inputs that it keeps, and checks at close that
    // they did not change: Spark writes the next batches to other buffers, also for add_one,
    // which reads the same column.
    checkRows(
      session.range(0, 20, 1, 2).selectExpr("keep_input(id)", "add_one(id)"),
      (0L until 20L).map(id => Row(id, id + 1)))
  }

  nativeGridTest("reject a library that Spark cannot use")(Seq(
    ("-DABI_VERSION=2", "UNSUPPORTED_UDF_ABI_VERSION", None),
    ("-DTABLE_SIZE=8", "INVALID_LIBRARY",
      Some("spark_udf_init returned a function table with a size of 8 bytes.")),
    ("-DTABLE_SIZE=24", "INVALID_LIBRARY",
      Some("The function table that spark_udf_init returned, of 24 bytes, does not have the " +
        "function create.")),
    ("-DTABLE_SIZE=offsetof(SparkUdfLibrary,close)", "INVALID_LIBRARY",
      Some("The function table that spark_udf_init returned, of 40 bytes, does not have the " +
        "function close.")),
    ("-DINIT_FAILURE=1", "INVALID_LIBRARY", Some("spark_udf_init failed with the error code 1.")),
    ("-DINIT_FAILURE=2", "INVALID_LIBRARY", Some("spark_udf_init returned no function table.")),
    ("-DNO_INIT", "INVALID_LIBRARY",
      Some("The library does not export the function spark_udf_init."))).map {
    case (define, subClass, reason) => Case(define, (define, subClass, reason))
  }) {
    case Case(_, (define, subClass, reason)) =>
      val variant = compile(
        "test_native_udf_" + define.replaceAll("[^A-Za-z0-9]", "_").toLowerCase(Locale.ROOT),
        define)
      val pkg = createPackage(variant)
      val session = newSession()
      register(session, pkg.getPath, "add_one")
      // Each query gets the error of the library, which is not loaded again.
      (1 to 2).foreach { _ =>
        val e = intercept[Exception](session.sql("SELECT add_one(1)").collect())
        val parameters = reason match {
          case Some(r) =>
            Map("library" -> s"${NativePlatform.current}/${variant.getName}", "reason" -> r)
          case None => Map("version" -> "2", "supported" -> "1")
        }
        checkError(
          exception = findError(e, s"INVALID_NATIVE_DATA_SOURCE_PACKAGE.$subClass"),
          condition = s"INVALID_NATIVE_DATA_SOURCE_PACKAGE.$subClass",
          parameters = Map("path" -> pkg.getCanonicalPath) ++ parameters)
      }
  }

  nativeTest("log the messages of a library in the log of Spark, with the path of the library") {
    val name = "test_native_udf_logs"
    val pkg = NativeDataSourcePackage.read(createPackage(compile(name, "-DINIT_FAILURE=1")))
    val logAppender = new LogAppender("native function logs")
    withLogAppender(logAppender, loggerNames = Seq(classOf[NativeUdfLibrary].getName)) {
      intercept[Exception](NativeLibraries.getFunctionLibrary(pkg))
    }
    val events = logAppender.loggingEvents.filter(
      _.getMessage.getFormattedMessage.contains("injected failure in spark_udf_init"))
    assert(events.map(_.getLevel) == Seq(Level.ERROR))
    assert(events.head.getMessage.getFormattedMessage.contains(System.mapLibraryName(name)))
  }

  nativeTest("tell an eval that its task was killed") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "wait_for_interrupt")
    val marker = new File(Utils.createTempDir(), "interrupted")
    val df = session.sql(s"SELECT wait_for_interrupt('${marker.getPath}') FROM range(1)")
    val jobGroup = s"native-udf-${UUID.randomUUID()}"
    val error = new AtomicReference[Throwable]()
    val contexts = NativeLibrary.openTaskContexts()
    val thread = new Thread(() => {
      session.sparkContext.setJobGroup(jobGroup, "an eval that waits", interruptOnCancel = true)
      try df.collect() catch {
        case e: Throwable => error.set(e)
      }
    })
    thread.start()
    // The eval waits until its task is killed, which it only learns from is_interrupted: Spark
    // cannot interrupt native code.
    eventually(timeout(1.minute)) {
      assert(new File(marker.getPath + ".waiting").isFile)
    }
    session.sparkContext.cancelJobGroup(jobGroup)
    thread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!thread.isAlive)
    assert(error.get() != null)
    eventually(timeout(1.minute)) {
      assert(marker.isFile && Files.readString(marker.toPath) == "interrupted")
      assert(NativeLibrary.openTaskContexts() <= contexts)
    }
  }

  nativeTest("close the states of a task that stops early") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ARROW_EXECUTION_MAX_RECORDS_PER_BATCH.key, "100")
    register(session, defaultPackage.getName, "rows_before")
    val contexts = NativeLibrary.openTaskContexts()
    checkRows(session.sql("SELECT rows_before(id) FROM range(0, 100000, 1, 1) LIMIT 1"),
      Seq(Row(0L)))
    eventually(timeout(1.minute))(assert(NativeLibrary.openTaskContexts() <= contexts))
  }

  /** The context of a task, whose memory manager has pages of `pageSize` bytes. */
  private def taskContext(pageSize: Long): (TaskContextImpl, TestMemoryManager) = {
    val memory = new TestMemoryManager(new SparkConf(false).set(BUFFER_PAGESIZE, pageSize))
    (new TaskContextImpl(0, 0, 0, 0, 0, 1, new TaskMemoryManager(memory, 0), new Properties, null),
      memory)
  }

  nativeGridTest("close a state once its eval returns when the task ends during it, with")(
      Seq("the arguments read back", "the rows kept")) { rowsOf =>
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    register(session, defaultPackage.getName, "wait_for_interrupt")
    val marker = new File(Utils.createTempDir(), "interrupted")
    val path = UTF8String.fromString(marker.getPath)
    val (query, input) = if (rowsOf == "the rows kept") {
      (s"SELECT wait_for_interrupt(path), n FROM VALUES ('${marker.getPath}', 1) AS t(path, n)",
        InternalRow(path, 1))
    } else {
      (s"SELECT wait_for_interrupt(path) FROM VALUES ('${marker.getPath}') AS t(path)",
        InternalRow(path))
    }
    val exec = evaluations(session.sql(query)).head
    val evaluator = new NativeUDFEvaluator(exec.child.output, exec.udfs, exec.udfs.map(_.pkg),
      batchSize = 10, maxBytes = Long.MaxValue, timeZoneId = "UTC", metrics = exec.metrics)
    val (context, _) = taskContext(pageSize = 1024)
    val memory = context.taskMemoryManager
    val contexts = NativeLibrary.openTaskContexts()
    val rows = evaluator.evaluate(context, 0, Iterator(input))
    val failure = new AtomicReference[Throwable]()
    // The consumer runs on another thread than the end of the task, as a pipelined writer does.
    val consumer = new Thread(() => {
      try rows.hasNext catch {
        case e: Throwable => failure.set(e)
      }
    })
    consumer.start()
    eventually(timeout(1.minute)) {
      assert(new File(marker.getPath + ".waiting").isFile)
    }
    assert((memory.getMemoryConsumptionForThisTask > 0) == (rowsOf == "the rows kept"))
    context.markTaskCompleted(None)
    assert(memory.getMemoryConsumptionForThisTask == 0)
    assert(NativeLibrary.openTaskContexts() == contexts + 1)
    context.markInterrupted("test")
    consumer.join(TimeUnit.MINUTES.toMillis(1))
    assert(!consumer.isAlive)
    assert(findError(failure.get(), "NATIVE_UDF_ERROR").getMessageParameters.get("msg") ==
      "wait_for_interrupt: interrupted")
    assert(NativeLibrary.openTaskContexts() == contexts)
    assert(!rows.hasNext)
    intercept[NoSuchElementException](rows.next())
  }

  nativeTest("spill the rows that a batch keeps when the task needs their memory") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    register(session, defaultPackage.getName, "add_one")
    val exec = evaluations(session.sql(
      "SELECT add_one(id), payload FROM VALUES (1L, 'x') AS t(id, payload)")).head
    val evaluator = new NativeUDFEvaluator(exec.child.output, exec.udfs, exec.udfs.map(_.pkg),
      batchSize = 1000, maxBytes = Long.MaxValue, timeZoneId = "UTC", metrics = exec.metrics)
    val pageSize = 1024L
    val (context, memoryManager) = taskContext(pageSize)
    val memory = context.taskMemoryManager
    val payload = UTF8String.fromString("x" * 200)
    val rows = evaluator.evaluate(context, 0, (0L until 20L).iterator.map(InternalRow(_, payload)))
    assert(rows.hasNext)
    assert(memory.getMemoryConsumptionForThisTask > pageSize)
    memoryManager.limit(0)
    val other = new TestMemoryConsumer(memory)
    other.use(pageSize)
    assert(other.getUsed == pageSize)
    assert(rows.map(_.copy()).map(row => (row.getLong(0), row.getUTF8String(1), row.getLong(2)))
      .toSeq == (0L until 20L).map(id => (id, payload, id + 1)))
    other.free(pageSize)
    context.markTaskCompleted(None)
    assert(memory.getMemoryConsumptionForThisTask == 0)
  }

  nativeGridTest("evaluate the arguments and the results of Python UDFs, with Arrow")(
      Seq(false, true)) { arrow =>
    assume(IntegratedUDFTestUtils.shouldTestPythonUDFs, "Python UDFs need Python and pyspark.")
    assume(!arrow || IntegratedUDFTestUtils.shouldTestPandasUDFs, "Arrow needs pandas and pyarrow.")
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    register(session, defaultPackage.getName, "add_one")
    val evalType =
      if (arrow) PythonEvalType.SQL_ARROW_BATCHED_UDF else PythonEvalType.SQL_BATCHED_UDF
    IntegratedUDFTestUtils.registerTestUDF(
      IntegratedUDFTestUtils.TestPythonUDF(name = "py_udf", pythonEvalType = evalType), session)
    // Python returns its argument, so the order of the operators tells which call is first.
    def operators(df: DataFrame): Seq[String] = df.queryExecution.executedPlan.collect {
      case _: EvalNativeUDFsExec => "native"
      case _: BatchEvalPythonExec if !arrow => "python"
      case _: ArrowEvalPythonExec if arrow => "python"
    }
    val nativeFirst = session.sql("SELECT py_udf(add_one(id)) FROM range(3)")
    checkRows(nativeFirst, Seq(Row(1L), Row(2L), Row(3L)))
    assert(operators(nativeFirst) == Seq("python", "native"))
    val pythonFirst = session.sql("SELECT add_one(py_udf(id)) FROM range(3)")
    checkRows(pythonFirst, Seq(Row(1L), Row(2L), Row(3L)))
    assert(operators(pythonFirst) == Seq("native", "python"))
  }

  nativeTest("share an evaluation between the sibling calls of an operator") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    register(session, defaultPackage.getName, "add_one", "shout")
    val df = session.sql("SELECT add_one(add_one(id)), shout(CAST(id AS STRING)) FROM range(3)")
    checkRows(df, Seq(Row(2L, "0!"), Row(3L, "1!"), Row(4L, "2!")))
    assert(evaluations(df).map(_.udfs.map(_.name)) == Seq(Seq("add_one"), Seq("add_one", "shout")))
  }

  nativeTest("evaluate a call only on the rows that the conjuncts written before it keep") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "fail_on")
    checkRows(
      session.sql("SELECT id FROM range(0, 5, 1, 1) WHERE id <> 3 AND fail_on(id, 3) >= 0"),
      Seq(Row(0L), Row(1L), Row(2L), Row(4L)))
  }

  nativeTest("push a limit below the evaluation of native calls") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
    register(session, defaultPackage.getName, "add_one")
    val df = session.sql("SELECT add_one(id) FROM range(1000000) LIMIT 5")
    assert(df.collect().length == 5)
    assert(evaluations(df).map(_.child.exists(_.isInstanceOf[LocalLimitExec])) == Seq(true))
  }

  nativeGridTest("filter the partitions of a file source with a native call, from")(
      Seq("a catalog table", "a path with the V2 file source")) { source =>
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one")
    val rows = session.range(6).selectExpr("id", "id % 3 AS p").write.partitionBy("p")
    val df = if (source == "a catalog table") {
      rows.saveAsTable("native_partitioned")
      session.table("native_partitioned")
    } else {
      session.conf.set(SQLConf.USE_V1_SOURCE_LIST.key, "")
      val dir = new File(Utils.createTempDir(), "native_partitioned")
      rows.parquet(dir.getPath)
      session.read.parquet(dir.getPath)
    }
    try {
      checkRows(df.where("add_one(p) = 2").select("id"), Seq(Row(1L), Row(4L)))
    } finally {
      session.sql("DROP TABLE IF EXISTS native_partitioned")
    }
  }

  test("push no native call to a data source as a partition predicate") {
    val session = spark.newSession()
    registerForPlans(session)
    session.conf.set("spark.sql.catalog.testpartfilter",
      classOf[InMemoryTableEnhancedPartitionFilterCatalog].getName)
    session.sql("CREATE TABLE testpartfilter.t (p BIGINT, data STRING) USING " +
      s"${classOf[FakeV2ProviderWithCustomSchema].getName} PARTITIONED BY (p)")
    session.sql("INSERT INTO testpartfilter.t VALUES (1, 'a'), (2, 'b')")
    val plan = session.sql("SELECT data FROM testpartfilter.t WHERE add_one(p) = 2")
      .queryExecution.optimizedPlan
    val pushed = plan.collect { case relation: DataSourceV2ScanRelation => relation.scan }.map {
      case scan: InMemoryEnhancedPartitionFilterTable#InMemoryEnhancedPartitionFilterBatchScan =>
        scan.getPushedPartitionPredicates
    }
    assert(pushed == Seq(Nil), plan)
  }

  gridTest("inject no runtime filter that evaluates a native call, for a join key")(Seq(
    Case("computed by a call on the creation side",
      "f.id = d.k" -> "SELECT coalesce(add_one(id), 0) AS k FROM range(10) WHERE id = 5"),
    Case("with a call on the application side",
      "add_one(f.id) = d.k" -> "SELECT id AS k FROM range(10) WHERE id = 5"))) {
    case Case(_, (condition, creationSide)) =>
      val session = spark.newSession()
      registerForPlans(session)
      session.conf.set(SQLConf.RUNTIME_BLOOM_FILTER_APPLICATION_SIDE_SCAN_SIZE_THRESHOLD.key, "0")
      session.conf.set(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key, "-1")
      val plan = session.sql(
        s"SELECT f.id FROM range(1000) f JOIN ($creationSide) d ON $condition")
        .queryExecution.optimizedPlan
      assert(!plan.exists(_.expressions.exists(_.exists(_.isInstanceOf[BloomFilterMightContain]))),
        plan)
  }

  private val plansManifest = manifestJson(Seq(function("add_one", Seq("bigint"), "bigint")),
    Map(NativePlatform.current -> "lib.so"))

  gridTest("resolve the package of a call against the packages of the active session, for")(Seq(
    Case("another package with the same file name", ((artifact: File, dir: File) =>
      createZip(dir, artifact.getName,
        NativeDataSourcePackage.MANIFEST_NAME -> plansManifest.getBytes(UTF_8),
        "lib.so" -> "another library".getBytes(UTF_8)), false)),
    Case("a copy in another directory", ((artifact: File, dir: File) =>
      Files.copy(artifact.toPath, new File(dir, artifact.getName).toPath).toFile, true)),
    Case("a copy with another file name", ((artifact: File, dir: File) =>
      Files.copy(artifact.toPath, new File(dir, "other.sparkpkg").toPath).toFile, false)))) {
    case Case(_, (packageFile, usesSessionCopy)) =>
      try {
        val artifact = createZip(Utils.createTempDir(), "plans.sparkpkg",
          NativeDataSourcePackage.MANIFEST_NAME -> plansManifest.getBytes(UTF_8),
          "lib.so" -> "a library".getBytes(UTF_8))
        val session = newSession(artifact)
        val pkg = NativeDataSourcePackage.read(packageFile(artifact, Utils.createTempDir()))
        val resolved = NativeFunctionRegistry.inActiveSession(pkg)
        if (usesSessionCopy) {
          assert(resolved.path ==
            session.artifactManager.getNativeDataSourcePackages._1.head.getCanonicalPath)
        } else {
          assert(resolved eq pkg)
        }
      } finally SparkSession.setActiveSession(spark)
  }

  private val callInLambda = "SELECT transform(array(1L, 2L), x -> add_one(x))"

  gridTest("reject a call in a lambda function at analysis, for")(Seq(
    Case("a query", callInLambda),
    Case("the schema of a query", s"DESCRIBE QUERY $callInLambda"),
    Case("a temporary view", s"CREATE TEMPORARY VIEW lambda_call AS $callInLambda"))) {
    case Case(_, query) =>
      val session = spark.newSession()
      registerForPlans(session)
      checkError(
        exception = intercept[AnalysisException](session.sql(query)),
        condition = "UNSUPPORTED_FEATURE.NATIVE_UDF_IN_LAMBDA",
        parameters = Map("functionName" -> "`add_one`"))
  }

  test("reject a call in a lambda function that reaches the optimizer") {
    val session = spark.newSession()
    registerForPlans(session)
    val x = NamedLambdaVariable("x", LongType, nullable = false)
    val call = session.sessionState.catalog.lookupFunction(FunctionIdentifier("add_one"), Seq(x))
    val plan = Project(
      Seq(Alias(ArrayTransform(Literal(Array(1L)), LambdaFunction(call, Seq(x))), "r")()),
      OneRowRelation())
    checkError(
      exception = intercept[AnalysisException](ExtractNativeUDFs(plan)),
      condition = "UNSUPPORTED_FEATURE.NATIVE_UDF_IN_LAMBDA",
      parameters = Map("functionName" -> "`add_one`"))
  }

  nativeTest("evaluate calls in joins, aggregates and the arguments of higher-order functions") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one", "edit_distance")
    checkRows(
      session.sql("SELECT a.id, b.id FROM range(3) a JOIN range(3) b " +
        "ON edit_distance(CAST(a.id AS STRING), CAST(b.id AS STRING)) = 0 AND a.id < 2"),
      Seq(Row(0L, 0L), Row(1L, 1L)))
    checkError(
      exception = intercept[AnalysisException] {
        session.sql("SELECT * FROM range(3) a LEFT JOIN range(3) b " +
          "ON edit_distance(CAST(a.id AS STRING), CAST(b.id AS STRING)) = 0").collect()
      },
      condition = "UNSUPPORTED_FEATURE.NATIVE_UDF_IN_ON_CLAUSE",
      parameters = Map("joinType" -> "LEFT OUTER"))
    checkError(
      exception = intercept[AnalysisException] {
        session.sql("SELECT * FROM range(3) a WHERE EXISTS (SELECT 1 FROM range(4) b " +
          "WHERE edit_distance(CAST(a.id AS STRING), CAST(b.id AS STRING)) = 0)").collect()
      },
      condition = "UNSUPPORTED_FEATURE.NATIVE_UDF_IN_ON_CLAUSE",
      parameters = Map("joinType" -> "LEFT SEMI"))
    checkRows(
      session.sql("SELECT add_one(id % 2) AS k, add_one(count(*)) AS c FROM range(10) " +
        "GROUP BY add_one(id % 2)"),
      Seq(Row(1L, 6L), Row(2L, 6L)))
    val constant = session.sql("SELECT add_one(1), count(*) FROM range(3)")
    checkRows(constant, Seq(Row(2L, 3L)))
    assert(constant.queryExecution.optimizedPlan.collect {
      case evaluation: EvalNativeUDFs => evaluation.child.exists(_.isInstanceOf[Aggregate])
    } == Seq(true))
    checkRows(
      session.sql("SELECT transform(array(add_one(id), id), x -> x * 10) FROM range(2)"),
      Seq(Row(Seq(10L, 0L)), Row(Seq(20L, 10L))))
  }

  nativeGridTest("evaluate a call on one side of an outer join or in a correlated subquery")({
    def join(joinType: String, side: String): String = "SELECT a.id, b.id FROM range(4) a " +
      s"$joinType JOIN range(4) b ON a.id = b.id AND add_one($side.id) > 2"
    val leftRows = Seq(Row(0L, null), Row(1L, null), Row(2L, 2L), Row(3L, 3L))
    val rightRows = Seq(Row(null, 0L), Row(null, 1L), Row(2L, 2L), Row(3L, 3L))
    val scalar = "SELECT id, " +
      "(SELECT add_one(count(b.id)) FROM range(3) b WHERE b.id = a.id) FROM range(4) a"
    val scalarRows = Seq(Row(0L, 2L), Row(1L, 2L), Row(2L, 2L), Row(3L, 1L))
    val noConf = Seq.empty[(String, String)]
    Seq(
      Case("LEFT OUTER JOIN, on the preserved side", (join("LEFT", "a"), noConf, leftRows)),
      Case("LEFT OUTER JOIN, on the null-supplying side", (join("LEFT", "b"), noConf, leftRows)),
      Case("RIGHT OUTER JOIN, on the preserved side", (join("RIGHT", "b"), noConf, rightRows)),
      Case("RIGHT OUTER JOIN, on the null-supplying side", (join("RIGHT", "a"), noConf, rightRows)),
      Case("correlated EXISTS", ("SELECT id FROM range(4) a WHERE EXISTS " +
        "(SELECT 1 FROM range(4) b WHERE b.id = a.id AND add_one(b.id) > 2)", noConf,
        Seq(Row(2L), Row(3L)))),
      Case("correlated IN", ("SELECT id FROM range(4) a WHERE id IN " +
        "(SELECT add_one(b.id) FROM range(4) b WHERE b.id < a.id)", noConf,
        Seq(Row(1L), Row(2L), Row(3L)))),
      Case("correlated scalar subquery", (scalar, noConf, scalarRows)),
      Case("correlated scalar subquery, with its aggregate optimized before the rewrite",
        (scalar, Seq(SQLConf.DECORRELATE_SUBQUERY_PREVENT_CONSTANT_FOLDING_FOR_COUNT_BUG.key ->
          "false"), scalarRows)))
  }) { case Case(_, (query, confs, expected)) =>
    val session = newSession(defaultPackage)
    confs.foreach { case (key, value) => session.conf.set(key, value) }
    register(session, defaultPackage.getName, "add_one")
    checkRows(session.sql(query), expected)
  }

  nativeGridTest("plan an equi-join on a call over one side of an inner join, for")(Seq(
    Case("a WHERE clause over two relations",
      "SELECT a.id, b.id FROM range(3) a, range(5) b WHERE add_one(a.id) = b.id" ->
        Seq(Row(0L, 1L), Row(1L, 2L), Row(2L, 3L))),
    Case("the ON clause of a join of three relations",
      "SELECT a.id, b.id, c.id FROM range(3) a JOIN range(5) b ON add_one(a.id) = b.id " +
        "JOIN range(5) c ON b.id = c.id" ->
        Seq(Row(0L, 1L, 1L), Row(1L, 2L, 2L), Row(2L, 3L, 3L))))) {
    case Case(_, (query, rows)) =>
      val session = newSession(defaultPackage)
      register(session, defaultPackage.getName, "add_one")
      val df = session.sql(query)
      val plan = df.queryExecution.optimizedPlan
      val joins = plan.collect { case join: Join => join }
      assert(joins.nonEmpty && joins.forall(_.condition.isDefined), plan)
      assert(plan.collect { case e: EvalNativeUDFs => e.child.exists(_.isInstanceOf[Join]) } ==
        Seq(false), plan)
      checkRows(df, rows)
  }

  nativeTest("evaluate an IN subquery on a call and both sides of an inner join") {
    val session = newSession(defaultPackage)
    register(session, defaultPackage.getName, "add_one")
    checkRows(
      session.sql("SELECT a.id, b.id FROM range(3) a, range(5) b " +
        "WHERE a.id = b.id AND add_one(a.id) + b.id IN (SELECT id FROM range(5))"),
      Seq(Row(0L, 0L), Row(1L, 1L)))
  }

  nativeGridTest("evaluate the calls of a deterministic function once")(Seq(true, false)) {
    deterministic =>
      val pkg = createPackage(library,
        functions = Seq(function("add_one", Seq("bigint"), "bigint", Some(deterministic))))
      val session = newSession(pkg)
      session.conf.set(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key, "false")
      register(session, pkg.getName, "add_one")
      // Spark never evaluates a call on the driver, also with constant arguments.
      val df = session.sql("SELECT add_one(1), add_one(1)")
      checkRows(df, Seq(Row(2L, 2L)))
      assert(evaluations(df).map(_.udfs.size) == Seq(if (deterministic) 1 else 2))
  }

  nativeTest("load a library that implements native data sources too") {
    val both = compileWithDataSources("test_native_udf_with_data_sources")
    val pkg = createPackage(both, dataSources = Some(NativeDataSourceTestUtils.dataSources("")))
    val session = newSession(pkg)
    register(session, pkg.getName, "add_one")
    checkRows(
      session.read.format("native_range").option("end", "3").load().selectExpr("add_one(id)"),
      Seq(Row(1L), Row(2L), Row(3L)))
  }

  nativeTest("initialize a function library once") {
    val pkg = NativeDataSourcePackage.read(createPackage(library))
    val pool = Executors.newFixedThreadPool(8)
    try {
      val loaded = pool.invokeAll((1 to 8).map { _ =>
        (() => NativeLibraries.getFunctionLibrary(pkg)): Callable[NativeUdfLibrary]
      }.asJava).asScala.map(_.get(1, TimeUnit.MINUTES))
      assert(loaded.forall(_ eq loaded.head))
    } finally {
      pool.shutdownNow()
    }
  }

  nativeTest("initialize the two interfaces of a library one at a time") {
    val pkg = NativeDataSourcePackage.read(
      createPackage(compile("test_native_udf_with_ds_init", "-DDS_INIT")))
    val start = new CyclicBarrier(2)
    val pool = Executors.newFixedThreadPool(2)
    try {
      val loads = Seq[() => AnyRef](
        () => NativeLibraries.get(pkg),
        () => NativeLibraries.getFunctionLibrary(pkg))
      pool.invokeAll(loads.map { load =>
        (() => { start.await(); load() }): Callable[AnyRef]
      }.asJava).asScala.foreach(_.get(1, TimeUnit.MINUTES))
    } finally {
      pool.shutdownNow()
    }
  }
}
