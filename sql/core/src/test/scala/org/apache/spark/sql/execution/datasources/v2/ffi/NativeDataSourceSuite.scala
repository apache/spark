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
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.UUID
import java.util.concurrent.{Callable, CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.scalatest.time.SpanSugar._

import org.apache.spark.{SPARK_VERSION, SparkClassNotFoundException, SparkFiles, SparkRuntimeException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.api.python.{PythonBroadcast, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{AnalysisException, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.columnar.{ColumnarBatchScan, ColumnarScan}
import org.apache.spark.sql.execution.datasources.v2.python.{PythonDataSourceV2, UserDefinedPythonDataSource}
import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Tests native data sources with the library in `native-datasource/test_native_datasource.cc`,
 * compiled with the C++ compiler of the machine by [[NativeDataSourceTestUtils]]. The tests that
 * load a library are skipped when there is no C++ compiler, or before Java 22.
 */
class NativeDataSourceSuite extends QueryTest with SharedSparkSession {
  import NativeDataSourceTestUtils._

  // Jobs outside of SQL executions get the files of all the sessions, so the sessions are kept
  // until the end of the suite: none of their artifacts is removed while another test runs.
  private val sessions = ArrayBuffer.empty[SparkSession]

  // The test library and its variants, each implementing differently named data sources.
  private lazy val library = compile("test_native_datasource")
  private lazy val readOnlyLibrary =
    compile("test_native_datasource_ro", "-DREAD_ONLY", "-DNAME_PREFIX=\"ro_\"")
  private lazy val negatingLibrary =
    compile("test_native_datasource_b", "-DNAME_PREFIX=\"b_\"", "-DID_SIGN=-1")
  // Implements the data sources of the test library, as another version of it would.
  private lazy val renamedNegatingLibrary =
    compile("test_native_datasource_negating", "-DID_SIGN=-1")
  private lazy val abiV2Library =
    compile("test_native_datasource_v2", "-DABI_VERSION=2", "-DNAME_PREFIX=\"v2_\"")
  private lazy val noLastErrorLibrary =
    compile("test_native_datasource_ne", "-DNO_LAST_ERROR", "-DNAME_PREFIX=\"ne_\"")
  private lazy val smallTableLibrary = compile("test_native_datasource_small",
    "-DTABLE_SIZE=offsetof(SparkDsLibrary,read)", "-DNAME_PREFIX=\"small_\"")
  private lazy val failingInitLibrary =
    compile("test_native_datasource_if", "-DINIT_FAILURE=1", "-DNAME_PREFIX=\"if_\"")
  private lazy val noTableLibrary =
    compile("test_native_datasource_nt", "-DINIT_FAILURE=2", "-DNAME_PREFIX=\"nt_\"")
  private lazy val noInitLibrary =
    compile("test_native_datasource_ni", "-DNO_INIT", "-DNAME_PREFIX=\"ni_\"")
  private lazy val installedLibrary =
    compile("test_native_datasource_installed", "-DNAME_PREFIX=\"installed_\"")
  private lazy val installedNegatingLibrary = compile(
    "test_native_datasource_installed_b", "-DNAME_PREFIX=\"installed_\"", "-DID_SIGN=-1")

  private lazy val defaultPackage = createPackage(library, dataSources(""))

  /** A new session that finds the native data source packages at the given paths. */
  // Like Python data sources, native data sources are found in the active session.
  private def newSession(paths: File*): SparkSession = {
    val session = spark.newSession()
    SparkSession.setActiveSession(session)
    sessions += session
    session.conf.set(SQLConf.NATIVE_DATA_SOURCE_PATHS.key, paths.map(_.getPath).mkString(","))
    session
  }

  // The test libraries record the calls that the order of the calls of the header rules out.
  override def afterEach(): Unit = {
    try {
      if (orderViolations.exists()) {
        val violations = Files.readString(orderViolations.toPath)
        orderViolations.delete()
        fail(s"The test library got calls that the header rules out:\n$violations")
      }
    } finally {
      super.afterEach()
    }
  }

  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and Java 22 or later are required.")
    try f finally SparkSession.setActiveSession(spark)
  }

  // Checks the answer without converting the DataFrame to an RDD: a job outside of a SQL
  // execution gets the files of all the sessions, and the streaming queries of the tests create
  // sessions whose files are removed when they are garbage collected.
  private def checkRows(df: => DataFrame, expected: Seq[Row]): Unit = {
    QueryTest.checkAnswer(df, expected, checkToRDD = false)
  }

  private def range(session: SparkSession, options: (String, String)*): DataFrame = {
    session.read.format("native_range").options(options.toMap).load()
  }

  private def scan(df: DataFrame): ColumnarScan = {
    df.queryExecution.optimizedPlan.collectFirst {
      case relation: DataSourceV2ScanRelation => relation.scan.asInstanceOf[ColumnarScan]
    }.get
  }

  private def expectedRow(id: Long): Row = Row(
    id,
    (id % 3).toInt,
    if (id % 5 == 0) null else s"n$id",
    id * 0.5,
    id % 2 == 0,
    Date.valueOf(LocalDate.ofEpochDay(id)),
    Timestamp.from(Instant.ofEpochSecond(id)))

  /** Finds the error with the given condition among the causes of a failure. */
  private def findError(e: Throwable, condition: String): SparkThrowable = {
    Iterator.iterate(e)(_.getCause).takeWhile(_ != null)
      .collectFirst { case t: SparkThrowable if t.getCondition == condition => t }
      .getOrElse(throw e)
  }

  /**
   * Checks that Spark closed each data writer that native_sink created under `dir`, and did not
   * use it afterwards. When a write fails, the tasks that Spark kills may still close their data
   * writers after the write returns.
   */
  private def checkDataWritersClosed(dir: File): Unit = eventually(timeout(1.minute)) {
    def names(subdir: String): Set[String] =
      Option(new File(dir, subdir).list()).map(_.toSet).getOrElse(Set.empty)
    assert(names("_created_data_writers").nonEmpty)
    assert(names("_closed_data_writers") == names("_created_data_writers"))
    assert(!new File(dir, "_DATA_WRITER_USED_AFTER_CLOSE").exists())
  }

  // Formats a double as the test library does.
  private def format(d: Double): String = if (d == d.toLong) d.toLong.toString else d.toString

  test("parse manifests") {
    assert(NativeDataSourcePackage.parseManifest(
      """{"abiVersion": 1, "dataSources": ["a", "b"], "libraries": {"linux-x86_64": "x.so"}}""") ==
      Right(NativeDataSourceManifest(1, Seq("a", "b"), Map("linux-x86_64" -> "x.so"))))
    Seq(
      "not json" -> "The manifest is not valid JSON",
      "[1]" -> "The manifest must be a JSON object.",
      """{"dataSources": ["a"], "libraries": {"p": "x"}}""" -> "'abiVersion'",
      """{"abiVersion": 0, "dataSources": ["a"], "libraries": {"p": "x"}}""" ->
        "positive integer field 'abiVersion'",
      // The JSON escape of the character U+0000.
      ("""{"abiVersion": 1, "dataSources": ["a""" + "\\u0000" +
        """"], "libraries": {"p": "x"}}""") -> "'dataSources'",
      """{"abiVersion": 1, "dataSources": [], "libraries": {"p": "x"}}""" -> "'dataSources'",
      """{"abiVersion": 1, "dataSources": [1], "libraries": {"p": "x"}}""" -> "'dataSources'",
      """{"abiVersion": 1, "dataSources": ["a"]}""" -> "'libraries'",
      """{"abiVersion": 1, "dataSources": ["a"], "libraries": {"p": 1}}""" -> "'libraries'"
    ).foreach { case (json, error) =>
      val result = NativeDataSourcePackage.parseManifest(json)
      assert(result.isLeft && result.swap.toOption.get.contains(error), s"$json: $result")
    }
  }

  test("platform names") {
    assert(NativePlatform.name("Linux", "amd64") == "linux-x86_64")
    assert(NativePlatform.name("Linux", "aarch64") == "linux-aarch64")
    assert(NativePlatform.name("Mac OS X", "aarch64") == "osx-aarch64")
    assert(NativePlatform.name("Mac OS X", "x86_64") == "osx-x86_64")
    assert(NativePlatform.name("Windows 11", "amd64") == "windows-x86_64")
    // Linux with musl, as on Alpine Linux, which maps its C library as the loader of musl.
    assert(NativePlatform.name("Linux", "amd64", musl = true) == "linux-x86_64-musl")
    assert(NativePlatform.name("Linux", "aarch64", musl = true) == "linux-aarch64-musl")
    assert(NativePlatform.usesMusl(Seq(
      "7f00a000-7f00b000 r-xp 00000000 08:01 123 /usr/lib/jvm/lib/server/libjvm.so",
      "7f00c000-7f00d000 r-xp 00000000 08:01 456 /lib/ld-musl-x86_64.so.1")))
    assert(!NativePlatform.usesMusl(Seq(
      "7f00c000-7f00d000 r-xp 00000000 08:01 456 /usr/lib/x86_64-linux-gnu/libc.so.6")))
    // The tests run on glibc, macOS or Windows.
    assert(NativePlatform.current == NativePlatform.name(
      System.getProperty("os.name"), System.getProperty("os.arch")))
  }

  test("fail clearly where the C data interface of Arrow Java does not load") {
    // Arrow Java fails like this without a library for the architecture, such as ppc64le, or
    // with one that does not link, such as on musl.
    Seq(
      new IllegalStateException("error loading native libraries: java.io.FileNotFoundException: " +
        "arrow_cdata_jni/ppc64le/libarrow_cdata_jni.so"),
      new UnsatisfiedLinkError("libarrow_cdata_jni.so: Error relocating: symbol not found")
    ).foreach { cause =>
      val e = intercept[SparkUnsupportedOperationException] {
        NativeLibraries.checkCDataInterface(throw cause)
      }
      checkError(
        exception = e,
        condition = "NATIVE_DATA_SOURCE_UNSUPPORTED_PLATFORM",
        parameters = Map("platform" -> NativePlatform.current, "reason" -> cause.toString))
      assert(e.getCause eq cause)
    }
    // It loads where the tests run.
    NativeLibraries.checkCDataInterface()
  }

  test("native library path") {
    def path(directories: String*): String = directories.mkString(File.pathSeparator)
    // The directories of java.library.path, and then the lib directory of each installation
    // prefix in PATH. Relative and duplicate directories are ignored.
    assert(NativeDataSourceRegistry.librarySearchPath(
      path("/opt/native", "", "relative", "/usr/lib"),
      path("/usr/local/bin", "/home/user/.venv/bin", "/usr/sbin", "relative/bin", "/usr/local/bin")
    ) == Seq("/opt/native", "/usr/lib", "/usr/local/lib", "/home/user/.venv/lib").map(new File(_)))
    assert(NativeDataSourceRegistry.librarySearchPath(null, null).isEmpty)
    assert(NativeDataSourceRegistry.libraryFileName("My_Source") ==
      System.mapLibraryName("spark_datasource_my_source"))
  }

  test("tell missing functions from link errors") {
    // A function that the library leaves NULL in its table is an unsupported operation.
    val missing = new NativeLibrary.MissingFunctionException("push_limit")
    assert(NativeCalls.optional(throw missing).isEmpty)
    checkError(
      exception = intercept[SparkRuntimeException](NativeCalls.call("x", "read")(throw missing)),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "read", "name" -> "x", "msg" -> "The library does not implement push_limit."))
    // Link errors are reported as they are, such as when Arrow, which still calls the Arrow C
    // structs through JNI, cannot load its JNI library, which it extracts to java.io.tmpdir, for
    // example if the directory does not allow it.
    val linkError = new UnsatisfiedLinkError("/tmp/libarrow_cdata_jni.so: failed to map segment")
    assert(intercept[UnsatisfiedLinkError](NativeCalls.optional(throw linkError)) eq linkError)
    checkError(
      exception = intercept[SparkRuntimeException](NativeCalls.call("x", "read")(throw linkError)),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map("action" -> "read", "name" -> "x", "msg" -> linkError.getMessage))
  }

  test("close the previous stream writer of a streaming query") {
    val released = ArrayBuffer.empty[Long]
    def handle(value: Long): NativeHandle = new NativeHandle(value, released += _)
    val (first, other, second) = (handle(1), handle(2), handle(3))
    NativeStreamWriters.register("query", first)
    NativeStreamWriters.register("other query", other)
    assert(released.isEmpty)
    NativeStreamWriters.register("query", second)
    assert(released == Seq(1L))
    second.close()
    other.close()
    assert(released == Seq(1L, 3L, 2L))
  }

  test("native data sources require Java 22 or later") {
    assume(!NativeLibrary.isSupported, "The Java version supports native data sources.")
    val dir = Utils.createTempDir()
    // Not loaded: Spark fails before it loads the library.
    Files.writeString(
      new File(dir, NativeDataSourceRegistry.libraryFileName("old_java")).toPath, "unused")
    withLibraryPath(dir) {
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          spark.read.format("old_java").load()
        },
        condition = "NATIVE_DATA_SOURCE_UNSUPPORTED_JAVA_VERSION",
        parameters = Map(
          "required" -> "22", "current" -> Runtime.version().feature().toString))
    }
  }

  test("the bindings match the layout of spark_datasource.h") {
    assume(compiler.isDefined, "A C++ compiler is required.")
    val layout = NativeLibrary.layout().asScala.map { case (key, value) => key -> value.longValue }
    val prints = layout.keys.map { key =>
      val value = key.split('.') match {
        case Array(struct) => s"sizeof($struct)"
        case Array(struct, field) => s"offsetof($struct, $field)"
      }
      s"""  std::printf("%s %zu\\n", "$key", $value);"""
    }
    val output = runProgram("layout",
      s"""#include <cstddef>
         |#include <cstdio>
         |#include "spark_datasource.h"
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
    assert(actual == layout.toMap)
  }

  nativeTest("disable native data sources") {
    val conf = new SQLConf
    conf.setConf(SQLConf.NATIVE_DATA_SOURCE_PATHS, Seq(defaultPackage.getPath))
    conf.setConf(StaticSQLConf.NATIVE_DATA_SOURCE_ENABLED, false)
    assert(NativeDataSourceRegistry.lookup("native_range", conf).isEmpty)
  }

  nativeTest("read with an inferred schema") {
    val session = newSession(defaultPackage)
    val df = range(session, "end" -> "20")
    assert(df.schema == StructType.fromDDL(
      "id BIGINT, mod INT, name STRING, value DOUBLE, flag BOOLEAN, day DATE, ts TIMESTAMP"))
    checkRows(df, (0L until 20L).map(expectedRow))
    // The scan is described by the name of the data source.
    assert(scan(df).description() == "native_range")
    assert(!df.queryExecution.executedPlan.toString.contains("NativeDataSource"))
    // The data source names are case-insensitive, and so are the option keys.
    checkRows(
      session.read.format("NATIVE_RANGE").option("END", "3").load(),
      (0L until 3L).map(expectedRow))
  }

  nativeTest("read in partitions and batches") {
    val df = range(newSession(defaultPackage), "end" -> "50", "partitions" -> "4",
      "batch_size" -> "3")
    assert(df.rdd.getNumPartitions == 4)
    checkRows(df, (0L until 50L).map(expectedRow))
  }

  nativeTest("read with a user-specified schema") {
    val session = newSession(defaultPackage)
    checkRows(
      session.read.schema("id BIGINT, name STRING").format("native_range").option("end", "6")
        .load(),
      (0L until 6L).map(id => Row(id, if (id % 5 == 0) null else s"n$id")))

    // The Arrow schema does not have the collation, which applies to the data that Spark reads.
    val collated = session.read.schema("id BIGINT, name STRING COLLATE UTF8_LCASE")
      .format("native_range").option("end", "6").load()
    checkRows(collated.filter("name = 'N1'"), Seq(Row(1L, "n1")))

    val e = intercept[Exception] {
      session.read.schema("id INT").format("native_range").load().collect()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "read",
        "name" -> "native_range",
        "msg" -> "The data has the schema id BIGINT, but the expected schema is id INT."))
  }

  nativeTest("prune columns") {
    val df = range(newSession(defaultPackage), "end" -> "10").select("name", "id")
    checkRows(df, (0L until 10L).map(id => Row(if (id % 5 == 0) null else s"n$id", id)))
    assert(scan(df).readSchema() == StructType.fromDDL("id BIGINT, name STRING"))

    // The columns that the query filters on are read too, since Spark filters the rows after
    // reading.
    val filtered = range(newSession(defaultPackage), "end" -> "10").filter("id > 7").select("name")
    checkRows(filtered, Seq(Row("n8"), Row("n9")))
    assert(scan(filtered).readSchema() == StructType.fromDDL("id BIGINT, name STRING"))

    // A count reads no column at all.
    assert(range(newSession(defaultPackage), "end" -> "37", "batch_size" -> "10").count() == 37)
  }

  nativeTest("push down a limit") {
    val df = range(newSession(defaultPackage), "end" -> "100", "partitions" -> "5").limit(3)
    assert(df.collect().length == 3)
    assert(scan(df).getMetaData()("PushedLimit") == "LIMIT 3")

    // Spark pushes the limit of LIMIT ... OFFSET twice, but the library, which fails if
    // pushLimit is called again, is only asked once.
    val withOffset = range(newSession(defaultPackage), "end" -> "100", "partitions" -> "5")
      .limit(10).offset(5)
    checkRows(withOffset, (5L until 10L).map(expectedRow))
    assert(scan(withOffset).getMetaData()("PushedLimit") == "LIMIT 10")

    // Spark filters the rows after reading, so it does not push a limit with a filter.
    val filtered = range(newSession(defaultPackage), "end" -> "100").filter("id > 5").limit(3)
    assert(filtered.collect().length == 3)
    assert(!scan(filtered).getMetaData().contains("PushedLimit"))
  }

  nativeTest("read maps whose entry fields have other names") {
    // The library names the fields of the entries `keys` and `values`, as arrow-rs does.
    val df = range(newSession(defaultPackage), "end" -> "3", "extra" -> "map").select("id", "m")
    assert(df.schema("m").dataType == MapType(StringType, LongType, valueContainsNull = true))
    checkRows(df, (0L until 3L).map(id => Row(id, Map(s"k$id" -> id))))
  }

  nativeTest("read structs in several batches") {
    // The reader imports the batches of a partition into the same vectors, which Spark must not
    // close when it moves to the next batch: closing a struct drops its child vectors.
    val df = range(newSession(defaultPackage), "end" -> "10", "partitions" -> "1",
      "batch_size" -> "3", "extra" -> "struct").select("id", "st")
    assert(df.queryExecution.executedPlan.exists(_.isInstanceOf[WholeStageCodegenExec]))
    checkRows(df, (0L until 10L).map(id => Row(id, Row(id, s"s$id"))))
    // So the columns are ReusedArrowColumnVectors, which the consumers of the batches recognize
    // as reused by their producer.
    val batch = scan(df).toBatch
    val reader = batch.createReaderFactory().createColumnarReader(batch.planInputPartitions().head)
    try {
      assert(reader.next())
      val columns = (0 until reader.get().numCols()).map(reader.get().column)
      assert(columns.nonEmpty && columns.forall(_.isInstanceOf[ReusedArrowColumnVector]))
    } finally {
      reader.close()
    }

    // A nested field is read with its top-level column: the library only prunes the top-level
    // columns, and Spark the nested fields.
    val nested = range(newSession(defaultPackage), "end" -> "4", "extra" -> "struct")
      .select("st.b")
    checkRows(nested, (0L until 4L).map(id => Row(s"s$id")))
    assert(scan(nested).readSchema() == StructType.fromDDL("st STRUCT<a: BIGINT, b: STRING>"))
  }

  nativeTest("reject the data that has nulls where the schema to read has none") {
    // The column name is null for the ids that are multiples of 5.
    val session = newSession(defaultPackage)
    val e = intercept[Exception] {
      session.read.schema("id BIGINT, name STRING NOT NULL").format("native_range")
        .option("end", "4").load().collect()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "read",
        "name" -> "native_range",
        "msg" -> "The column name has a null, but the schema to read declares it not nullable."))
    // The fields that are not nullable have no nulls.
    checkRows(
      session.read.schema("id BIGINT NOT NULL, st STRUCT<a: BIGINT NOT NULL, b: STRING NOT NULL>")
        .format("native_range").option("end", "2").option("extra", "struct").load(),
      Seq(Row(0L, Row(0L, "s0")), Row(1L, Row(1L, "s1"))))
  }

  nativeTest("reject the types and the strings that a library cannot get") {
    val session = newSession(defaultPackage)
    def check(action: String, name: String, msg: String)(f: => Unit): Unit = {
      val e = intercept[Exception](f)
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> action, "name" -> name, "msg" -> msg))
    }
    // Spark rejects the types that version 1 does not have before it passes a schema to the
    // library: here, before create_reader and create_writer.
    check("plan a scan of", "native_range", "The column id has the type INTERVAL DAY TO " +
        "SECOND, which native data sources do not support.") {
      session.read.schema("id INTERVAL DAY TO SECOND").format("native_range").load().collect()
    }
    val dir = Utils.createTempDir()
    check("plan a write to", "native_sink", "The column v.element has the type VARIANT, which " +
        "native data sources do not support.") {
      session.sql("SELECT array(parse_json('1')) AS v").write.format("native_sink")
        .option("path", dir.getPath).mode("append").save()
    }
    assert(!new File(dir, "_CREATED").exists())
    // A NUL-terminated string cannot hold the character U+0000.
    check("create", "native_range", "The option end contains the character U+0000, which Spark " +
        "cannot pass to a library.") {
      range(session, "end" -> "1\u00002")
    }
  }

  nativeTest("Arrow UDFs on the columns of a native data source") {
    assume(IntegratedUDFTestUtils.shouldTestPandasUDFs)
    // The reader imports every batch into the same vectors, and closes them once its partition
    // ends, while Spark writes batches to Python before the results of the earlier ones come
    // back: the UDF must not read the pass-through columns of a batch after that.
    val session = newSession(defaultPackage)
    val udf = IntegratedUDFTestUtils.TestScalarPandasUDF(
      name = "native_str", returnType = Some(StringType))
    IntegratedUDFTestUtils.registerTestUDF(udf, session)
    // One batch, two, and many. The test UDF casts its input to string: on the string column
    // name, the cast is removed, so the batches go to Python as they are.
    Seq(100, 200, 200000).foreach { end =>
      val rows = range(session, "end" -> end.toString, "partitions" -> "1", "batch_size" -> "100")
        .selectExpr("id", "name", "native_str(name) AS udf_name").collect()
      assert(rows.map(_.getLong(0)).toSeq == (0L until end))
      rows.foreach { row =>
        val id = row.getLong(0)
        val name = if (id % 5 == 0) null else s"n$id"
        assert(row.getString(1) == name, s"name mismatch for id $id")
        assert(row.getString(2) == name, s"UDF mismatch for id $id")
      }
    }
  }

  nativeTest("reject the Arrow data that Spark cannot read") {
    def check(msg: String, options: (String, String)*): Unit = {
      checkWithSchema(None, msg, options: _*)
    }
    // The schema to read is given for the types from which Spark cannot infer it either.
    def checkWithSchema(schema: Option[String], msg: String, options: (String, String)*): Unit = {
      val e = intercept[Exception] {
        val reader = newSession(defaultPackage).read.format("native_range")
        schema.fold(reader)(s => reader.schema(s)).options(options.toMap + ("end" -> "4")).load()
          .collect()
      }
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> "read", "name" -> "native_range", "msg" -> msg))
    }
    // Arrow Java would read a slice from the start of its buffers.
    check("The column id has an offset of 1, but Spark only reads arrays with an offset of 0. " +
      "Copy sliced arrays before exporting them.", "slice" -> "true")
    // Arrow Java would extend an array with fewer values than its parent refers to with nulls.
    // Each batch has 2 rows.
    check("The column id has a length of 1, but the batch has a length of 2.", "short" -> "id")
    // Arrow Java would import the missing columns as nulls.
    check("The batch has 0 child arrays, but the schema has 7 columns.", "short" -> "children")
    // Arrow Java would read a null row of a batch as a row of the values of its columns. The
    // nulls are counted when the library did not count them.
    Seq("count", "unknown").foreach { nullRows =>
      check("The struct of the batch marks 1 of its rows as null, but Spark only reads batches " +
        "without null rows: mark the values of the columns as null instead.",
        "null_rows" -> nullRows)
    }
    // The C data interface only allows an array without a validity buffer if its null_count is
    // 0, and Arrow Java would fail to import it.
    check("The batch has a null_count of -1 but no validity buffer, which the C data interface " +
      "only allows with a null_count of 0.", "null_rows" -> "no_buffer")
    check("The column id has a null_count of -1 but no validity buffer, which the C data " +
      "interface only allows with a null_count of 0.", "null_rows" -> "column")
    // A batch without null rows is read, also when the library did not count its nulls.
    checkRows(range(newSession(defaultPackage), "end" -> "4", "null_rows" -> "unknown_valid"),
      (0L until 4L).map(expectedRow))
    Seq("entries", "values").foreach { array =>
      check("The column m contains an array with a length of 1, but its parent refers to 2 " +
        "values.", "extra" -> "map", "short" -> array)
    }
    check("The column dec contains the Arrow type Decimal(10, 2, 256), which is not supported.",
      "extra" -> "decimal256")
    // Arrow Java would also extend the elements of a list view with nulls, and Spark does not
    // check their lengths.
    check("The column lv contains the Arrow type ListView, which is not supported.",
      "extra" -> "listview")
    // Types that Spark does not convert, and dictionary encoding.
    checkWithSchema(Some("id BIGINT, ll ARRAY<BIGINT>"),
      "The column ll contains the Arrow type LargeList, which is not supported.",
      "extra" -> "largelist")
    checkWithSchema(Some("id BIGINT, dict STRING"),
      "The column dict is dictionary-encoded, which is not supported.",
      "extra" -> "dictionary")
  }

  nativeTest("batch write") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    def write(mode: String, ids: Range): Unit = {
      import session.implicits._
      ids.map(i => (i.toLong, s"v$i")).toDF("id", "name").repartition(2)
        .write.format("native_sink").option("path", dir.getPath).mode(mode).save()
    }
    def written(): Set[String] = dir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala).toSet

    write("append", 0 until 4)
    assert(written() == (0 until 4).map(i => s"$i,v$i").toSet)
    assert(new File(dir, "_SUCCESS").isFile)
    // The writer is closed after the commit, and so are the data writers of the tasks.
    assert(new File(dir, "_CLOSED").isFile)
    checkDataWritersClosed(dir)
    write("append", 4 until 6)
    assert(written() == (0 until 6).map(i => s"$i,v$i").toSet)
    write("overwrite", 10 until 12)
    assert(written() == (10 until 12).map(i => s"$i,v$i").toSet)

    // Nulls and the other types of the test library.
    range(session, "end" -> "6").write.format("native_sink").option("path", dir.getPath)
      .mode("overwrite").save()
    assert(written() == (0 until 6).map { id =>
      val name = if (id % 5 == 0) "null" else s"n$id"
      s"$id,${id % 3},$name,${format(id * 0.5)},${id % 2 == 0},$id,${id * 1000000L}"
    }.toSet)
  }

  nativeTest("tables in a catalog") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    try {
      session.sql("CREATE TABLE native_range_table USING native_range OPTIONS (end 3)")
      checkRows(session.table("native_range_table"), (0L until 3L).map(expectedRow))
      checkRows(
        session.read.option("end", "2").table("native_range_table"),
        (0L until 2L).map(expectedRow))

      session.sql("CREATE TABLE native_sink_table (id BIGINT, name STRING) USING native_sink " +
        s"OPTIONS (path '${dir.getPath}')")
      session.sql("INSERT INTO native_sink_table VALUES (1, 'a'), (2, 'b')")
      val written = dir.listFiles().filter(_.getName.startsWith("part-"))
        .flatMap(file => Files.readAllLines(file.toPath).asScala)
      assert(written.toSet == Set("1,a", "2,b"))
    } finally {
      session.sql("DROP TABLE IF EXISTS native_range_table")
      session.sql("DROP TABLE IF EXISTS native_sink_table")
    }
  }

  nativeTest("abort a failed write") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val e = intercept[Exception] {
      session.range(10).write.format("native_sink").option("path", dir.getPath)
        .option("fail", "write").mode("append").save()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "write to",
        "name" -> "native_sink",
        "msg" -> "injected failure in write"))
    assert(new File(dir, "_ABORTED").isFile)
    assert(new File(dir, "_CLOSED").isFile)
    assert(!dir.listFiles().exists(_.getName.startsWith("part-")))
  }

  nativeTest("create no writer when computing the data to write fails") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    // The error happens in a shuffle stage, which adaptive query execution runs before the write.
    val e = intercept[Exception] {
      session.range(10).selectExpr("id", "if(id >= 0, raise_error('boom'), 'x') AS name")
        .repartition(2)
        .write.format("native_sink").option("path", dir.getPath).mode("append").save()
    }
    assert(e.getMessage.contains("boom"))
    assert(!new File(dir, "_CREATED").exists())

    session.range(2).write.format("native_sink").option("path", dir.getPath).mode("append").save()
    assert(new File(dir, "_CREATED").isFile)
  }

  nativeTest("abort a write whose commit failed") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val e = intercept[Exception] {
      session.range(10).write.format("native_sink").option("path", dir.getPath)
        .option("fail", "commit").mode("append").save()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "commit a write to",
        "name" -> "native_sink",
        "msg" -> "injected failure in commit"))
    assert(e.getSuppressed.isEmpty)
    // Spark aborted the write with the writer, which it closed afterwards.
    assert(new File(dir, "_ABORTED").isFile)
    assert(new File(dir, "_CLOSED").isFile)
  }

  nativeTest("abort a data writer whose commit failed") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val e = intercept[Exception] {
      session.range(10).repartition(2).write.format("native_sink").option("path", dir.getPath)
        .option("fail", "commitDataWriter").mode("append").save()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "write to",
        "name" -> "native_sink",
        "msg" -> "injected failure in commitDataWriter"))
    // abort_data_writer removed the temporary files of the tasks, whose data writers were then
    // closed.
    assert(new File(dir, "_ABORTED").isFile)
    assert(new File(dir, "_temporary").listFiles().isEmpty)
    assert(!dir.listFiles().exists(_.getName.startsWith("part-")))
    checkDataWritersClosed(dir)
  }

  nativeTest("do not abort a data writer that committed with an invalid message") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    // The library commits the data writer, but passes two commit messages.
    val e = intercept[Exception] {
      session.range(10).repartition(1).write.format("native_sink").option("path", dir.getPath)
        .option("commit_messages", "2").mode("append").save()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "write to",
        "name" -> "native_sink",
        "msg" -> "commit_data_writer returned 2 values instead of one."))
    // The task failed without aborting the committed data writer, which it closed, and the driver
    // aborted the write.
    assert(!new File(dir, "_ABORTED_COMMITTED_DATA_WRITER").exists())
    checkDataWritersClosed(dir)
    assert(new File(dir, "_ABORTED").isFile)
    assert(!dir.listFiles().exists(_.getName.startsWith("part-")))
  }

  nativeTest("abort a micro-batch whose write failed") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val e = intercept[Exception] {
      val query = session.readStream.format("native_counter").option("max_offset", "4").load()
        .writeStream
        .format("native_sink")
        .option("path", dir.getPath)
        .option("fail", "write")
        .option("checkpointLocation", Utils.createTempDir().getPath)
        .start()
      try query.processAllAvailable() finally query.stop()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "write to",
        "name" -> "native_sink",
        "msg" -> "injected failure in write"))
    // Spark aborted the first epoch, with a null message for its task, which did not commit.
    assert(Files.readString(new File(dir, "_ABORTED_0").toPath) == "1")
    assert(!new File(dir, "_EPOCH_0").exists())
  }

  nativeTest("continuous processing fails with a native data source") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val e = intercept[Exception] {
      val query = session.readStream.format("rate").load()
        .writeStream
        .format("native_sink")
        .option("path", dir.getPath)
        .option("checkpointLocation", Utils.createTempDir().getPath)
        .trigger(Trigger.Continuous("1 second"))
        .start()
      try query.awaitTermination(60000) finally query.stop()
    }
    checkError(
      exception = findError(e, "DATA_SOURCE_CONTINUOUS_WRITE_NOT_SUPPORTED"),
      condition = "DATA_SOURCE_CONTINUOUS_WRITE_NOT_SUPPORTED",
      parameters = Map("description" -> "native_sink"))
    // The query failed before Spark created a writer in the library.
    assert(!new File(dir, "_CREATED").exists())
    // A native scan does not report continuous reads, so Spark rejects them when it starts.
    val readError = intercept[Exception] {
      session.readStream.format("native_counter").load()
        .writeStream.format("noop").trigger(Trigger.Continuous("1 second")).start()
    }
    checkError(
      exception = findError(readError, "_LEGACY_ERROR_TEMP_2253"),
      condition = "_LEGACY_ERROR_TEMP_2253",
      parameters = Map("sourceName" -> "native_counter"))
  }

  nativeTest("streaming read and write") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val commitPath = new File(Utils.createTempDir(), "committed")
    val closedPath = new File(Utils.createTempDir(), "closed")
    val query = session.readStream.format("native_counter")
      .option("max_offset", "12").option("step", "5").option("batch_size", "2")
      .option("commit_path", commitPath.getPath)
      .option("closed_path", closedPath.getPath)
      .load()
      .writeStream
      .format("native_sink")
      .option("path", dir.getPath)
      .option("checkpointLocation", Utils.createTempDir().getPath)
      .start()
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    // Each call to latestOffset makes 5 more rows available: [0, 5), [5, 10) and [10, 12).
    assert((0 to 2).forall(epoch => new File(dir, s"_EPOCH_$epoch").isFile))
    // Each micro-batch creates a new stream writer, which closes the one of the previous
    // micro-batch. The last one is closed once it is no longer referenced.
    assert((0 to 1).forall(epoch => new File(dir, s"_CLOSED_$epoch").isFile))
    val written = dir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala)
    assert(written.sorted.toSeq == (0 until 12).map(_.toString).sorted)
    // Spark commits the offsets of a micro-batch when it runs the next one.
    assert(Files.readString(commitPath.toPath) == "{\"offset\":10}")
    // The stream reader is closed when the query stops.
    assert(closedPath.isFile)

    // The streaming source can also be read in micro-batches into a memory sink.
    val memoryQuery = session.readStream.format("native_counter").option("max_offset", "4")
      .load().writeStream.format("memory").queryName("native_counter_memory").start()
    try {
      memoryQuery.processAllAvailable()
      checkRows(session.table("native_counter_memory"), (0L until 4L).map(Row(_)))
    } finally {
      memoryQuery.stop()
    }
  }

  nativeTest("pass the checkpoint location and the start of a micro-batch to the stream reader") {
    val session = newSession(defaultPackage)
    val checkpoint = Utils.createTempDir()
    val dir = Utils.createTempDir()
    // Runs the query until it processed the rows [0, maxOffset), 5 more rows in each micro-batch,
    // and returns the checkpoint location and the start offsets that the stream reader got.
    def run(maxOffset: Int): (String, Seq[Long]) = {
      val checkpointPath = new File(dir, s"checkpoint_$maxOffset")
      val startsPath = new File(dir, s"starts_$maxOffset")
      val query = session.readStream.format("native_counter")
        .option("max_offset", maxOffset.toString).option("step", "5")
        .option("checkpoint_path", checkpointPath.getPath)
        .option("starts_path", startsPath.getPath)
        .load()
        .writeStream
        .format("noop")
        .option("checkpointLocation", checkpoint.getPath)
        .start()
      try query.processAllAvailable() finally query.stop()
      val starts = Files.readAllLines(startsPath.toPath).asScala.toSeq
        .map(_.stripPrefix("{\"offset\":").stripSuffix("}").toLong)
      (Files.readString(checkpointPath.toPath), starts)
    }
    // The first micro-batch starts after the initial offset, and the next ones after the end of
    // the previous one, until no more rows are available.
    val (location, starts) = run(12)
    assert(location.endsWith(s"${checkpoint.getCanonicalPath}/sources/0"), location)
    assert(starts.take(4) == Seq(0L, 5L, 10L, 12L) && starts.drop(4).forall(_ == 12L), starts)
    // A query that restarts from its checkpoint starts after the end of its last micro-batch, with
    // the same checkpoint location.
    val (restartedLocation, restartedStarts) = run(15)
    assert(restartedLocation == location)
    assert(restartedStarts.take(2) == Seq(12L, 15L), restartedStarts)
  }

  nativeTest("reject offsets that are not JSON on a single line") {
    // Spark writes each offset as a line of the offset log of the checkpoint.
    def check(badOffset: String, msg: String): Unit = {
      val e = intercept[Exception] {
        val query = newSession(defaultPackage).readStream.format("native_counter")
          .option("bad_offset", badOffset).load()
          .writeStream
          .format("noop")
          .option("checkpointLocation", Utils.createTempDir().getPath)
          .start()
        try query.processAllAvailable() finally query.stop()
      }
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> "plan a scan of", "name" -> "native_counter", "msg" -> msg))
    }
    check("none", "latest_offset returned no offset.")
    check("multiline", "latest_offset returned an offset with a line break, but an " +
      "offset must be JSON on a single line.")
    check("invalid", "latest_offset returned an offset that is not valid JSON: offset 10")
  }

  nativeTest("pass the ID of a streaming query to its stream writers") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val checkpoint = Utils.createTempDir().getPath
    // Runs the query until it processed the rows [0, maxOffset).
    def run(maxOffset: Int): StreamingQuery = {
      val query = session.readStream.format("native_counter")
        .option("max_offset", maxOffset.toString).load()
        .writeStream
        .format("native_sink")
        .option("path", dir.getPath)
        .option("checkpointLocation", checkpoint)
        .start()
      try query.processAllAvailable() finally query.stop()
      query
    }
    val queryId = new File(dir, "_QUERY_ID")
    val first = run(3)
    assert(Files.readString(queryId.toPath) == first.id.toString)
    // The ID stays the same when the query restarts from its checkpoint, unlike the ID of the
    // run, so that the library can tell the commits of the query from those of other queries.
    assert(queryId.delete())
    val restarted = run(6)
    assert(restarted.id == first.id && restarted.runId != first.runId)
    assert(Files.readString(queryId.toPath) == first.id.toString)
  }

  nativeTest("report the errors of native code") {
    def check(action: String, msg: String, name: String = "native_range")(
        f: SparkSession => Unit): Unit = {
      val e = intercept[Exception](f(newSession(defaultPackage)))
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> action, "name" -> name, "msg" -> msg))
    }
    check("create", "injected failure in create") { session =>
      range(session, "fail" -> "create")
    }
    check("infer the schema of", "injected failure in schema") { session =>
      range(session, "fail" -> "schema")
    }
    check("plan a scan of", "injected failure in plan") { session =>
      range(session, "fail" -> "plan").collect()
    }
    check("read", "injected failure in open") { session =>
      range(session, "fail" -> "open").collect()
    }
    check("read", "injected failure in get_next") { session =>
      range(session, "fail" -> "read").collect()
    }
    check("read", "The library did not initialize the stream, or a callback of the stream is " +
        "NULL.") { session =>
      range(session, "fail" -> "uninitialized").collect()
    }
    // An error of the library is not an unsupported operation: here, of a data source that
    // declares batch reads that it does not support.
    check("plan a scan of", "native_sink does not support batch reads", "native_sink") {
      session => session.read.schema("id BIGINT").format("native_sink")
        .option("capabilities", NativeLibrary.BATCH_READ.toString).load().collect()
    }
    // Without last_error, only the value that the function returned is reported.
    val e = intercept[Exception] {
      newSession(createPackage(noLastErrorLibrary, dataSources("ne_")))
        .read.format("ne_native_range").option("fail", "create").load()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "create",
        "name" -> "ne_native_range",
        "msg" -> "create failed with the error code 1."))
  }

  nativeTest("reject a state returned more than once") {
    def check(action: String, name: String, function: String)(f: SparkSession => Unit): Unit = {
      val e = intercept[Exception](f(newSession(defaultPackage)))
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> action, "name" -> name,
          "msg" -> s"$function returned 2 states instead of one."))
    }
    check("plan a scan of", "native_range", "partitions") { session =>
      range(session, "states" -> "2").collect()
    }
    check("plan a scan of", "native_counter", "stream_partitions") { session =>
      val query = session.readStream.format("native_counter").option("states", "2").load()
        .writeStream.format("noop").start()
      try query.processAllAvailable() finally query.stop()
    }
    check("plan a write to", "native_sink", "create_writer") { session =>
      session.range(2).write.format("native_sink").option("path", Utils.createTempDir().getPath)
        .option("states", "2").mode("append").save()
    }
  }

  nativeTest("a scan whose planning failed fails again with the same error") {
    // The reader is released after planning, even if it failed. The query execution of a
    // DataFrame keeps the failure of its plan, but a scan can be planned again, for example by
    // the lazy values of the physical plan that call it.
    val session = newSession(defaultPackage)
    val (name, location) =
      NativeDataSourceRegistry.lookup("native_range", session.sessionState.conf).get
    val options = new CaseInsensitiveStringMap(Map("fail" -> "plan").asJava)
    val batch = new ColumnarBatchScan(
      new NativeDataSource(location, name, options).batchPlanner(StructType.fromDDL("id BIGINT")))
    (1 to 2).foreach { _ =>
      checkError(
        exception = intercept[SparkRuntimeException](batch.planInputPartitions()),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> "plan a scan of",
          "name" -> "native_range",
          "msg" -> "injected failure in plan"))
    }
  }

  nativeTest("reject NULL handles") {
    def check(function: String, action: String, name: String)(f: SparkSession => Unit): Unit = {
      val e = intercept[Exception](f(newSession(defaultPackage)))
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> action,
          "name" -> name,
          "msg" -> s"$function returned a NULL handle."))
    }
    def runStream(start: => StreamingQuery): Unit = {
      val query = start
      try query.processAllAvailable() finally query.stop()
    }
    check("create", "create", "native_range") { session =>
      range(session, "zero" -> "create")
    }
    check("create_reader", "plan a scan of", "native_range") { session =>
      range(session, "zero" -> "create_reader").collect()
    }
    check("create_stream_reader", "plan a scan of", "native_counter") { session =>
      runStream {
        session.readStream.format("native_counter")
          .option("zero", "create_stream_reader").load()
          .writeStream.format("noop").start()
      }
    }
    Seq("create_writer" -> "plan a write to", "create_data_writer" -> "write to").foreach {
      case (function, action) =>
        check(function, action, "native_sink") { session =>
          session.range(2).write.format("native_sink")
            .option("path", Utils.createTempDir().getPath).option("zero", function)
            .mode("append").save()
        }
    }
    check("create_writer", "plan a write to", "native_sink") { session =>
      runStream {
        session.readStream.format("native_counter").load()
          .writeStream
          .format("native_sink")
          .option("path", Utils.createTempDir().getPath)
          .option("zero", "create_writer")
          .option("checkpointLocation", Utils.createTempDir().getPath)
          .start()
      }
    }
  }

  nativeTest("operations that a data source does not support") {
    val session = newSession(defaultPackage, createPackage(readOnlyLibrary, dataSources("ro_")))
    // The library does not push anything down: it reads all the columns, and Spark applies the
    // limit.
    val df = session.read.format("ro_native_range").option("end", "10").load()
    val limited = df.select("id").limit(3)
    assert(limited.collect().length == 3)
    assert(scan(limited).readSchema() == df.schema)
    assert(!scan(limited).getMetaData().contains("PushedLimit"))

    def check(condition: String, parameters: Map[String, String])(f: => Unit): Unit = {
      val e = intercept[Exception](f)
      checkError(exception = findError(e, condition), condition = condition,
        parameters = parameters)
    }
    def runStream(start: => StreamingQuery): Unit = {
      val query = start
      try query.processAllAvailable() finally query.stop()
    }
    // A data source declares the operations that it supports with its capabilities. Spark
    // reports the others as unsupported, without calling the library, which here does not
    // implement them either. The table reports the batch writes to Data Source V2, which rejects
    // them while it analyzes the query, before it computes the data to write: here, in a shuffle
    // stage that adaptive query execution would run before the write.
    check("UNSUPPORTED_FEATURE.TABLE_OPERATION",
        Map("tableName" -> "`ro_native_sink`", "operation" -> "batch write")) {
      session.range(10).selectExpr("if(id >= 0, raise_error('boom'), id) AS id").repartition(2)
        .write.format("ro_native_sink").mode("append").save()
    }
    check("DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED", Map("description" -> "ro_native_counter")) {
      runStream {
        session.readStream.format("ro_native_counter").load().writeStream.format("noop").start()
      }
    }
    check("DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED", Map("description" -> "ro_native_sink")) {
      runStream {
        session.readStream.format("rate").load().writeStream.format("ro_native_sink")
          .option("checkpointLocation", Utils.createTempDir().getPath).start()
      }
    }
    // The capabilities are those of each data source: native_counter only supports streaming
    // reads, although its library implements batch reads.
    check("DATA_SOURCE_BATCH_SCAN_NOT_SUPPORTED", Map("description" -> "native_counter")) {
      session.read.format("native_counter").load().collect()
    }
    // A data source that declares a capability must implement its functions.
    check("NATIVE_DATA_SOURCE_ERROR", Map("action" -> "plan a write to",
        "name" -> "ro_native_sink", "msg" -> "The library does not implement create_writer.")) {
      session.range(1).write.format("ro_native_sink")
        .option("capabilities", NativeLibrary.BATCH_WRITE.toString).mode("append").save()
    }
    // Replacing the existing data requires the capability SPARK_DS_BATCH_TRUNCATE for a batch
    // write, and SPARK_DS_STREAMING_TRUNCATE for a streaming write: either is not enough for the
    // other.
    val dir = Utils.createTempDir()
    val writes = NativeLibrary.BATCH_WRITE | NativeLibrary.STREAMING_WRITE
    val batchTruncate = (writes | NativeLibrary.BATCH_TRUNCATE).toString
    val streamingTruncate = (writes | NativeLibrary.STREAMING_TRUNCATE).toString
    check("UNSUPPORTED_FEATURE.TABLE_OPERATION",
        Map("tableName" -> "`native_sink`", "operation" -> "truncate in batch mode")) {
      session.range(1).write.format("native_sink").option("path", dir.getPath)
        .option("capabilities", streamingTruncate).mode("overwrite").save()
    }
    // The table has the capabilities that the data source declares with the options of the table,
    // which the options of a write to a table in a catalog can override: the write checks them
    // again.
    try {
      session.sql("CREATE TABLE native_sink_table (id BIGINT) USING native_sink " +
        s"OPTIONS (path '${dir.getPath}')")
      check("DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED", Map("description" -> "native_sink")) {
        session.range(1).writeTo("native_sink_table")
          .option("capabilities", NativeLibrary.BATCH_READ.toString).append()
      }
      check("NATIVE_DATA_SOURCE_ERROR", Map("action" -> "plan a write to",
          "name" -> "native_sink",
          "msg" -> "The data source does not support the save mode overwrite.")) {
        session.range(1).writeTo("native_sink_table").option("capabilities", streamingTruncate)
          .overwrite(lit(true))
      }
    } finally {
      session.sql("DROP TABLE IF EXISTS native_sink_table")
    }
    def completeWrite(dir: File, capabilities: String): Unit = runStream {
      session.readStream.format("native_counter").load().groupBy().count().writeStream
        .outputMode("complete")
        .format("native_sink")
        .option("path", dir.getPath)
        .option("capabilities", capabilities)
        .option("checkpointLocation", Utils.createTempDir().getPath)
        .start()
    }
    check("NATIVE_DATA_SOURCE_ERROR", Map("action" -> "plan a write to", "name" -> "native_sink",
        "msg" -> "The data source does not support the output mode complete.")) {
      completeWrite(dir, batchTruncate)
    }
    assert(!new File(dir, "_CREATED").exists())
    // Each capability replaces the existing data in its own kind of write.
    val overwritten = Utils.createTempDir()
    session.range(1).write.format("native_sink").option("path", overwritten.getPath)
      .option("capabilities", batchTruncate).mode("overwrite").save()
    assert(new File(overwritten, "_SUCCESS").isFile)
    val completed = Utils.createTempDir()
    completeWrite(completed, streamingTruncate)
    assert(new File(completed, "_EPOCH_0").isFile)
  }

  nativeTest("read only the functions that fit in the table of a library") {
    // The table of the library ends before read, as the table of a library built against an
    // earlier header ends before the functions added later: Spark does not read past its end.
    val session = newSession(createPackage(smallTableLibrary, dataSources("small_")))
    val df = session.read.format("small_native_range").load()
    assert(df.schema.length == 7)
    checkError(
      exception = findError(intercept[Exception](df.collect()), "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "plan a scan of",
        "name" -> "small_native_range",
        "msg" -> "The library does not implement read."))
  }

  nativeTest("pass the version of Spark, and log with the log of Spark") {
    val session = newSession(defaultPackage)
    checkRows(range(session, "end" -> "2", "spark_version" -> SPARK_VERSION).select("id"),
      Seq(Row(0L), Row(1L)))
    checkError(
      exception = findError(
        intercept[Exception](range(session, "spark_version" -> "1.0.0")),
        "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "create",
        "name" -> "native_range",
        "msg" -> (s"the version of Spark is $SPARK_VERSION, which supports version 1 of the " +
          "interface")))

    val logAppender = new LogAppender("native data source logs")
    withLogAppender(logAppender, loggerNames = Seq(classOf[NativeLibrary].getName)) {
      range(session, "log" -> "a message from native code")
    }
    assert(logAppender.loggingEvents.exists { event =>
      val message = event.getMessage.getFormattedMessage
      event.getLevel == org.apache.logging.log4j.Level.WARN &&
        message.contains("a message from native code") &&
        message.contains(System.mapLibraryName("test_native_datasource"))
    })
    // The library does not log a warning when Spark logs errors only.
    val errorAppender = new LogAppender("native data source error logs")
    withLogAppender(errorAppender, loggerNames = Seq(classOf[NativeLibrary].getName),
        level = Some(org.apache.logging.log4j.Level.ERROR)) {
      range(session, "log" -> "a message that Spark does not log")
    }
    assert(!errorAppender.loggingEvents.exists(
      _.getMessage.getFormattedMessage.contains("a message that Spark does not log")))

    // is_log_enabled follows the level of the log of Spark: the library gets whether Spark logs
    // each level, from error to trace.
    val levels = new File(Utils.createTempDir(), "levels")
    Seq(
      org.apache.logging.log4j.Level.TRACE -> "1,1,1,1,1",
      org.apache.logging.log4j.Level.DEBUG -> "1,1,1,1,0",
      org.apache.logging.log4j.Level.INFO -> "1,1,1,0,0",
      org.apache.logging.log4j.Level.WARN -> "1,1,0,0,0",
      org.apache.logging.log4j.Level.ERROR -> "1,0,0,0,0").foreach { case (level, expected) =>
      withLogAppender(new LogAppender("native data source log levels"),
          loggerNames = Seq(classOf[NativeLibrary].getName), level = Some(level)) {
        range(session, "log_levels_path" -> levels.getPath)
      }
      assert(Files.readString(levels.toPath) == expected, level)
    }
  }

  nativeTest("pass the task to reads and data writers") {
    val session = newSession(defaultPackage)
    // Checks the SparkDsTaskContext that the library got in each task, by partition.
    def check(dir: File): Unit = {
      val tasks = dir.listFiles().map { file =>
        file.getName.toInt -> Files.readString(file.toPath).split(",").map(_.toLong).toSeq
      }.toMap
      assert(tasks.keySet == Set(0, 1))
      tasks.foreach {
        case (partition, Seq(size, stageId, stageAttempt, partitionId, attempt, taskAttemptId)) =>
          assert(size == NativeLibrary.layout().get("SparkDsTaskContext").longValue)
          assert(partitionId == partition)
          assert(stageId >= 0 && stageAttempt == 0 && attempt == 0 && taskAttemptId >= 0)
      }
      assert(tasks.values.map(_(5)).toSet.size == 2)
    }
    val readTasks = Utils.createTempDir()
    checkRows(range(session, "end" -> "6", "task_path" -> readTasks.getPath).select("id"),
      (0L until 6L).map(Row(_)))
    check(readTasks)
    val writeTasks = Utils.createTempDir()
    session.range(6).repartition(2).write.format("native_sink")
      .option("path", Utils.createTempDir().getPath)
      .option("task_path", writeTasks.getPath)
      .mode("append")
      .save()
    check(writeTasks)
  }

  nativeTest("tell a read that its task was killed") {
    val session = newSession(defaultPackage)
    val marker = new File(Utils.createTempDir(), "interrupted")
    val df = range(session, "end" -> "10", "partitions" -> "1",
      "wait_for_interrupt" -> marker.getPath)
    val jobGroup = s"native-data-source-${UUID.randomUUID()}"
    val error = new AtomicReference[Throwable]()
    val thread = new Thread(() => {
      session.sparkContext.setJobGroup(jobGroup, "a read that waits", interruptOnCancel = true)
      try df.collect() catch {
        case e: Throwable => error.set(e)
      }
    })
    thread.start()
    // The read waits until its task is killed, which it only learns from is_interrupted: Spark
    // cannot interrupt native code.
    eventually(timeout(1.minute)) {
      assert(new File(marker.getPath + ".waiting").isFile)
    }
    session.sparkContext.cancelJobGroup(jobGroup)
    thread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!thread.isAlive)
    assert(error.get() != null)
    // The job fails right away, and its task ends once the library sees that it was killed.
    eventually(timeout(1.minute)) {
      assert(marker.isFile && Files.readString(marker.toPath) == "interrupted")
    }
  }

  nativeTest("load libraries that export the same functions side by side") {
    val session = newSession(
      defaultPackage, createPackage(negatingLibrary, dataSources("b_")))
    checkRows(range(session, "end" -> "3").select("id"), Seq(Row(0L), Row(1L), Row(2L)))
    checkRows(
      session.read.format("b_native_range").option("end", "3").load().select("id"),
      Seq(Row(0L), Row(-1L), Row(-2L)))
  }

  nativeTest("add packages with spark.addArtifact") {
    val session = newSession()
    session.addArtifact(defaultPackage.getPath)
    val (added, artifactUUID) = session.artifactManager.getNativeDataSourcePackages
    assert(added.map(_.getName) == Seq(defaultPackage.getName))
    checkRows(range(session, "end" -> "4"), (0L until 4L).map(expectedRow))

    // The executors find the package in the files of the session.
    val pkg = NativeDataSourcePackage.read(added.head).copy(artifactUUID = artifactUUID)
    assert(NativeDataSourceRegistry.distributedCopies(pkg).exists(_.isFile))

    // A package that the session added is distributed as it is.
    assert(NativeDataSourceRegistry.addToArtifacts(session, pkg) == pkg)

    // A package under spark.sql.dataSource.native.paths is copied to the artifacts of the session
    // when it is used, except in local mode, where the executors run in the driver. The copy is
    // named after the checksum of the package, and it is not a package of the session.
    val configuredDir = Utils.createTempDir()
    val configured = createPackage(library, dataSources(""), dir = configuredDir,
      fileName = defaultPackage.getName)
    val other = newSession(configured)
    range(other, "end" -> "1").collect()
    assert(!other.artifactManager.hasFileArtifact(defaultPackage.getName))
    val configuredPackage = NativeDataSourcePackage.read(configured)
    val distributed = NativeDataSourceRegistry.addToArtifacts(other, configuredPackage)
    val copyName = s"${defaultPackage.getName}.${configuredPackage.sha256}"
    assert(distributed == configuredPackage.copy(
      artifactUUID = other.artifactManager.getNativeDataSourcePackages._2,
      distributedFileName = Some(copyName)))
    assert(other.artifactManager.hasFileArtifact(copyName))
    assert(other.artifactManager.getNativeDataSourcePackages._1.isEmpty)
    assert(NativeDataSourceRegistry.distributedCopies(distributed).exists(_.isFile))
    // Adding the same package again does nothing.
    assert(NativeDataSourceRegistry.addToArtifacts(other, configuredPackage) == distributed)

    // A package with the same file name as a package of the session does not collide with it.
    other.addArtifact(defaultPackage.getPath)
    val sameFileName = createPackage(negatingLibrary, dataSources("b_"),
      dir = Utils.createTempDir(), fileName = defaultPackage.getName)
    val sameFileNameCopy =
      NativeDataSourceRegistry.addToArtifacts(other, NativeDataSourcePackage.read(sameFileName))
    assert(sameFileNameCopy.distributedFileName.exists(_.startsWith(defaultPackage.getName + ".")))

    // The executors also check the copies in the files of the other sessions, such as the session
    // of a streaming query, which is a clone of the session that started it.
    val sessionDir = new File(SparkFiles.getRootDirectory(), s"test-${UUID.randomUUID()}")
    try {
      val copy = new File(sessionDir, defaultPackage.getName)
      sessionDir.mkdirs()
      Files.copy(defaultPackage.toPath, copy.toPath)
      val copies = NativeDataSourceRegistry.distributedCopies(pkg.copy(artifactUUID = Some("x")))
      assert(copies.take(2) == Seq(
        new File(new File(SparkFiles.getRootDirectory(), "x"), defaultPackage.getName),
        new File(SparkFiles.getRootDirectory(), defaultPackage.getName)))
      assert(copies.contains(copy))
    } finally {
      Utils.deleteRecursively(sessionDir)
    }
  }

  nativeTest("distribute a configured package for concurrent queries") {
    // Queries of a session can use a configured package for the first time at the same time.
    val configured = createPackage(library, dataSources(""), dir = Utils.createTempDir())
    val session = newSession(configured)
    val pkg = NativeDataSourcePackage.read(configured)
    val threads = 8
    val pool = ThreadUtils.newDaemonFixedThreadPool(threads, "distribute-package")
    try {
      val start = new CountDownLatch(1)
      val results = (1 to threads).map { _ =>
        pool.submit(new Callable[NativeDataSourcePackage] {
          override def call(): NativeDataSourcePackage = {
            start.await()
            NativeDataSourceRegistry.addToArtifacts(session, pkg)
          }
        })
      }
      start.countDown()
      val distributed = results.map(_.get(1, TimeUnit.MINUTES)).distinct
      assert(distributed.length == 1)
      assert(session.artifactManager.hasFileArtifact(distributed.head.distributedFileName.get))
    } finally {
      pool.shutdownNow()
    }
  }

  nativeTest("load a package only with the checksum of the driver") {
    // The package at the path of the driver is valid, but it is not the package that the driver
    // read, for example because it was replaced since, and neither is any distributed copy.
    val pkg = NativeDataSourcePackage.read(defaultPackage).copy(sha256 = "0" * 64)
    checkError(
      exception = intercept[SparkRuntimeException](NativeLibraries.get(pkg)),
      condition = "NATIVE_DATA_SOURCE_PACKAGE_NOT_FOUND",
      parameters = Map("fileName" -> defaultPackage.getName, "checksum" -> pkg.sha256))
  }

  test("distribute a package without an active session") {
    val pkg = NativeDataSourcePackage(
      "/path/to/x.sparkpkg", "checksum", NativeDataSourceManifest(1, Seq("x"), Map.empty))
    val logAppender = new LogAppender("distribute without an active session")
    SparkSession.clearActiveSession()
    try {
      withLogAppender(logAppender, level = Some(org.apache.logging.log4j.Level.WARN)) {
        assert(NativeDataSourceRegistry.distribute(pkg) == pkg)
      }
    } finally {
      SparkSession.setActiveSession(spark)
    }
    assert(logAppender.loggingEvents.exists(
      _.getMessage.getFormattedMessage.contains("there is no active session")))
  }

  nativeTest("write with a session that is not the active one") {
    val session = newSession()
    session.addArtifact(defaultPackage.getPath)
    SparkSession.setActiveSession(spark)
    val dir = Utils.createTempDir()
    session.range(3).selectExpr("id", "concat('v', id) AS name")
      .write.format("native_sink").option("path", dir.getPath).mode("append").save()
    val written = dir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala)
    assert(written.toSet == Set("0,v0", "1,v1", "2,v2"))

    val streamDir = Utils.createTempDir()
    val query = session.readStream.format("native_counter").option("max_offset", "3").load()
      .writeStream
      .format("native_sink")
      .option("path", streamDir.getPath)
      .option("checkpointLocation", Utils.createTempDir().getPath)
      .start()
    try {
      query.processAllAvailable()
    } finally {
      query.stop()
    }
    assert(streamDir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala).sorted.toSeq == Seq("0", "1", "2"))
  }

  nativeTest("ignore configured directories that cannot be read") {
    val dir = Utils.createTempDir()
    createPackage(library, dataSources(""), dir = dir, fileName = "a.sparkpkg")
    assert(dir.setReadable(false))
    try {
      assume(dir.listFiles() == null, "The directory can still be read, for example by root.")
      val session = newSession(dir)
      checkError(
        exception = intercept[SparkClassNotFoundException] {
          session.read.format("no_such_source").load()
        },
        condition = "DATA_SOURCE_NOT_FOUND",
        parameters = Map("provider" -> "no_such_source"))
      val e = intercept[AnalysisException](session.sql("SELECT * FROM missing_db.missing_table"))
      assert(e.getCondition == "TABLE_OR_VIEW_NOT_FOUND")
    } finally {
      dir.setReadable(true)
    }
  }

  nativeTest("replace a configured package in place") {
    val dir = Utils.createTempDir()
    val configured = createPackage(library, dataSources(""), dir = dir, fileName = "x.sparkpkg")
    val session = newSession(configured)
    checkRows(range(session, "end" -> "2"), (0L until 2L).map(expectedRow))
    // On a cluster, the package is copied to the artifacts of the session when it is used.
    val first =
      NativeDataSourceRegistry.addToArtifacts(session, NativeDataSourcePackage.read(configured))

    // An operator replaces the package with a new version: here, with one more file.
    val path = s"${NativePlatform.current}/${library.getName}"
    createZip(dir, "x.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        dataSources(""), Map(NativePlatform.current -> path)).getBytes(UTF_8),
      path -> Files.readAllBytes(library.toPath),
      "README" -> "version 2".getBytes(UTF_8))
    // The copy of the previous version is not a package of the session, so the session uses the
    // new version, which is copied under another name.
    checkRows(range(session, "end" -> "3"), (0L until 3L).map(expectedRow))
    val second =
      NativeDataSourceRegistry.addToArtifacts(session, NativeDataSourcePackage.read(configured))
    assert(second.sha256 != first.sha256)
    assert(second.distributedFileName != first.distributedFileName)
    assert(session.artifactManager.getNativeDataSourcePackages._1.isEmpty)
  }

  nativeTest("look up a native data source once") {
    val session = newSession(defaultPackage)
    val provider = DataSource.lookupDataSourceV2("native_range", session.sessionState.conf).get
    assert(provider.isInstanceOf[NativeDataSourceV2])
    assert(NativeDataSourceRegistry.takeLookup("native_range").isEmpty)
    // The provider uses the result of the lookup that found its class, even if the package is
    // no longer configured.
    session.conf.unset(SQLConf.NATIVE_DATA_SOURCE_PATHS.key)
    assert(provider.inferSchema(CaseInsensitiveStringMap.empty()).fieldNames.head == "id")

    // The result is only kept until the next lookup on the thread, whatever it finds, so a
    // provider never takes the result of an earlier lookup.
    session.conf.set(SQLConf.NATIVE_DATA_SOURCE_PATHS.key, defaultPackage.getPath)
    assert(DataSource.lookupDataSource("native_range", session.sessionState.conf) ==
      classOf[NativeDataSourceV2])
    assert(DataSource.lookupDataSource("json", session.sessionState.conf) != null)
    assert(NativeDataSourceRegistry.takeLookup("native_range").isEmpty)
  }

  nativeTest("Python data sources take precedence") {
    val dir = Utils.createTempDir()
    installLibrary(installedLibrary, "installed_native_range", dir)
    withLibraryPath(dir) {
      val session = newSession()
      val conf = session.sessionState.conf
      assert(DataSource.lookupDataSource("installed_native_range", conf) ==
        classOf[NativeDataSourceV2])
      // A Python data source can be registered with the name of an installed native data
      // source, and takes precedence over it.
      val pythonDataSource = UserDefinedPythonDataSource(SimplePythonFunction(
        command = Seq.empty,
        envVars = new java.util.HashMap[String, String](),
        pythonIncludes = java.util.Collections.emptyList[String](),
        pythonExec = "python3",
        pythonVer = "3",
        broadcastVars = java.util.Collections.emptyList[Broadcast[PythonBroadcast]](),
        accumulator = null))
      session.dataSource.registerPython("installed_native_range", pythonDataSource)
      assert(DataSource.lookupDataSource("installed_native_range", conf) ==
        classOf[PythonDataSourceV2])
    }
  }

  nativeTest("find installed libraries automatically") {
    val dir = Utils.createTempDir()
    val installed = installLibrary(installedLibrary, "installed_native_range", dir)
    // A library that implements several data sources is installed under each of their names.
    Files.createSymbolicLink(
      new File(dir, NativeDataSourceRegistry.libraryFileName("installed_native_sink")).toPath,
      installed.toPath)
    withLibraryPath(dir) {
      // The session neither adds a package nor configures a path. The name is case-insensitive.
      val session = newSession()
      checkRows(
        session.read.format("Installed_Native_Range").option("end", "3").load().select("id"),
        Seq(Row(0L), Row(1L), Row(2L)))
      val output = Utils.createTempDir()
      session.range(2).selectExpr("id", "concat('v', id) AS name")
        .write.format("installed_native_sink").option("path", output.getPath).mode("append").save()
      assert(output.listFiles().filter(_.getName.startsWith("part-"))
        .flatMap(file => Files.readAllLines(file.toPath).asScala).toSet == Set("0,v0", "1,v1"))

      // A package of the session takes precedence over an installed library.
      val other = newSession(createPackage(installedNegatingLibrary, dataSources("installed_")))
      checkRows(
        other.read.format("installed_native_range").option("end", "3").load().select("id"),
        Seq(Row(0L), Row(-1L), Row(-2L)))

      // An executor finds the library in its native library path, if it is not where the driver
      // found it.
      val checksum = InstalledNativeLibrary(installed).sha256
      assert(checksum == NativeDataSourcePackage.sha256(installed))
      val elsewhere = new File(Utils.createTempDir(), installed.getName).getPath
      assert(NativeLibraries.get(InstalledNativeLibrary(elsewhere, checksum)) eq
        NativeLibraries.get(InstalledNativeLibrary(installed)))

      // An executor only loads the same build as the driver, which serialized the state for its
      // build: neither the library at the same path nor the one in its native library path is
      // loaded if the driver found another build.
      val otherBuild = NativeDataSourcePackage.sha256(installedNegatingLibrary)
      Seq(installed.getPath, elsewhere).foreach { path =>
        checkError(
          exception = intercept[SparkRuntimeException] {
            NativeLibraries.get(InstalledNativeLibrary(path, otherBuild))
          },
          condition = "NATIVE_DATA_SOURCE_LIBRARY_NOT_FOUND",
          parameters =
            Map("fileName" -> installed.getName, "checksum" -> otherBuild, "path" -> path))
      }
    }

    val missing = new File(Utils.createTempDir(), installed.getName).getPath
    checkError(
      exception = intercept[SparkRuntimeException] {
        NativeLibraries.get(InstalledNativeLibrary(missing, "0"))
      },
      condition = "NATIVE_DATA_SOURCE_LIBRARY_NOT_FOUND",
      parameters = Map("fileName" -> installed.getName, "checksum" -> "0", "path" -> missing))
    checkError(
      exception = intercept[SparkClassNotFoundException] {
        newSession().read.format("installed_native_range").load()
      },
      condition = "DATA_SOURCE_NOT_FOUND",
      parameters = Map("provider" -> "installed_native_range"))
  }

  nativeTest("reject an installed library replaced in place") {
    val dir = Utils.createTempDir()
    val installed = installLibrary(installedLibrary, "installed_native_range", dir)
    withLibraryPath(dir) {
      def read(): DataFrame = {
        newSession().read.format("installed_native_range").option("end", "2").load().select("id")
      }
      checkRows(read(), Seq(Row(0L), Row(1L)))
      val loaded = NativeDataSourcePackage.sha256(installed)

      // An operator installs another build at the same path, as `make install` does. The JVM
      // cannot load it from the same path, so the driver and the executors that loaded the
      // previous build reject it, rather than running a build that does not match the state
      // that the driver serializes.
      Files.copy(installedNegatingLibrary.toPath, installed.toPath, REPLACE_EXISTING)
      val replaced = NativeDataSourcePackage.sha256(installed)
      checkError(
        exception = intercept[SparkRuntimeException](read()),
        condition = "INVALID_NATIVE_DATA_SOURCE_LIBRARY.REPLACED",
        parameters = Map(
          "path" -> installed.getCanonicalPath,
          "loadedChecksum" -> loaded,
          "checksum" -> replaced))

      // The library works again once the build that was loaded is installed again.
      Files.copy(installedLibrary.toPath, installed.toPath, REPLACE_EXISTING)
      checkRows(read(), Seq(Row(0L), Row(1L)))
    }
  }

  nativeTest("find packages in directories") {
    val dir = Utils.createTempDir()
    createPackage(library, dataSources(""), dir = dir, fileName = "a.sparkpkg")
    // Files without the extension are ignored.
    createZip(dir, "ignored.zip", "x" -> Array.emptyByteArray)
    checkRows(range(newSession(dir), "end" -> "2"), (0L until 2L).map(expectedRow))
  }

  nativeTest("conflicting packages") {
    val dir = Utils.createTempDir()
    val first = createPackage(library, dataSources(""), dir = dir, fileName = "a.sparkpkg")
    val second = createPackage(library, Seq("native_range"), dir = dir, fileName = "b.sparkpkg")
    checkError(
      exception = intercept[AnalysisException](range(newSession(dir))),
      condition = "NATIVE_DATA_SOURCE_PACKAGE_CONFLICT",
      parameters = Map(
        "name" -> "native_range",
        "packages" -> Seq(first, second).map(_.getCanonicalPath).mkString(", ")))
    // The same package found twice is not a conflict: here, in the configured paths and in the
    // artifacts of the session.
    val session = newSession(first)
    session.addArtifact(first.getPath)
    checkRows(range(session, "end" -> "2"), Seq(expectedRow(0L), expectedRow(1L)))
  }

  nativeTest("the package that a session added last replaces the other packages") {
    // A session cannot remove an artifact, nor add another file with the same name, so it
    // upgrades a package by adding the new version under another file name.
    val session = newSession(defaultPackage)
    def ids(): DataFrame = range(session, "end" -> "3").select("id")
    checkRows(ids(), Seq(Row(0L), Row(1L), Row(2L)))
    val appender = new LogAppender("replaced packages")
    withLogAppender(appender) {
      // It replaces the configured package...
      val negating = createPackage(renamedNegatingLibrary, dataSources(""),
        fileName = "range-1.0.sparkpkg")
      session.addArtifact(negating.getPath)
      checkRows(ids(), Seq(Row(0L), Row(-1L), Row(-2L)))
      checkRows(ids(), Seq(Row(0L), Row(-1L), Row(-2L)))
      // ... and the packages that the session added before.
      session.addArtifact(createPackage(library, dataSources(""),
        fileName = "range-1.1.sparkpkg").getPath)
      checkRows(ids(), Seq(Row(0L), Row(1L), Row(2L)))
    }
    // Each replacement is logged once.
    val logged = appender.loggingEvents.map(_.getMessage.getFormattedMessage)
      .filter(_.contains("which the session added last"))
    assert(logged.size == 2, logged)
    assert(logged.forall(_.contains("for the data source native_range")), logged)
  }

  nativeTest("Java data sources take precedence") {
    val session = newSession(createPackage(library, Seq("json")))
    val dir = Utils.createTempDir().getPath + "/json"
    session.range(2).write.json(dir)
    checkRows(session.read.format("json").load(dir), Seq(Row(0L), Row(1L)))
  }

  nativeTest("invalid packages") {
    val dir = Utils.createTempDir()
    def check(pkg: File, subClass: String, params: Map[String, String]): Unit = {
      checkError(
        exception = intercept[SparkRuntimeException](range(newSession(pkg)).collect()),
        condition = s"INVALID_NATIVE_DATA_SOURCE_PACKAGE.$subClass",
        parameters = params)
    }

    val missingLibrary = createZip(dir, "missing_library.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        Seq("native_range"), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8))
    check(missingLibrary, "INVALID_MANIFEST", Map(
      "path" -> missingLibrary.getCanonicalPath,
      "reason" -> "The package does not contain the library lib.so."))

    val newerAbi = createPackage(library, Seq("native_range"), dir = dir,
      fileName = "newer_abi.sparkpkg", abiVersion = 2)
    check(newerAbi, "UNSUPPORTED_ABI_VERSION", Map(
      "path" -> newerAbi.getCanonicalPath, "version" -> "2", "supported" -> "1"))

    val otherPlatform = createPackage(library, Seq("native_range"), dir = dir,
      fileName = "other_platform.sparkpkg", platform = "other-platform")
    check(otherPlatform, "UNSUPPORTED_PLATFORM", Map(
      "path" -> otherPlatform.getCanonicalPath,
      "platform" -> NativePlatform.current,
      "platforms" -> "other-platform"))

    val path = s"${NativePlatform.current}/${System.mapLibraryName("not_a_library")}"
    val notALibrary = createZip(dir, "not_a_library.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        Seq("native_range"), Map(NativePlatform.current -> path)).getBytes(UTF_8),
      path -> "not a library".getBytes(UTF_8))
    val e = intercept[SparkRuntimeException](range(newSession(notALibrary)))
    assert(e.getCondition == "INVALID_NATIVE_DATA_SOURCE_PACKAGE.INVALID_LIBRARY")
    assert(e.getMessageParameters.get("path") == notALibrary.getCanonicalPath)
    assert(e.getMessageParameters.get("library") == path)

    // The library implements another version of the interface than its manifest says.
    val mismatchedAbi = createPackage(abiV2Library, dataSources("v2_"), dir = dir,
      fileName = "mismatched_abi.sparkpkg")
    checkError(
      exception = intercept[SparkRuntimeException] {
        newSession(mismatchedAbi).read.format("v2_native_range").load()
      },
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_ABI_VERSION",
      parameters = Map(
        "path" -> mismatchedAbi.getCanonicalPath, "version" -> "2", "supported" -> "1"))
  }

  nativeTest("ignore invalid packages that do not provide the data source") {
    val dir = Utils.createTempDir()
    createPackage(library, dataSources(""), dir = dir, fileName = "a.sparkpkg")
    createZip(dir, "no_manifest.sparkpkg", "x" -> Array.emptyByteArray)
    Files.writeString(new File(dir, "not_a_zip.sparkpkg").toPath, "not a zip")
    val otherSource = createZip(dir, "other_source.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        Seq("other_source"), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8))
    val session = newSession(dir)
    checkRows(range(session, "end" -> "2"), (0L until 2L).map(expectedRow))
    checkError(
      exception = intercept[SparkClassNotFoundException] {
        session.read.format("com.typo.Missing").load()
      },
      condition = "DATA_SOURCE_NOT_FOUND",
      parameters = Map("provider" -> "com.typo.Missing"))
    val e = intercept[AnalysisException](session.sql("SELECT * FROM missing_db.missing_table"))
    assert(e.getCondition == "TABLE_OR_VIEW_NOT_FOUND")
    // The invalid package whose manifest lists the data source reports why it is invalid, when
    // no other package or installed library provides the data source.
    checkError(
      exception = intercept[SparkRuntimeException] {
        session.read.format("other_source").load()
      },
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.INVALID_MANIFEST",
      parameters = Map(
        "path" -> otherSource.getCanonicalPath,
        "reason" -> "The package does not contain the library lib.so."))
  }

  nativeTest("stale packages do not hide a valid one") {
    // A shared directory still has broken and outdated copies of the package of native_range.
    val dir = Utils.createTempDir()
    createZip(dir, "native_range-1.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        dataSources(""), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8))
    createPackage(library, dataSources(""), dir = dir, fileName = "native_range-2.sparkpkg",
      abiVersion = 2)
    val session = newSession(dir)
    checkError(
      exception = intercept[SparkRuntimeException](range(session)),
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_ABI_VERSION",
      parameters = Map(
        "path" -> new File(dir, "native_range-2.sparkpkg").getCanonicalPath,
        "version" -> "2",
        "supported" -> "1"))

    // A valid package added to the session takes precedence over them, without a conflict.
    session.addArtifact(defaultPackage.getPath)
    checkRows(range(session, "end" -> "2"), (0L until 2L).map(expectedRow))

    // So does an installed library.
    val libraries = Utils.createTempDir()
    installLibrary(installedLibrary, "installed_native_range", libraries)
    createZip(dir, "installed-1.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        Seq("installed_native_range"), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8))
    withLibraryPath(libraries) {
      checkRows(
        newSession(dir).read.format("installed_native_range").option("end", "2").load()
          .select("id"),
        Seq(Row(0L), Row(1L)))
    }
  }

  nativeTest("log the packages of a version that Spark does not support") {
    // An operator left an outdated package in a configured directory.
    val dir = Utils.createTempDir()
    val outdated = createPackage(library, Seq("other_native_range"), dir = dir,
      fileName = "outdated.sparkpkg", abiVersion = 2)
    val logAppender = new LogAppender("packages of an unsupported version")
    withLogAppender(logAppender, level = Some(org.apache.logging.log4j.Level.WARN)) {
      checkRows(range(newSession(dir, defaultPackage), "end" -> "2"),
        (0L until 2L).map(expectedRow))
    }
    assert(logAppender.loggingEvents.exists { event =>
      val message = event.getMessage.getFormattedMessage
      message.contains(outdated.getCanonicalPath) &&
        message.contains("implements version 2 of the native data source interface")
    })
  }

  nativeTest("forget the packages of a session that is cleaned up") {
    val session = spark.newSession()
    session.addArtifact(defaultPackage.getPath)
    val added = session.artifactManager.getNativeDataSourcePackages._1.head.getCanonicalPath
    NativeDataSourcePackage.read(new File(added))
    assert(NativeDataSourcePackage.isCached(added))
    session.artifactManager.cleanUpResourcesForTesting()
    assert(!NativeDataSourcePackage.isCached(added))

    // Only the packages under the directory are forgotten.
    val dir = Utils.createTempDir()
    val pkg = createPackage(library, dataSources(""), dir = dir).getCanonicalPath
    NativeDataSourcePackage.read(new File(pkg))
    NativeDataSourcePackage.forgetPackagesIn(new File(dir.getPath + "x"))
    assert(NativeDataSourcePackage.isCached(pkg))
    NativeDataSourcePackage.forgetPackagesIn(dir)
    assert(!NativeDataSourcePackage.isCached(pkg))
  }

  nativeTest("invalid installed libraries") {
    val dir = Utils.createTempDir()
    val notALibrary = new File(dir, NativeDataSourceRegistry.libraryFileName("not_a_library"))
    Files.writeString(notALibrary.toPath, "not a library")
    val newerAbi = installLibrary(abiV2Library, "v2_native_range", dir)
    withLibraryPath(dir) {
      val e = intercept[SparkRuntimeException](newSession().read.format("not_a_library").load())
      assert(e.getCondition == "INVALID_NATIVE_DATA_SOURCE_LIBRARY.CANNOT_LOAD")
      assert(e.getMessageParameters.get("path") == notALibrary.getCanonicalPath)

      // The library stays loaded, and the error is the same when it is used again.
      (1 to 2).foreach { _ =>
        checkError(
          exception = intercept[SparkRuntimeException] {
            newSession().read.format("v2_native_range").load()
          },
          condition = "INVALID_NATIVE_DATA_SOURCE_LIBRARY.UNSUPPORTED_ABI_VERSION",
          parameters = Map(
            "path" -> newerAbi.getCanonicalPath, "version" -> "2", "supported" -> "1"))
      }
    }
  }

  nativeTest("reject the libraries that spark_ds_init does not initialize") {
    def check(library: File, prefix: String, reason: String): Unit = {
      val pkg = createPackage(library, dataSources(prefix))
      // The library stays loaded, and the error is the same when it is used again: Spark
      // initializes a library only once.
      (1 to 2).foreach { _ =>
        checkError(
          exception = intercept[SparkRuntimeException] {
            newSession(pkg).read.format(prefix + "native_range").load()
          },
          condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.INVALID_LIBRARY",
          parameters = Map(
            "path" -> pkg.getCanonicalPath,
            "library" -> s"${NativePlatform.current}/${library.getName}",
            "reason" -> reason))
      }
    }
    val logAppender = new LogAppender("spark_ds_init")
    withLogAppender(logAppender, loggerNames = Seq(classOf[NativeLibrary].getName)) {
      check(failingInitLibrary, "if_", "spark_ds_init failed with the error code 1.")
    }
    // The library logged why it failed, once.
    assert(logAppender.loggingEvents.count { event =>
      event.getLevel == org.apache.logging.log4j.Level.ERROR &&
        event.getMessage.getFormattedMessage.contains("injected failure in spark_ds_init")
    } == 1)
    check(noTableLibrary, "nt_", "spark_ds_init returned no function table.")
    check(noInitLibrary, "ni_", "The library does not export the function spark_ds_init.")
  }
}
