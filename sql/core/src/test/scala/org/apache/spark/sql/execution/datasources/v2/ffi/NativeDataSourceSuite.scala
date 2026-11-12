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

import java.io.{ByteArrayOutputStream, File, ObjectOutputStream}
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.{AccessDeniedException, Files, NoSuchFileException, Paths}
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.attribute.{PosixFileAttributeView, PosixFilePermission}
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.UUID
import java.util.concurrent.{Callable, CountDownLatch, CyclicBarrier, ExecutionException, TimeUnit}
import java.util.concurrent.atomic.AtomicReference
import java.util.zip.{ZipException, ZipFile}

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.arrow.c.{ArrowArray, ArrowArrayStream, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.{BigIntVector, VectorSchemaRoot}
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SPARK_VERSION, SparkClassNotFoundException, SparkException, SparkFiles, SparkRuntimeException, SparkThrowable, SparkUnsupportedOperationException, TaskContext, TaskKilledException, TestUtils}
import org.apache.spark.api.python.{PythonBroadcast, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{AnalysisException, Dataset, IntegratedUDFTestUtils, QueryTest, Row}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.catalog.SupportsRead
import org.apache.spark.sql.connector.expressions.{Expression => V2Expression, FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.Predicate
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, SupportsArrowStreamRead, SupportsPushDownRequiredColumns}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset, ReadLimit, SupportsAdmissionControl}
import org.apache.spark.sql.connector.write.PhysicalWriteInfoImpl
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2ScanRelation, MicroBatchScanExec, V2TableWriteExec}
import org.apache.spark.sql.execution.datasources.v2.columnar.{ColumnarBatchScan, ColumnarBatchWrite, ColumnarMicroBatchStream, ColumnarScan, ColumnarScanBuilder, ColumnarStreamingWrite, ColumnarStreamOffset}
import org.apache.spark.sql.execution.datasources.v2.python.{PythonDataSourceV2, UserDefinedPythonDataSource}
import org.apache.spark.sql.execution.python.ArrowEvalPythonExec
import org.apache.spark.sql.execution.streaming.runtime.StreamingQueryWrapper
import org.apache.spark.sql.execution.vectorized.ReusedArrowColumnVector
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.{ArrowUtils, CaseInsensitiveStringMap}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.Platform
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
  // A library built against the header before default_read_limit, whose table ends before it.
  private lazy val noReadLimitLibrary = compile("test_native_datasource_nrl",
    "-DTABLE_SIZE=offsetof(SparkDsLibrary,default_read_limit)", "-DNAME_PREFIX=\"nrl_\"")
  // Built as against a header without push_domains, the last function of the table.
  private lazy val noDomainsLibrary = compile("test_native_datasource_nd",
    "-DTABLE_SIZE=offsetof(SparkDsLibrary,push_domains)", "-DNAME_PREFIX=\"nd_\"")
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

  // The test libraries record the calls that the order of the calls of the header rules out,
  // which fail the test that made them.
  // Each test also checks that the buffers of its readers are freed, see checkReadersFreed, also
  // when it fails, and only those of its own readers: a reader that a failing test leaves behind
  // is reported with the failure of that test, and does not fail the next one.
  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and Java 22 or later are required.")
    val earlierReaders = readerAllocators()
    try {
      try {
        checkingOrderOfCalls(f)
      } catch {
        case e: Throwable =>
          try {
            checkReadersFreed(earlierReaders)
          } catch {
            case leak: Throwable => e.addSuppressed(leak)
          }
          throw e
      }
      checkReadersFreed(earlierReaders)
    } finally {
      SparkSession.setActiveSession(spark)
    }
  }

  /**
   * Checks that the buffers of the readers of native data sources are freed. A reader closed while
   * the batch that it returned may still be read, as when a query stops before the end of a
   * partition, frees the buffers of the batch once it is unreachable. Each test checks that they
   * are freed after it, so that a test that leaks a reader fails, and the readers of a test are
   * freed before the next test, see readerAndAllocator, and before the suites that run next in
   * this JVM, which may count the memory of Arrow. `earlierReaders` are those that were not freed
   * before the test, which a failed test may have left behind, and which it does not check.
   */
  private def checkReadersFreed(earlierReaders: Set[BufferAllocator]): Unit = {
    if ((readerAllocators() -- earlierReaders).nonEmpty) {
      eventually(timeout(1.minute), interval(100.milliseconds)) {
        System.gc()
        val readers = readerAllocators() -- earlierReaders
        assert(readers.isEmpty, readers.map(a => s"${a.getName}: ${a.getAllocatedMemory} bytes"))
      }
    }
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

  /** The allocators of the readers of native data sources that are not freed yet. */
  private def readerAllocators(): Set[BufferAllocator] = {
    ArrowUtils.rootAllocator.getChildAllocators.asScala
      .filter(_.getName.startsWith("native data source ")).toSet
  }

  /**
   * Creates the reader of a partition of the batch scan of `df`, and returns it with the allocator
   * of its buffers, which the reader closes once it frees them.
   */
  private def readerAndAllocator(
      df: DataFrame,
      partition: Int = 0): (PartitionReader[ColumnarBatch], BufferAllocator) = {
    val batch = scan(df).toBatch
    val (factory, partitions) = (batch.createReaderFactory(), batch.planInputPartitions())
    val before = readerAllocators()
    val reader = factory.createColumnarReader(partitions(partition))
    val created = readerAllocators() -- before
    if (created.size != 1) {
      reader.close()
      fail(s"The reader has ${created.size} allocators.")
    }
    (reader, created.head)
  }

  private def expectedRow(id: Long): Row = Row(
    id,
    (id % 3).toInt,
    if (id % 5 == 0) null else s"n$id",
    id * 0.5,
    id % 2 == 0,
    Date.valueOf(LocalDate.ofEpochDay(id)),
    Timestamp.from(Instant.ofEpochSecond(id)))

  /** Checks the failure of a next called once the reader of native_range was closed. */
  private def checkClosed(e: Throwable): Unit = e match {
    case e: SparkRuntimeException =>
      checkError(
        exception = e,
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map(
          "action" -> "read",
          "name" -> "native_range",
          "msg" -> "The partition reader is closed."))
    case _ => fail(s"Unexpected failure: $e", e)
  }

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
    // Closed before the query creates its next stream writer.
    NativeStreamWriters.closePrevious("query")
    assert(released == Seq(1L))
    NativeStreamWriters.register("query", second)
    NativeStreamWriters.closePrevious("new query")
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
    // The values of the macros of the header that the bindings check or pass, such as the
    // capabilities of a data source.
    val constants = Map(
      "SPARK_DS_BATCH_READ" -> NativeLibrary.BATCH_READ,
      "SPARK_DS_MICRO_BATCH_READ" -> NativeLibrary.MICRO_BATCH_READ,
      "SPARK_DS_BATCH_WRITE" -> NativeLibrary.BATCH_WRITE,
      "SPARK_DS_STREAMING_WRITE" -> NativeLibrary.STREAMING_WRITE,
      "SPARK_DS_BATCH_TRUNCATE" -> NativeLibrary.BATCH_TRUNCATE,
      "SPARK_DS_STREAMING_TRUNCATE" -> NativeLibrary.STREAMING_TRUNCATE,
      "SPARK_DS_WRITE_APPEND" -> NativeLibrary.WRITE_APPEND.toLong,
      "SPARK_DS_WRITE_TRUNCATE" -> NativeLibrary.WRITE_TRUNCATE.toLong,
      "SPARK_DS_READ_BATCH" -> NativeLibrary.READ_BATCH.toLong,
      "SPARK_DS_READ_MICRO_BATCH" -> NativeLibrary.READ_MICRO_BATCH.toLong)
    val prints = layout.keys.map { key =>
      val value = key.split('.') match {
        case Array(struct) => s"sizeof($struct)"
        case Array(struct, field) => s"offsetof($struct, $field)"
      }
      s"""  std::printf("%s %zu\\n", "$key", $value);"""
    } ++ constants.keys.map { name =>
      s"""  std::printf("%s %lld\\n", "$name", static_cast<long long>($name));"""
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
    assert(actual == layout.toMap ++ constants)
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
    // A data source compares strings by their bytes, so the column has no domain.
    val collated = session.read.schema("id BIGINT, name STRING COLLATE UTF8_LCASE")
      .format("native_range").option("end", "6").load()
    checkRows(collated.filter("name = 'N1'"), Seq(Row(1L, "n1")))
    assert(scan(collated.filter("name = 'N1'")).getMetaData()("PushedFilters") == "[]")

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

  nativeTest("export the schemas of a scan with the time zone of the session that planned it") {
    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "Asia/Tokyo")
    val df = range(session, "end" -> "4").select("id", "ts")
    // The reader gets the column ts as a timestamp of this time zone. The query then runs with
    // another one, which the executors get too: read fails unless the schema to read that they
    // export has the time zone that the reader got.
    df.queryExecution.executedPlan
    session.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "UTC")
    checkRows(df, (0L until 4L).map(id => Row(id, Timestamp.from(Instant.ofEpochSecond(id)))))
  }

  nativeTest("create the data writers with the time zone of their writer") {
    val session = newSession(defaultPackage)
    val (name, location) =
      NativeDataSourceRegistry.lookup("native_sink", session.sessionState.conf).get
    val dir = Utils.createTempDir()
    session.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "Asia/Tokyo")
    val writer = session.withActive {
      new NativeDataSource(location, name,
          new CaseInsensitiveStringMap(Map("path" -> dir.getPath).asJava))
        .writer(StructType.fromDDL("id BIGINT, ts TIMESTAMP"), overwrite = false)
    }
    // A data writer gets the schema that its writer got, with the time zone of its timestamps,
    // also when the session that creates it has another one: create_data_writer fails otherwise.
    session.conf.set(SQLConf.SESSION_LOCAL_TIMEZONE.key, "UTC")
    session.withActive {
      val dataWriter = writer.createWriter(0, 0)
      Utils.tryWithSafeFinally(dataWriter.abort())(dataWriter.close())
    }
    writer.abort(Array.empty)
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

    // The reader that the limit was pushed down to is also asked to prune the columns, after the
    // limit, and returns only the column that the query reads.
    val pruned = range(newSession(defaultPackage), "end" -> "100").select("id").limit(3)
    assert(pruned.collect().map(_.getLong(0)).toSeq == Seq(0L, 1L, 2L))
    assert(scan(pruned).readSchema() == StructType.fromDDL("id BIGINT"))
    assert(scan(pruned).getMetaData()("PushedLimit") == "LIMIT 3")
    assert(scan(pruned).toBatch.createReaderFactory().isInstanceOf[NativePartitionReaderFactory])
  }

  nativeTest("push down the domains of filtered columns") {
    val file = new File(Utils.createTempDir(), "domains")
    def readIn(session: SparkSession, options: (String, String)*): DataFrame = {
      file.delete()
      range(session, Seq("end" -> "45", "domains_path" -> file.getPath) ++ options: _*)
    }
    def read(options: (String, String)*): DataFrame = {
      readIn(newSession(defaultPackage), options: _*)
    }
    // The domains that the library got: for each, the path of the column, whether null and NaN are
    // in the domain, and the ranges.
    def received(): Seq[String] = Files.readAllLines(file.toPath, UTF_8).asScala.toSeq

    // The library reads the blocks of 10 rows whose ids may match, [10, 20) and [20, 30), with all
    // their rows, and Spark filters the rows after reading.
    val df = read().filter("id > 13 AND id < 27")
    checkRows(df, (14L until 27L).map(expectedRow))
    assert(received() == Seq("0 - - (13,27)"))
    assert(scan(df).toBatch.planInputPartitions().length == 2)
    val pushed = scan(df).getMetaData()("PushedFilters")
    assert(pushed.contains("id > 13") && pushed.contains("id < 27"), pushed)
    assert(scan(df).getMetaData()("PushedDomains") == "[id: (13L, 27L)]")

    // The library gets the domains of the other columns too, and only uses the one of id, so the
    // filters on the others are not shown as pushed.
    val other = read().filter("id < 5 AND name = 'n3'")
    checkRows(other, Seq(expectedRow(3L)))
    assert(received() == Seq("0 - - (-inf,5)", "2 - - [n3,n3]"))
    assert(scan(other).getMetaData()("PushedDomains") == "[id: (-inf, 5L)]")
    assert(!scan(other).getMetaData()("PushedFilters").contains("name"))
    val all = read("use_domains" -> "all").filter("id < 5 AND name = 'n3'")
    checkRows(all, Seq(expectedRow(3L)))
    assert(scan(all).getMetaData()("PushedFilters").contains("name = 'n3'"))
    // A library that uses no domain reads all the data.
    val none = read("use_domains" -> "none").filter("id > 40")
    checkRows(none, (41L until 45L).map(expectedRow))
    assert(scan(none).getMetaData()("PushedFilters") == "[]")
    assert(!scan(none).getMetaData().contains("PushedDomains"))
    assert(scan(none).toBatch.planInputPartitions().length == 2)

    // NaN is greater than any other value, so it is in the domain of value > 10.
    val doubles = read().filter("value > 10.0")
    checkRows(doubles, (21L until 45L).map(expectedRow))
    assert(received() == Seq("3 - nan (10,+inf)"))
    // LIKE 'n1%' is STARTS_WITH(name, 'n1'): the strings from 'n1', up to 'n2' excluded.
    val prefixed = read().filter("name LIKE 'n1%'")
    checkRows(prefixed, Seq(1L, 11L, 12L, 13L, 14L, 16L, 17L, 18L, 19L).map(expectedRow))
    assert(received() == Seq("2 - - [n1,n2)"))
    // A field of a struct, and timestamps, in microseconds.
    val nested = read("extra" -> "struct").filter("st.a >= 40 AND ts < timestamp_seconds(42)")
    checkRows(nested.select("id"), Seq(Row(40L), Row(41L)))
    assert(received() == Seq("6 - - (-inf,42000000)", "7.0 - - [40,+inf)"))
    // A domain without values: the library reads nothing.
    val empty = read().filter("id < 5 AND id > 10")
    checkRows(empty, Nil)
    assert(received() == Seq("0 - - "))
    assert(scan(empty).toBatch.planInputPartitions().isEmpty)
    // Domains with null, and with more than one range: the library reads the blocks that one of
    // the ranges of id may match.
    val nullSafe = read().filter("NOT (id <=> 5)")
    checkRows(nullSafe, (0L until 45L).filter(_ != 5).map(expectedRow))
    assert(received() == Seq("0 null - (-inf,5) (5,+inf)"))
    val nullNames = read().filter("name IS NULL")
    checkRows(nullNames, (0L until 45L by 5).map(expectedRow))
    assert(received() == Seq("2 null - "))
    val notIn = read().filter("id NOT IN (5, 20)")
    checkRows(notIn, (0L until 45L).filter(id => id != 5 && id != 20).map(expectedRow))
    assert(received() == Seq("0 - - (-inf,5) (5,20) (20,+inf)"))
    val between = read().filter("id < 3 OR (id > 22 AND id < 25)")
    checkRows(between, Seq(0L, 1L, 2L, 23L, 24L).map(expectedRow))
    assert(received() == Seq("0 - - (-inf,3) (22,25)"))
    assert(scan(between).toBatch.planInputPartitions().length == 2)
    // Filters on other expressions are not pushed: Spark evaluates them.
    val functions = read().filter("id % 7 = 0 AND upper(name) = 'N14'")
    checkRows(functions, Seq(expectedRow(14L)))
    val notPushed = scan(functions).getMetaData()("PushedFilters")
    assert(!notPushed.contains("%") && !notPushed.contains("UPPER"), notPushed)

    // A library compares strings by their bytes, so a column of a non-binary collation gets no
    // domain, even for a library that uses all the domains.
    file.delete()
    val collated = newSession(defaultPackage).read
      .schema("id BIGINT, name STRING COLLATE UTF8_LCASE").format("native_range")
      .options(Map("end" -> "6", "domains_path" -> file.getPath, "use_domains" -> "all")).load()
      .filter("name = 'N1' AND id < 5")
    checkRows(collated, Seq(Row(1L, "n1")))
    assert(received() == Seq("0 - - (-inf,5)"))
    assert(!scan(collated).getMetaData()("PushedFilters").contains("name"))

    val session = newSession(defaultPackage)
    session.conf.set(SQLConf.NATIVE_DATA_SOURCE_FILTER_PUSHDOWN_ENABLED.key, "false")
    val disabled = readIn(session).filter("id > 40")
    checkRows(disabled, (41L until 45L).map(expectedRow))
    assert(!file.exists())
    assert(scan(disabled).getMetaData()("PushedFilters") == "[]")
  }

  nativeTest("push no domain to a library built against a header without push_domains") {
    val file = new File(Utils.createTempDir(), "domains")
    val session = newSession(createPackage(noDomainsLibrary, dataSources("nd_")))
    val df = session.read.format("nd_native_range")
      .options(Map("end" -> "20", "domains_path" -> file.getPath)).load().filter("id > 15")
    checkRows(df, (16L until 20L).map(expectedRow))
    // push_domains is set in the table of the library, but after its end.
    assert(!file.exists())
    assert(scan(df).getMetaData()("PushedFilters") == "[]")
    // Nor does Spark create a reader to push them down while it optimizes the query: a reader
    // that the library cannot create only fails the read.
    val noReader = session.read.format("nd_native_range")
      .options(Map("end" -> "20", "zero" -> "create_reader")).load().filter("id > 15")
    noReader.queryExecution.optimizedPlan
    val e = intercept[Exception](noReader.collect())
    assert(Utils.exceptionString(e).contains("create_reader returned a NULL handle."))
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

  nativeTest("read the columns at the addresses of their buffers, and decimals as longs") {
    val df = range(newSession(defaultPackage), "end" -> "10", "partitions" -> "1",
      "extra" -> "decimal")
    assert(df.schema("d").dataType == DecimalType(10, 2))
    checkRows(df, (0L until 10L).map { id =>
      Row.fromSeq(expectedRow(id).toSeq :+ java.math.BigDecimal.valueOf(id * 100, 2))
    })
    val batch = scan(df).toBatch
    val reader = batch.createReaderFactory().createColumnarReader(batch.planInputPartitions().head)
    try {
      assert(reader.next())
      val columns = (0 until reader.get().numCols()).map(reader.get().column)
      // The columns of all these types read their buffers through a subclass of
      // NativeColumnVector.
      columns.foreach { column =>
        assert(column.isInstanceOf[NativeColumnVector] &&
          column.getClass != classOf[NativeColumnVector], column.getClass.getName)
      }
      // The decimals hold their unscaled longs, as those of Spark's own column vectors do.
      (0 until reader.get().numRows()).foreach { row =>
        assert(columns.last.getDecimal(row, 10, 2).toDebugString ===
          s"Decimal(compact, ${row * 100}, 10, 2)")
      }
    } finally {
      reader.close()
    }
  }

  nativeTest("reject the data that has nulls where the schema to read has none") {
    // The column name is null for the ids that are multiples of 5.
    val session = newSession(defaultPackage)
    def checkNullError(e: Throwable): Unit = checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "read",
        "name" -> "native_range",
        "msg" -> "The column name has a null, but the schema to read declares it not nullable."))
    checkNullError(intercept[Exception] {
      session.read.schema("id BIGINT, name STRING NOT NULL").format("native_range")
        .option("end", "4").load().collect()
    })
    // A batch that is rejected after others were read: the second partition reads the ids 6-7,
    // 8-9 and 10-11, and the name of id 10 is null.
    val df = session.read.schema("id BIGINT, name STRING NOT NULL").format("native_range")
      .options(Map("end" -> "12", "partitions" -> "2", "batch_size" -> "2")).load()
    val batch = scan(df).toBatch
    val reader = batch.createReaderFactory().createColumnarReader(batch.planInputPartitions()(1))
    try {
      assert(reader.next() && reader.next())
      assert(reader.get().numRows() == 2)
      assert(reader.get().column(1).getUTF8String(1).toString == "n9")
      checkNullError(intercept[Exception](reader.next()))
      // The batch has no rows, and its column vectors read the buffers of the rejected batch,
      // since the import released those of the previous one.
      assert(reader.get().numRows() == 0)
      assert(reader.get().column(1).getUTF8String(1).toString == "n11")
    } finally {
      reader.close()
    }
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
    // name, the cast is removed, so the batches go to Python as they are. With columnar input,
    // the UDF reads the batches of the reader, whose column vectors are ReusedArrowColumnVectors,
    // so it copies their pass-through columns rather than keep them past the next batch.
    Seq(false, true).foreach { columnarInput =>
      session.conf.set(
        SQLConf.ARROW_PYSPARK_UDF_COLUMNAR_INPUT_ENABLED.key, columnarInput.toString)
      Seq(100, 200, 200000).foreach { end =>
        val df = range(session, "end" -> end.toString, "partitions" -> "1", "batch_size" -> "100")
          .selectExpr("id", "name", "native_str(name) AS udf_name")
        val plan = df.queryExecution.executedPlan
        assert(plan.collectFirst { case e: ArrowEvalPythonExec => e.supportsColumnar }
          .contains(columnarInput), plan)
        val rows = df.collect()
        assert(rows.map(_.getLong(0)).toSeq == (0L until end))
        rows.foreach { row =>
          val id = row.getLong(0)
          val name = if (id % 5 == 0) null else s"n$id"
          assert(row.getString(1) == name, s"name mismatch for id $id")
          assert(row.getString(2) == name, s"UDF mismatch for id $id")
        }
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
    // Spark reads the last offset of a map through the pointer to its buffers, and the validity
    // of a batch through the one of the batch.
    check("The column m has 2 buffers, but a NULL pointer to them.", "extra" -> "map",
      "short" -> "buffers")
    check("The batch has 1 buffers, but a NULL pointer to them.", "short" -> "batch_buffers")
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

    // Nulls and the other types of the test library, from the Arrow batches of a native scan,
    // which the write gets without a transition to rows.
    val plans = QueryTest.withQueryExecutionsCaptured(session) {
      range(session, "end" -> "6").write.format("native_sink").option("path", dir.getPath)
        .mode("overwrite").save()
    }.flatMap(_.executedPlan.collectFirst { case write: V2TableWriteExec => write.query })
    assert(plans.length == 1 && plans.head.isInstanceOf[BatchScanExec], plans)
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
      session.range(10).repartition(2).write.format("native_sink").option("path", dir.getPath)
        .option("fail", "write").mode("append").save()
    }
    checkError(
      exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "write to",
        "name" -> "native_sink",
        "msg" -> "injected failure in write"))
    // Spark aborted the write with a null message for each of the two tasks, which both failed.
    assert(Files.readString(new File(dir, "_ABORTED").toPath) == "2,2,0")
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
      session.range(10).repartition(2).write.format("native_sink").option("path", dir.getPath)
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
    // Spark aborted the write with the writer, which it closed afterwards, with the messages of
    // the two tasks, which both committed: the library removes the data that they point to.
    assert(Files.readString(new File(dir, "_ABORTED").toPath) == "0,2,0")
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
    // closed, and Spark aborted the write with a null message for each of them.
    assert(Files.readString(new File(dir, "_ABORTED").toPath) == "2,2,0")
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
    // The task failed without aborting the committed data writer, which the test library would
    // record as a violation of the order of the calls, and closed it. The driver aborted the write.
    checkDataWritersClosed(dir)
    assert(Files.readString(new File(dir, "_ABORTED").toPath) == "1,1,0")
    assert(!dir.listFiles().exists(_.getName.startsWith("part-")))
  }

  nativeTest("keep the order of the calls whatever the callers of the adapter do") {
    val session = newSession(defaultPackage)
    def dataSource(name: String, options: (String, String)*): NativeDataSource = {
      val (resolved, location) =
        NativeDataSourceRegistry.lookup(name, session.sessionState.conf).get
      new NativeDataSource(location, resolved, new CaseInsensitiveStringMap(options.toMap.asJava))
    }
    val schema = StructType.fromDDL("id BIGINT")
    def newBatchWrite(dir: File, options: (String, String)*): ColumnarBatchWrite = {
      val write = new ColumnarBatchWrite(
        () => dataSource("native_sink", ("path" -> dir.getPath) +: options: _*)
          .writer(schema, false),
        schema)
      write.createBatchWriterFactory(PhysicalWriteInfoImpl(1))
      write
    }
    def newStreamWrite(dir: File, options: (String, String)*): ColumnarStreamingWrite = {
      val write = new ColumnarStreamingWrite("native_sink",
        () => dataSource("native_sink", ("path" -> dir.getPath) +: options: _*)
          .streamWriter(schema, false, UUID.randomUUID().toString),
        schema)
      write.createStreamingWriterFactory(PhysicalWriteInfoImpl(1))
      write
    }
    // The test library records each call that the header rules out, which fails the test.

    // A write or a micro-batch that committed is not aborted, and its writer is closed right away.
    val batchDir = Utils.createTempDir()
    val batchWrite = newBatchWrite(batchDir)
    batchWrite.commit(Array.empty)
    batchWrite.abort(Array.empty)
    assert(new File(batchDir, "_SUCCESS").isFile && !new File(batchDir, "_ABORTED").exists())
    assert(new File(batchDir, "_CLOSED").isFile)
    val streamDir = Utils.createTempDir()
    val streamWrite = newStreamWrite(streamDir)
    streamWrite.commit(0, Array.empty)
    streamWrite.abort(0, Array.empty)
    assert(new File(streamDir, "_EPOCH_0").isFile && !new File(streamDir, "_ABORTED_0").exists())
    assert(new File(streamDir, "_CLOSED_0").isFile)

    // A writer whose abort fails is closed all the same.
    def checkAbortError(abort: => Unit): Unit = checkError(
      exception = findError(intercept[Exception](abort), "NATIVE_DATA_SOURCE_ERROR"),
      condition = "NATIVE_DATA_SOURCE_ERROR",
      parameters = Map(
        "action" -> "abort a write to",
        "name" -> "native_sink",
        "msg" -> "injected failure in abort"))
    val failingBatchDir = Utils.createTempDir()
    val failingBatchWrite = newBatchWrite(failingBatchDir, "fail" -> "abort")
    checkAbortError(failingBatchWrite.abort(Array.empty))
    assert(!new File(failingBatchDir, "_ABORTED").exists())
    assert(new File(failingBatchDir, "_CLOSED").isFile)
    val failingStreamDir = Utils.createTempDir()
    val failingStreamWrite = newStreamWrite(failingStreamDir, "fail" -> "abort")
    checkAbortError(failingStreamWrite.abort(0, Array.empty))
    assert(!new File(failingStreamDir, "_ABORTED_0").exists())
    assert(new File(failingStreamDir, "_CLOSED_0").isFile)

    // After a write fails, a data writer only aborts and closes the data writer of the library.
    val failingDir = Utils.createTempDir()
    val writer = dataSource("native_sink", "path" -> failingDir.getPath, "fail" -> "write")
      .writer(schema, false)
    val dataWriter = writer.createWriter(0, 0)
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("test", 0, Long.MaxValue)
    val vector = new BigIntVector("id", allocator)
    try {
      vector.allocateNew(1)
      vector.set(0, 1L)
      vector.setValueCount(1)
      val batch = new ColumnarBatch(Array(new ArrowColumnVector(vector)), 1)
      val writeError = intercept[Exception](dataWriter.write(batch))
      assert(findError(writeError, "NATIVE_DATA_SOURCE_ERROR").getMessageParameters
        .get("msg") == "injected failure in write")
      Seq(() => dataWriter.write(batch), () => dataWriter.commit()).foreach { call =>
        checkError(
          exception = intercept[SparkException](call()),
          condition = "INTERNAL_ERROR",
          parameters = Map("message" -> "A write or a commit of the data writer failed."))
      }
      dataWriter.abort()
    } finally {
      dataWriter.close()
      vector.close()
      allocator.close()
      writer.abort(Array.empty)
    }

    // The reader of a scan prunes its columns once.
    val rangeSource = dataSource("native_range")
    val builder = new ColumnarScanBuilder("native_range", rangeSource.schema(), () => rangeSource)
    builder.pruneColumns(StructType.fromDDL("id BIGINT"))
    builder.pruneColumns(StructType.fromDDL("id BIGINT"))
    checkError(
      exception = intercept[SparkException] {
        builder.pruneColumns(StructType.fromDDL("name STRING"))
      },
      condition = "INTERNAL_ERROR",
      parameters = Map(
        "message" ->
          "The columns of a scan were pruned again, with columns that it does not read."))
    // Plans the scan, which releases the reader.
    builder.build().toBatch.planInputPartitions()
  }

  Seq(("write", "write to", "1,1,0"), ("commit", "commit a write to", "0,1,0")).foreach {
    case (fail, action, aborted) =>
      nativeTest(s"abort a micro-batch whose $fail failed") {
        val session = newSession(defaultPackage)
        val dir = Utils.createTempDir()
        val e = intercept[Exception] {
          val query = session.readStream.format("native_counter").option("max_offset", "4").load()
            .writeStream
            .format("native_sink")
            .option("path", dir.getPath)
            .option("fail", fail)
            .option("checkpointLocation", Utils.createTempDir().getPath)
            .start()
          try query.processAllAvailable() finally query.stop()
        }
        checkError(
          exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
          condition = "NATIVE_DATA_SOURCE_ERROR",
          parameters = Map(
            "action" -> action,
            "name" -> "native_sink",
            "msg" -> s"injected failure in $fail"))
        // Spark aborted the first epoch with its writer, which it closed afterwards: with a null
        // message for a task that did not commit, or the message of one that did.
        assert(Files.readString(new File(dir, "_ABORTED_0").toPath) == aborted)
        assert(!new File(dir, "_EPOCH_0").exists())
        assert(new File(dir, "_CLOSED_0").isFile)
      }
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
      // A micro-batch writes the Arrow batches of the scan, without a transition to rows.
      val plan =
        query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
      assert(plan.collectFirst { case write: V2TableWriteExec => write.query }
        .exists(_.isInstanceOf[MicroBatchScanExec]), plan)
      // Each micro-batch creates a new stream writer, which is closed once it committed, so the
      // writer of the last micro-batch is closed too while the query still runs.
      assert((0 to 2).forall(epoch => new File(dir, s"_CLOSED_$epoch").isFile))
    } finally {
      query.stop()
    }
    // Each micro-batch makes 5 more rows available after its start: [0, 5), [5, 10) and [10, 12).
    assert((0 to 2).forall(epoch => new File(dir, s"_EPOCH_$epoch").isFile))
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

  nativeTest("read up to the latest offset with Trigger.AvailableNow, also after a restart") {
    val session = newSession(defaultPackage)
    // Spark does not wrap the stream of a native data source, which supports the trigger itself,
    // even with the internal flag that wraps the others.
    session.conf.set("spark.sql.streaming.triggerAvailableNowWrapper.enabled", "true")
    val checkpoint = Utils.createTempDir()
    val dir = Utils.createTempDir()
    val out = Utils.createTempDir()
    // Runs the query with Trigger.AvailableNow, and returns the start offsets that the stream
    // reader got.
    def run(name: String): Seq[String] = {
      val startsPath = new File(dir, name)
      val query = session.readStream.format("native_counter")
        .option("max_offset", "12").option("step", "5").option("starts_path", startsPath.getPath)
        .load()
        .writeStream
        .format("native_sink")
        .option("path", out.getPath)
        .option("checkpointLocation", checkpoint.getPath)
        .trigger(Trigger.AvailableNow())
        .start()
      try query.awaitTermination() finally query.stop()
      Files.readAllLines(startsPath.toPath).asScala.toSeq
    }
    // Each run asks for the latest offset once, after the end of the last micro-batch, and reads
    // up to it: a restart neither reads the same rows again nor goes back to the initial offset.
    assert(run("first") == Seq("""{"offset":0}"""))
    assert(run("second") == Seq("""{"offset":5}"""))
    val written = out.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala)
    assert(written.map(_.toLong).sorted.toSeq == (0L until 10L))
  }

  nativeTest("limit the rows of each micro-batch with admission control") {
    val session = newSession(defaultPackage)
    // Runs the query with a trigger until it read the rows [0, 12), and returns the number of rows
    // of each micro-batch, the latest offset that the first one reported, and the start, the limit
    // and the end of the run of each call of latest_offset, as "limits_path" records them.
    def run(
        trigger: Option[Trigger],
        options: (String, String)*): (Seq[Long], String, Seq[String]) = {
      val limitsPath = new File(Utils.createTempDir(), "limits")
      val writer = session.readStream.format("native_counter").option("max_offset", "12")
        .option("limits_path", limitsPath.getPath)
        .options(options.toMap).load()
        .writeStream
        .format("noop")
        .option("checkpointLocation", Utils.createTempDir().getPath)
      val query = trigger.map(writer.trigger).getOrElse(writer).start()
      try {
        if (trigger.isDefined) query.awaitTermination() else query.processAllAvailable()
      } finally {
        query.stop()
      }
      val batches = query.recentProgress.filter(_.numInputRows > 0).toSeq
      (batches.map(_.numInputRows), batches.head.sources.head.latestOffset,
        Files.readAllLines(limitsPath.toPath).asScala.toSeq)
    }
    // Each micro-batch reads at most 5 rows, all available from the start, while the progress
    // reports the latest offset without the limit, which Spark asks for first. Spark only asks
    // with the limit while there is data after the start.
    val (rows, latest, calls) = run(None, "max_rows_per_trigger" -> "5")
    assert((rows, latest) == ((Seq(5L, 5L, 2L), """{"offset":12}""")))
    assert(calls.take(6) == Seq("0 all -", "0 5 -", "5 all -", "5 5 -", "10 all -", "10 5 -"))
    assert(calls.drop(6).nonEmpty && calls.drop(6).forall(_ == "12 all -"), calls)
    // A run of Trigger.AvailableNow reads the rows available when it starts, within the limit:
    // Spark asks for the end of the run once, without a limit, and passes it with the limit.
    assert(run(Some(Trigger.AvailableNow()), "max_rows_per_trigger" -> "5") ==
      ((Seq(5L, 5L, 2L), """{"offset":12}""", Seq("0 all -", "0 5 12", "5 5 12", "10 5 12"))))
    // Only the rows [0, 6) are available when the run starts, and native_counter makes 6 more
    // rows available after each start: the run stops at the end that it got when it started,
    // without asking again once it reached it.
    assert(run(Some(Trigger.AvailableNow()), "max_rows_per_trigger" -> "5", "step" -> "6") ==
      ((Seq(5L, 1L), """{"offset":6}""", Seq("0 all -", "0 5 6", "5 5 6"))))
    // Trigger.Once ignores the limit, and reads all the available rows in one micro-batch.
    assert(run(Some(Trigger.Once()), "max_rows_per_trigger" -> "5") ==
      ((Seq(12L), """{"offset":12}""", Seq("0 all -"))))
    // Without the capability, Spark asks for all the available rows, once for each micro-batch.
    val (unlimitedRows, _, unlimitedCalls) = run(None, "max_rows_per_trigger" -> "5",
      "capabilities" -> NativeLibrary.MICRO_BATCH_READ.toString)
    assert(unlimitedRows == Seq(12L))
    assert(unlimitedCalls.head == "0 all -" && unlimitedCalls.tail.forall(_ == "12 all -"),
      unlimitedCalls)
    // So it does with the capability, when the default read limit is all the available rows.
    val limited = NativeLibrary.MICRO_BATCH_READ | NativeLibrary.ADMISSION_CONTROL
    assert(run(None, "capabilities" -> limited.toString)._1 == Seq(12L))
  }

  nativeTest("start a new run of Trigger.AvailableNow with a limit when the query restarts") {
    val session = newSession(defaultPackage)
    val checkpoint = Utils.createTempDir()
    val dir = Utils.createTempDir()
    // The rows of each micro-batch that succeeded, with its ID.
    val batches = ArrayBuffer.empty[(Long, Seq[Long])]
    // Runs the query, which records the start, the limit and the end of the run of each call of
    // latest_offset in the file `limits`.
    def run(failingBatch: Long, limits: String): Unit = {
      val writeBatch: (Dataset[Row], Long) => Unit = { (df, id) =>
        if (id == failingBatch) throw new IllegalStateException(s"micro-batch $id fails")
        batches += id -> df.collect().map(_.getLong(0)).toSeq.sorted
      }
      // A run from 0 ends at 6, since native_counter makes 6 more rows available after each start.
      val query = session.readStream.format("native_counter")
        .option("max_offset", "12").option("step", "6").option("max_rows_per_trigger", "5")
        .option("limits_path", new File(dir, limits).getPath)
        .load()
        .writeStream
        .trigger(Trigger.AvailableNow())
        .option("checkpointLocation", checkpoint.getPath)
        .foreachBatch(writeBatch)
        .start()
      try query.awaitTermination() finally query.stop()
    }
    def calls(limits: String): Seq[String] =
      Files.readAllLines(new File(dir, limits).toPath).asScala.toSeq
    // The second micro-batch, [5, 6), fails, so it is planned but not committed.
    val e = intercept[Exception](run(failingBatch = 1, "first"))
    assert(Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null)
      .exists(_.getMessage == "micro-batch 1 fails"))
    assert(batches.toSeq == Seq(0L -> (0L until 5L)))
    assert(calls("first") == Seq("0 all -", "0 5 6", "5 5 6"))
    // The new run first runs that micro-batch again, up to its end in the offset log, without
    // asking for a latest offset, then asks for a new end, 12, and reads up to it within the limit.
    run(failingBatch = -1, "second")
    assert(batches.toSeq ==
      Seq(0L -> (0L until 5L), 1L -> Seq(5L), 2L -> (6L until 11L), 3L -> Seq(11L)))
    assert(calls("second") == Seq("6 all -", "6 5 12", "11 5 12"))
  }

  nativeTest("reject the read limits that a micro-batch cannot have") {
    // Checks the error of a query, and returns whether its stream reader was closed then.
    def check(format: String, options: Map[String, String], msg: String): Boolean = {
      val packages = if (format.startsWith("nrl_")) {
        createPackage(noReadLimitLibrary, dataSources("nrl_"))
      } else {
        defaultPackage
      }
      val closedPath = new File(Utils.createTempDir(), "closed")
      val e = intercept[Exception] {
        val query = newSession(packages).readStream.format(format).options(options)
          .option("closed_path", closedPath.getPath)
          .load()
          .writeStream
          .format("noop")
          .option("checkpointLocation", Utils.createTempDir().getPath)
          .start()
        try query.processAllAvailable() finally query.stop()
      }
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> "plan a scan of", "name" -> format, "msg" -> msg))
      closedPath.isFile
    }
    // Spark asks for the limit before it records the streams to stop when the query stops, so the
    // stream closes its reader itself when the limit fails.
    assert(check("native_counter", Map("bad_read_limit" -> "kind"),
      "default_read_limit returned the unknown kind of read limit 7."))
    assert(check("native_counter", Map("bad_read_limit" -> "rows"),
      "default_read_limit returned a maximum of 0 rows, but a micro-batch reads at least 1 row."))
    // A data source must not declare a capability whose functions are NULL. Spark checks them
    // before it creates the stream reader.
    assert(!check("nrl_native_counter", Map("max_rows_per_trigger" -> "5"),
      "The library does not implement default_read_limit."))
  }

  nativeTest("ask for the initial offset once, and keep it in the checkpoint") {
    val session = newSession(defaultPackage)
    val checkpoint = Utils.createTempDir()
    val dir = Utils.createTempDir()
    // The initial offset of the stream reader is the number of times that it was asked before, as
    // for an initial offset that depends on when it is asked.
    val initialOffsets = new File(Utils.createTempDir(), "initial_offsets")
    def run(fail: Boolean): Unit = {
      val writer = session.readStream.format("native_counter")
        .option("max_offset", "4").option("initial_offsets_path", initialOffsets.getPath)
        .load()
        .writeStream
        .format("native_sink")
        .option("path", dir.getPath)
        .option("checkpointLocation", checkpoint.getPath)
      val query = (if (fail) writer.option("fail", "write") else writer).start()
      try query.processAllAvailable() finally query.stop()
    }
    // The first micro-batch fails, so it is not committed. A query that starts without a
    // checkpoint asks for the initial offset once, for the start of the first latest offset and
    // of the first micro-batch.
    val e = intercept[Exception](run(fail = true))
    findError(e, "NATIVE_DATA_SOURCE_ERROR")
    assert(Files.readAllLines(initialOffsets.toPath).size == 1)
    assert(new File(checkpoint, s"sources/0/${ColumnarMicroBatchStream.InitialOffsetLog}")
      .isDirectory)
    // The restarted query plans the first micro-batch again from the initial offset that the
    // checkpoint kept, without asking the stream reader.
    run(fail = false)
    assert(Files.readAllLines(initialOffsets.toPath).size == 1)
    val written = dir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala)
    assert(written.map(_.toLong).sorted.toSeq == (0L until 4L))
  }

  nativeTest("pass only the columns to read to the stream reader") {
    val session = newSession(defaultPackage)
    val columnsPath = new File(Utils.createTempDir(), "columns")
    val relation = session.readStream.format("native_counter").schema("id BIGINT, mod INT")
      .option("columns_path", columnsPath.getPath).load()
      .queryExecution.analyzed.collectFirst { case relation: StreamingRelationV2 => relation }.get
    val builder = relation.table.asInstanceOf[SupportsRead].newScanBuilder(relation.extraOptions)
    builder.asInstanceOf[SupportsPushDownRequiredColumns]
      .pruneColumns(StructType.fromDDL("mod INT"))
    val scan = builder.build()
    val stream = scan.toMicroBatchStream(Utils.createTempDir().getPath)
    try {
      assert(Files.readString(columnsPath.toPath) == "mod")
      assert(scan.readSchema() == StructType.fromDDL("mod INT"))
      // read gets these columns as the schema to read, which the test library checks, and its
      // batches only have them.
      val start = stream.initialOffset()
      val end = stream.asInstanceOf[SupportsAdmissionControl]
        .latestOffset(start, ReadLimit.allAvailable())
      val factory = stream.createReaderFactory()
      val mods = stream.planInputPartitions(start, end).toSeq.flatMap { partition =>
        val reader = factory.createColumnarReader(partition)
        try {
          Iterator.continually(reader.next()).takeWhile(identity).flatMap { _ =>
            val batch = reader.get()
            assert(batch.numCols() == 1)
            (0 until batch.numRows()).map(batch.column(0).getInt)
          }.toList
        } finally {
          reader.close()
        }
      }
      assert(mods == (0 until 10).map(_ % 3))
    } finally {
      stream.stop()
    }
  }

  nativeTest("plan each micro-batch once, also when a query reads a source twice") {
    val session = newSession(defaultPackage)
    val ranges = new File(Utils.createTempDir(), "ranges")
    val counter = session.readStream.format("native_counter")
      .option("max_offset", "6").option("step", "3").option("ranges_path", ranges.getPath)
      .load()
    // Both sides of the union read the stream of the source. The partitions and the state of each
    // stream_partitions call have the number of the call, so read fails if it gets a partition of
    // a call with the state of another.
    val query = counter.union(counter).writeStream.format("memory")
      .queryName("native_counter_self_union").start()
    try {
      query.processAllAvailable()
      checkRows(session.table("native_counter_self_union"),
        (0L until 6L).flatMap(id => Seq(Row(id), Row(id))))
    } finally {
      query.stop()
    }
    // stream_partitions is called once for each micro-batch.
    assert(Files.readAllLines(ranges.toPath).asScala.toSeq ==
      Seq("""{"offset":0} {"offset":3}""", """{"offset":3} {"offset":6}"""))

    // Spark may plan the scans of a micro-batch on several threads at the same time, such as the
    // side of a broadcast join, and they still get the partitions of a single call: the stream of
    // a columnar data source plans each range once.
    val (counterName, counterLocation) =
      NativeDataSourceRegistry.lookup("native_counter", session.sessionState.conf).get
    def stream(options: (String, String)*): ColumnarMicroBatchStream = {
      val checkpoint = Utils.createTempDir().getPath
      val reader = new NativeDataSource(counterLocation, counterName,
          new CaseInsensitiveStringMap(options.toMap.asJava))
        .streamReader(StructType.fromDDL("id BIGINT"), checkpoint)
      new ColumnarMicroBatchStream(reader, checkpoint)
    }
    def offset(i: Int): Offset = ColumnarStreamOffset(s"""{"offset":$i}""")
    val concurrentRanges = new File(Utils.createTempDir(), "ranges")
    val concurrent = stream("max_offset" -> "100", "ranges_path" -> concurrentRanges.getPath)
    try {
      (0 until 20).foreach { i =>
        val planning = new CyclicBarrier(2)
        val planned = ThreadUtils.parmap(1 to 2, "plan-micro-batch", 2) { _ =>
          planning.await()
          concurrent.planInputPartitions(offset(i), offset(i + 1))
        }
        assert(planned.head eq planned.last)
      }
    } finally {
      concurrent.stop()
    }
    assert(Files.readAllLines(concurrentRanges.toPath).size == 20)
    // So does a failure: planning the range again fails with the same error, without calling the
    // library again on the handle whose call failed.
    val failing = stream("fail" -> "stream_partitions")
    try {
      val failure = intercept[SparkRuntimeException] {
        failing.planInputPartitions(offset(0), offset(1))
      }
      assert(failure.getMessage.contains("injected failure in stream_partitions"))
      assert(intercept[SparkRuntimeException] {
        failing.planInputPartitions(offset(0), offset(1))
      } eq failure)
    } finally {
      failing.stop()
    }
  }

  nativeTest("reject offsets that are not JSON on a single line") {
    // Spark writes each offset as a line of the offset log of the checkpoint, and the initial
    // offset in the checkpoint of the source.
    def check(option: String, badOffset: String, msg: String): Unit = {
      val e = intercept[Exception] {
        val query = newSession(defaultPackage).readStream.format("native_counter")
          .option(option, badOffset).load()
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
    Seq(("bad_offset", "latest_offset", 10), ("bad_initial_offset", "initial_offset", 0)).foreach {
      case (option, function, offset) =>
        check(option, "none", s"$function returned no offset.")
        check(option, "multiline", s"$function returned an offset with a line break, but an " +
          "offset must be JSON on a single line.")
        check(option, "invalid",
          s"$function returned an offset that is not valid JSON: offset $offset")
    }
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
    // Spark releases the ranges of the domains that it pushes down, after a failure of push_domains
    // as after a success.
    val allocated = ArrowUtils.rootAllocator.getAllocatedMemory
    check("plan a scan of", "injected failure in push_domains") { session =>
      range(session, "end" -> "5", "fail" -> "push_domains").where("id IN (1, 3)").collect()
    }
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocated)
    checkRows(range(newSession(defaultPackage), "end" -> "5").where("id IN (1, 3)").select("id"),
      Seq(Row(1L), Row(3L)))
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocated)
    // A library may also move the ranges to take ownership of them, and keep them in its reader:
    // they stay allocated until the library releases them, here when its reader is closed, once
    // the partitions are planned, which read them again.
    val movingSession = newSession(defaultPackage)
    val (rangeName, rangeLocation) =
      NativeDataSourceRegistry.lookup("native_range", movingSession.sessionState.conf).get
    val moving = new NativeDataSource(rangeLocation, rangeName,
      new CaseInsensitiveStringMap(Map("end" -> "5", "move_domains" -> "true").asJava))
    val builder = new ColumnarScanBuilder(rangeName, moving.schema(), () => moving)
    val in = new Predicate("IN", Array[V2Expression](FieldReference("id"),
      LiteralValue(1L, LongType), LiteralValue(3L, LongType)))
    movingSession.withActive(builder.pushPredicates(Array(in)))
    assert(ArrowUtils.rootAllocator.getAllocatedMemory > allocated)
    assert(builder.build().toBatch.planInputPartitions().nonEmpty)
    assert(ArrowUtils.rootAllocator.getAllocatedMemory == allocated)
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
    val dir = Utils.createTempDir()
    check("plan a write to", "native_sink", "create_writer") { session =>
      session.range(2).write.format("native_sink").option("path", dir.getPath)
        .option("states", "2").mode("append").save()
    }
    // The rejected writer is aborted, as rejected and with no messages, so that the library can
    // undo what create_writer prepared, and released.
    assert(Files.readString(new File(dir, "_ABORTED").toPath) == "0,0,1")
    assert(new File(dir, "_CLOSED").isFile)
    // So is the rejected writer of a micro-batch, with an epoch ID of -1, before its epoch.
    val streamDir = Utils.createTempDir()
    check("plan a write to", "native_sink", "create_writer") { session =>
      val query = session.readStream.format("native_counter").option("max_offset", "4").load()
        .writeStream
        .format("native_sink")
        .option("path", streamDir.getPath)
        .option("states", "2")
        .option("checkpointLocation", Utils.createTempDir().getPath)
        .start()
      try query.processAllAvailable() finally query.stop()
    }
    assert(Files.readString(new File(streamDir, "_ABORTED").toPath) == "0,0,1")
    assert(!new File(streamDir, "_ABORTED_0").exists())
    assert(new File(streamDir, "_CLOSED").isFile)
  }

  nativeTest("call no other function on a reader whose call failed than its close function") {
    def checkFailure(step: String)(f: => Unit): Unit = {
      val e = intercept[Exception](f)
      assert(findError(e, "NATIVE_DATA_SOURCE_ERROR").getMessageParameters.get("msg") ==
        s"injected failure in $step")
    }
    // The test library records the calls after a failed one, which fail the test.
    Seq("push_limit" -> ((df: DataFrame) => df.limit(1)),
        "prune_columns" -> ((df: DataFrame) => df.select("id"))).foreach { case (step, query) =>
      checkFailure(step) {
        query(range(newSession(defaultPackage), "fail" -> step)).collect()
      }
    }
    // A stream reader is closed after one of its functions failed, when the query stops. The
    // offsets of the first micro-batch are committed when the second one runs.
    Seq("initial_offset", "latest_offset", "stream_partitions", "commit_offset").foreach { step =>
      val closedPath = new File(Utils.createTempDir(), "closed")
      checkFailure(step) {
        val query = newSession(defaultPackage).readStream.format("native_counter")
          .option("max_offset", "12").option("step", "5").option("fail", step)
          .option("closed_path", closedPath.getPath)
          .load()
          .writeStream
          .format("noop")
          .option("checkpointLocation", Utils.createTempDir().getPath)
          .start()
        try query.processAllAvailable() finally query.stop()
      }
      assert(closedPath.isFile, s"the stream reader is not closed after $step failed")
    }
  }

  nativeTest("read and write without a state") {
    // A reader or a writer without a state does not call set_state. read then fails unless it
    // gets a state of length 0, and takes the columns from the schema to read. A data writer that
    // gets a state of length 0 passes its rows in its message, and commit fails unless the
    // messages have rows.
    val session = newSession(defaultPackage)
    checkRows(range(session, "end" -> "10", "stateless" -> "true"),
      (0L until 10L).map(expectedRow))
    checkRows(range(session, "end" -> "10", "stateless" -> "true").select("name", "id"),
      (0L until 10L).map(id => Row(if (id % 5 == 0) null else s"n$id", id)))
    val query = session.readStream.format("native_counter")
      .option("max_offset", "6").option("step", "3").option("stateless", "true").load()
      .writeStream.format("memory").queryName("native_counter_stateless").start()
    try {
      query.processAllAvailable()
      checkRows(session.table("native_counter_stateless"), (0L until 6L).map(Row(_)))
    } finally {
      query.stop()
    }
    val dir = Utils.createTempDir()
    range(session, "end" -> "4", "stateless" -> "true").select("id", "name")
      .write.format("native_sink").option("path", dir.getPath).option("stateless", "true")
      .mode("append").save()
    val written = dir.listFiles().filter(_.getName.startsWith("part-"))
      .flatMap(file => Files.readAllLines(file.toPath).asScala)
    assert(written.sorted.toSeq == Seq("0,null", "1,n1", "2,n2", "3,n3"))
    assert(Files.readString(new File(dir, "_SUCCESS").toPath) == "2")
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
    // The library does not push anything down, but read still gets only the column that the
    // query reads, which it returns as the contract requires, and Spark applies the limit.
    val df = session.read.format("ro_native_range").option("end", "10").load()
    val limited = df.select("id").limit(3)
    val ids = limited.collect().map(_.getLong(0))
    assert(ids.length == 3 && ids.forall(id => id >= 0 && id < 10))
    assert(scan(limited).readSchema() == StructType.fromDDL("id BIGINT"))
    assert(!scan(limited).getMetaData().contains("PushedLimit"))
    assert(scan(limited).toBatch.createReaderFactory().isInstanceOf[NativePartitionReaderFactory])
    // A read that returns other columns than those of the schema to read fails.
    val allColumns = intercept[Exception] {
      session.read.format("ro_native_range").option("end", "10")
        .option("ignore_read_schema", "true").load().select("id").collect()
    }
    assert(Utils.exceptionString(allColumns).contains("NATIVE_DATA_SOURCE_ERROR"))
    assert(Utils.exceptionString(allColumns).contains("but the expected schema is id BIGINT"))
    // Nor does it push any filter down: Spark evaluates them after reading.
    val filtered = df.filter("id > 6")
    checkRows(filtered, (7L until 10L).map(expectedRow))
    assert(scan(filtered).getMetaData()("PushedFilters") == "[]")

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
    // The library records each call of log, with its level, in a file.
    val logCalls = new File(Utils.createTempDir(), "log_calls")
    withLogAppender(logAppender, loggerNames = Seq(classOf[NativeLibrary].getName)) {
      range(session, "log" -> "a message from native code", "log_calls_path" -> logCalls.getPath)
    }
    // Each data source that the read creates logs, at SPARK_DS_LOG_WARN.
    assert(Files.readAllLines(logCalls.toPath).asScala.toSet == Set("2"))
    assert(logAppender.loggingEvents.exists { event =>
      val message = event.getMessage.getFormattedMessage
      event.getLevel == org.apache.logging.log4j.Level.WARN &&
        message.contains("a message from native code") &&
        message.contains(System.mapLibraryName("test_native_datasource"))
    })
    // The library does not even call log for a warning when Spark logs errors only.
    val errorAppender = new LogAppender("native data source error logs")
    val errorLogCalls = new File(Utils.createTempDir(), "log_calls")
    withLogAppender(errorAppender, loggerNames = Seq(classOf[NativeLibrary].getName),
        level = Some(org.apache.logging.log4j.Level.ERROR)) {
      range(session, "log" -> "a message that Spark does not log",
        "log_calls_path" -> errorLogCalls.getPath)
    }
    assert(!errorLogCalls.exists())
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

    // A read gets the index of its partition in the scan, also when a task reads several
    // partitions, or the partitions of several scans: the task attempt that read each partition.
    def readers(dir: File): Map[Int, Long] = dir.listFiles().map { file =>
      file.getName.toInt -> Files.readString(file.toPath).split(",")(5).toLong
    }.toMap
    val (first, second) = (Utils.createTempDir(), Utils.createTempDir())
    checkRows(
      range(session, "end" -> "6", "task_path" -> first.getPath).select("id")
        .union(range(session, "end" -> "6", "task_path" -> second.getPath).select("id")),
      (0L until 6L).flatMap(i => Seq(Row(i), Row(i))))
    assert(readers(first).keySet == Set(0, 1) && readers(second).keySet == Set(0, 1))
    val coalesced = Utils.createTempDir()
    checkRows(range(session, "end" -> "6", "task_path" -> coalesced.getPath).select("id")
      .coalesce(1), (0L until 6L).map(Row(_)))
    assert(readers(coalesced).keySet == Set(0, 1) && readers(coalesced).values.toSet.size == 1)
    // So does a read of a micro-batch.
    val (firstStream, secondStream) = (Utils.createTempDir(), Utils.createTempDir())
    def counter(dir: File): DataFrame = session.readStream.format("native_counter")
      .option("max_offset", "4").option("task_path", dir.getPath).load()
    val query = counter(firstStream).union(counter(secondStream)).writeStream.format("noop").start()
    try query.processAllAvailable() finally query.stop()
    assert(readers(firstStream).keySet == Set(0) && readers(secondStream).keySet == Set(0))
  }

  nativeTest("open the partitions of batch and micro-batch scans as Arrow C streams") {
    val session = newSession(defaultPackage)
    // The reader factory and the partitions of the batch scan of a query, which an engine gets
    // from its plan.
    def plannedScan(df: DataFrame): (SupportsArrowStreamRead, Seq[InputPartition]) = {
      val scan = df.queryExecution.executedPlan.collectFirst { case s: BatchScanExec => s }.get
      (scan.readerFactory.asInstanceOf[SupportsArrowStreamRead], scan.inputPartitions)
    }
    // The batch scan of only some of the columns from a library that does not prune the others
    // still reads only them through its streams, since read returns exactly the columns of its
    // schema to read.
    val readOnly = newSession(defaultPackage, createPackage(readOnlyLibrary, dataSources("ro_")))
    val (projectedFactory, projectedPartitions) =
      plannedScan(readOnly.read.format("ro_native_range").option("end", "4").load().select("id"))
    val projectedReads = readOnly.artifactManager.withResources {
      readOnly.sparkContext.parallelize(Seq(0), 1).flatMap { _ =>
        projectedPartitions.map { partition =>
          val stream = ArrowArrayStream.allocateNew(ArrowUtils.rootAllocator)
          try {
            projectedFactory.openArrowStream(partition, stream.memoryAddress())
            try TestArrowEngine.readNamesAndIdsInPlace(stream) finally stream.release()
          } finally {
            stream.close()
          }
        }
      }.collect().toSeq
    }
    assert(projectedReads.map(_._1).distinct == Seq(Seq("id")))
    assert(projectedReads.flatMap(_._2).sorted == (0L until 4L))
    assert(NativeLibrary.openEngineStreams() == 0)
    // Runs `f` in a task that only gets the files of the session. It is the task of the partition
    // 1, so that the partition of the scan that a read gets is not the partition of its task.
    def inTask[T: ClassTag](f: => T): T = session.artifactManager.withResources {
      session.sparkContext.parallelize(Seq(0, 1), 2).flatMap(i => if (i == 1) Seq(f) else Nil)
        .collect().head
    }
    import TestArrowEngine.{callbacks, readIds, readIdsInPlace}
    // Reads the given partitions in a single task, as an engine does.
    def readAll(factory: SupportsArrowStreamRead, partitions: Seq[InputPartition]): Seq[Long] = {
      inTask {
        partitions.flatMap { partition =>
          val allocator = ArrowUtils.rootAllocator.newChildAllocator("engine", 0, Long.MaxValue)
          try {
            val stream = ArrowArrayStream.allocateNew(allocator)
            try factory.openArrowStream(partition, stream.memoryAddress()) catch {
              case e: Throwable =>
                stream.close()
                throw e
            }
            readIds(stream, allocator)
          } finally {
            allocator.close()
          }
        }
      }
    }
    // Checks that `f` opens at most `delta` more task contexts than it closes. The count is global
    // to the JVM, where the tasks of earlier tests may still close their contexts, so it is read
    // right before `f`, and may be lower afterwards.
    def checkContexts[T](delta: Int)(f: => T): T = {
      val before = NativeLibrary.openTaskContexts()
      val result = f
      assert(NativeLibrary.openTaskContexts() <= before + delta)
      result
    }
    // The size of struct ArrowArrayStream, which the layout test compares with the header.
    val streamSize = NativeLibrary.layout().get("ArrowArrayStream").longValue

    // An engine reads both partitions of the scan in a single task.
    val taskDir = Utils.createTempDir()
    val (factory, partitions) =
      plannedScan(range(session, "end" -> "6", "task_path" -> taskDir.getPath).select("id"))
    assert(partitions.length == 2)
    assert(readAll(factory, partitions).sorted == (0L until 6L))
    // The library got the partition of the scan that it reads, not the one of the task.
    val tasks = taskDir.listFiles().map { file =>
      file.getName.toInt -> Files.readString(file.toPath).split(",").map(_.toLong).toSeq
    }.toMap
    assert(tasks.keySet == Set(0, 1) && tasks.values.map(_(5)).toSet.size == 1, tasks)
    assert(NativeLibrary.openEngineStreams() == 0)
    // The reads of a task share the flag that tells them that the task ended, with a single pair
    // of listeners, which remove it once the task ended. The tasks of earlier tests may still
    // be ending.
    eventually(timeout(1.minute))(assert(NativeTask.tasksWithEndedFlags == 0))
    val flagsInTask = inTask {
      val ids = partitions.flatMap { partition =>
        val stream = ArrowArrayStream.allocateNew(ArrowUtils.rootAllocator)
        try {
          factory.openArrowStream(partition, stream.memoryAddress())
          try readIdsInPlace(stream) finally stream.release()
        } finally {
          stream.close()
        }
      }
      assert(ids.sorted == (0L until 6L))
      NativeTask.tasksWithEndedFlags
    }
    assert(flagsInTask == 1 && NativeTask.tasksWithEndedFlags == 0)

    // The engine may release the stream after its task ended, and on another thread: Spark frees
    // the context of the task once the library released the stream.
    val released = new File(Utils.createTempDir(), "released")
    val (laterFactory, laterPartitions) =
      plannedScan(range(session, "end" -> "6", "released_path" -> released.getPath).select("id"))
    // Opens a stream in a task, and returns it unreleased, with what `read` returned from it in
    // the task.
    def openInTask[T: ClassTag](factory: SupportsArrowStreamRead, partition: InputPartition)(
        read: ArrowArrayStream => T): (Long, T) = inTask {
      val stream = Platform.allocateMemory(streamSize)
      Platform.setMemory(stream, 0.toByte, streamSize)
      factory.openArrowStream(partition, stream)
      (stream, read(ArrowArrayStream.wrap(stream)))
    }
    // The engine reads the stream to its end in the task, but releases it later.
    val (address, readInTask) =
      checkContexts(1)(openInTask(laterFactory, laterPartitions.head)(readIdsInPlace))
    // A copy of the stream, as an engine that moves the stream may leave behind.
    val copy = Platform.allocateMemory(streamSize)
    Utils.tryWithSafeFinally {
      Platform.copyMemory(null, address, null, copy, streamSize)
      assert(readInTask == (0L until 3L))
      assert(NativeLibrary.openEngineStreams() == 1 && !released.exists())
      checkContexts(-1) {
        ArrowArrayStream.wrap(address).release()
        assert(NativeLibrary.openEngineStreams() == 0 && released.exists())
      }
      // Once the stream is released, the callbacks of the copy fail with EINVAL, and its release
      // only marks it released.
      checkContexts(0) {
        val array = ArrowArray.allocateNew(ArrowUtils.rootAllocator)
        val stale = try intercept[Exception](ArrowArrayStream.wrap(copy).getNext(array)) finally {
          array.close()
        }
        assert(Utils.exceptionString(stale)
          .contains("The stream was released, or is not a stream of Spark."))
        ArrowArrayStream.wrap(copy).release()
        assert(ArrowArrayStream.wrap(copy).snapshot().release == 0)
        assert(NativeLibrary.openEngineStreams() == 0)
      }
    } {
      // A failure above may leave the stream open, which would keep the context of its task.
      if (ArrowArrayStream.wrap(address).snapshot().release != 0) {
        ArrowArrayStream.wrap(address).release()
      }
      Platform.freeMemory(address)
      Platform.freeMemory(copy)
    }

    // Once the task ended, its context tells the library that it was interrupted, so that the
    // library stops waiting.
    val marker = new File(Utils.createTempDir(), "interrupted")
    val (waitingFactory, waitingPartitions) = plannedScan(
      range(session, "end" -> "6", "wait_for_interrupt" -> marker.getPath).select("id"))
    checkContexts(0) {
      val (waiting, _) = openInTask(waitingFactory, waitingPartitions.head)(_ => ())
      val error = intercept[Exception] {
        readIds(ArrowArrayStream.wrap(waiting), ArrowUtils.rootAllocator)
      }
      assert(Utils.exceptionString(error).contains("the task was killed"))
      assert(Files.readString(marker.toPath) == "interrupted")
      assert(NativeLibrary.openEngineStreams() == 0)
      Platform.freeMemory(waiting)
    }

    // So does the context of a stream that an engine reads in its task once the task is killed,
    // although the task did not end.
    val killedMarker = new File(Utils.createTempDir(), "interrupted")
    val (killedFactory, killedPartitions) = plannedScan(range(session, "end" -> "6",
      "partitions" -> "1", "wait_for_interrupt" -> killedMarker.getPath).select("id"))
    val contextsBeforeKill = NativeLibrary.openTaskContexts()
    val jobGroup = s"native-data-source-${UUID.randomUUID()}"
    val killError = new AtomicReference[Throwable]()
    val engineThread = new Thread(() => {
      session.sparkContext.setJobGroup(jobGroup, "an engine that waits", interruptOnCancel = true)
      try {
        inTask {
          // Reads the stream on the thread of the task, which waits in get_next.
          val stream = ArrowArrayStream.allocateNew(ArrowUtils.rootAllocator)
          try {
            killedFactory.openArrowStream(killedPartitions.head, stream.memoryAddress())
            try readIdsInPlace(stream) finally stream.release()
          } finally {
            stream.close()
          }
        }
      } catch {
        case e: Throwable => killError.set(e)
      }
    })
    engineThread.start()
    eventually(timeout(1.minute)) {
      assert(new File(killedMarker.getPath + ".waiting").isFile)
    }
    session.sparkContext.cancelJobGroup(jobGroup)
    engineThread.join(TimeUnit.MINUTES.toMillis(1))
    assert(!engineThread.isAlive && killError.get() != null)
    eventually(timeout(1.minute)) {
      assert(killedMarker.isFile && Files.readString(killedMarker.toPath) == "interrupted")
      assert(NativeLibrary.openEngineStreams() == 0)
      assert(NativeLibrary.openTaskContexts() <= contextsBeforeKill)
    }

    // So does the reader factory of a micro-batch scan, with the state of its micro-batch.
    val streamTaskDir = Utils.createTempDir()
    val query = session.readStream.format("native_counter").option("max_offset", "4")
      .option("task_path", streamTaskDir.getPath).load()
      .writeStream.format("noop").start()
    val microBatchScan = try {
      query.processAllAvailable()
      query.asInstanceOf[StreamingQueryWrapper].streamingQuery.lastExecution.executedPlan
        .collectFirst { case s: MicroBatchScanExec => s }.get
    } finally {
      query.stop()
    }
    // Only the reads of the engine record their tasks from now on.
    streamTaskDir.listFiles().foreach(_.delete())
    val streamFactory = microBatchScan.readerFactory.asInstanceOf[SupportsArrowStreamRead]
    val streamPartitions = microBatchScan.inputPartitions.toSeq
    checkContexts(0) {
      assert(readAll(streamFactory, streamPartitions).sorted == (0L until 4L))
    }
    assert(NativeLibrary.openEngineStreams() == 0)
    // The library got the partition of the scan, the only one of the micro-batch.
    assert(streamTaskDir.listFiles().map(_.getName.toInt).toSet == Set(0))
    // The partitions of a micro-batch keep the state that the stream reader returned with them:
    // a reader factory that an engine keeps reads them with it, also once Spark planned the next
    // micro-batch, and so does the reader of Spark.
    def microBatchStream(options: (String, String)*): MicroBatchStream = {
      val relation = session.readStream.format("native_counter").options(options.toMap).load()
        .queryExecution.analyzed.collectFirst { case relation: StreamingRelationV2 => relation }.get
      relation.table.asInstanceOf[SupportsRead].newScanBuilder(relation.extraOptions)
        .build().toMicroBatchStream(Utils.createTempDir().getPath)
    }
    def plan(stream: MicroBatchStream, start: Int, end: Int): Seq[InputPartition] = {
      def offset(i: Int): Offset = stream.deserializeOffset(s"""{"offset":$i}""")
      stream.planInputPartitions(offset(start), offset(end)).toSeq
    }
    def serialized(partition: NativeMicroBatchPartition): Seq[Byte] = {
      val bytes = new ByteArrayOutputStream()
      Utils.tryWithResource(new ObjectOutputStream(bytes))(_.writeObject(partition))
      bytes.toByteArray.toSeq
    }
    val padding = (NativeDataSourceStreamReader.maxInlineStateBytes + 1).toString
    val stream = microBatchStream("max_offset" -> "4", "state_padding" -> padding)
    try {
      val first = plan(stream, 0, 2)
      val keptFactory = stream.createReaderFactory()
      val second = plan(stream, 2, 4)
      // The executors get a state larger than 64 KiB once for the micro-batch, as a broadcast that
      // its partitions keep, rather than with the partition of each task.
      Seq(first, second).foreach { partitions =>
        val partition = partitions.head.asInstanceOf[NativeMicroBatchPartition]
        assert(partition.state.isRight && partition.readerState.length > padding.toInt)
        assert(!serialized(partition).containsSlice(partition.readerState.toSeq))
      }
      checkContexts(0) {
        assert(readAll(keptFactory.asInstanceOf[SupportsArrowStreamRead], first) == Seq(0L, 1L))
        assert(readAll(keptFactory.asInstanceOf[SupportsArrowStreamRead], second) == Seq(2L, 3L))
        val read = inTask {
          first.flatMap { partition =>
            val reader = keptFactory.createColumnarReader(partition)
            try {
              val ids = ArrayBuffer.empty[Long]
              while (reader.next()) {
                val batch = reader.get()
                (0 until batch.numRows()).foreach(i => ids += batch.column(0).getLong(i))
              }
              ids.toSeq
            } finally {
              reader.close()
            }
          }
        }
        assert(read == Seq(0L, 1L))
      }
    } finally {
      stream.stop()
    }
    // A smaller state, such as a few offsets, goes with the partition of each task, which costs
    // less than a broadcast.
    val small = microBatchStream("max_offset" -> "4")
    try {
      val partition = plan(small, 0, 2).head.asInstanceOf[NativeMicroBatchPartition]
      assert(partition.state.isLeft && partition.readerState.nonEmpty)
      assert(serialized(partition).containsSlice(partition.readerState.toSeq))
      checkContexts(0) {
        assert(readAll(small.createReaderFactory().asInstanceOf[SupportsArrowStreamRead],
          Seq(partition)) == Seq(0L, 1L))
      }
    } finally {
      small.stop()
    }
    // A micro-batch without partitions broadcasts no state, although the stream reader returned
    // a large one.
    val (counterName, counterLocation) =
      NativeDataSourceRegistry.lookup("native_counter", session.sessionState.conf).get
    val streamReader = new NativeDataSource(counterLocation, counterName,
        new CaseInsensitiveStringMap(Map("max_offset" -> "4", "state_padding" -> padding).asJava))
      .streamReader(StructType.fromDDL("id BIGINT"), Utils.createTempDir().getPath)
      .asInstanceOf[NativeDataSourceStreamReader]
    try {
      def offset(i: Int): String = s"""{"offset":$i}"""
      assert(streamReader.partitions(offset(0), offset(0)).isEmpty)
      assert(streamReader.uncommittedStates.isEmpty)
      // The broadcast of a micro-batch is destroyed once the micro-batch is committed, and those
      // of the micro-batches after it stay.
      assert(streamReader.partitions(offset(0), offset(2)).nonEmpty)
      assert(streamReader.partitions(offset(2), offset(4)).nonEmpty)
      assert(streamReader.partitions(offset(4), offset(6)).nonEmpty)
      val states = streamReader.uncommittedStates
      assert(states.length == 3 && states.forall(_.isValid))
      streamReader.commit(offset(2))
      assert(states.map(_.isValid) == Seq(false, true, true))
      assert(streamReader.uncommittedStates == states.tail)
      // Spark may not commit every micro-batch, as with async progress tracking, so a commit also
      // destroys the broadcasts of the micro-batches before it, also when the committed one has no
      // broadcast, as a micro-batch without partitions, here at an end not planned before.
      assert(streamReader.partitions(offset(7), offset(7)).isEmpty)
      streamReader.commit(offset(7))
      assert(states.forall(!_.isValid) && streamReader.uncommittedStates.isEmpty)
      // stop() destroys the broadcasts of the micro-batches that are not committed.
      assert(streamReader.partitions(offset(7), offset(9)).nonEmpty)
      val lastState = streamReader.uncommittedStates
      assert(lastState.length == 1 && lastState.head.isValid)
      streamReader.stop()
      assert(!lastState.head.isValid && streamReader.uncommittedStates.isEmpty)
    } finally {
      streamReader.stop()
    }
    // A micro-batch without a state has no bytes to read the partitions with.
    val stateless = microBatchStream("max_offset" -> "4", "states" -> "0")
    try {
      val partitions = plan(stateless, 0, 2).map(_.asInstanceOf[NativeMicroBatchPartition])
      assert(partitions.map(_.state.isLeft) == Seq(true) && partitions.head.readerState.isEmpty)
    } finally {
      stateless.stop()
    }

    // It runs in the running task, with a stream whose release callback is NULL.
    val outside = intercept[IllegalStateException] {
      factory.openArrowStream(partitions.head, 0)
    }
    assert(outside.getMessage.contains("must be called in the running task"))
    val completed = intercept[Exception] {
      session.artifactManager.withResources {
        session.sparkContext.parallelize(Seq(0), 1).foreach { _ =>
          TaskContext.get().addTaskCompletionListener[Unit] { _ =>
            factory.openArrowStream(partitions.head, 0)
          }
        }
      }
    }
    assert(Utils.exceptionString(completed).contains("must be called in the running task"))
    // Nor in a task that failed, while its failure listeners run.
    val failed = intercept[Exception] {
      session.artifactManager.withResources {
        session.sparkContext.parallelize(Seq(0), 1).foreach { _ =>
          TaskContext.get().addTaskFailureListener { (_, _) =>
            factory.openArrowStream(partitions.head, 0)
          }
          throw new IllegalStateException("injected failure of the task")
        }
      }
    }
    assert(Utils.exceptionString(failed).contains("injected failure of the task"))
    assert(Utils.exceptionString(failed).contains("must be called in the running task"))
    // A task that was killed gets the error of its kill.
    val killedContext = TaskContext.empty()
    killedContext.markInterrupted("killed for the test")
    TaskContext.setTaskContext(killedContext)
    val killed = try intercept[TaskKilledException] {
      factory.openArrowStream(partitions.head, 0)
    } finally {
      TaskContext.unset()
    }
    assert(killed.reason == "killed for the test")
    val noStream = intercept[Exception](inTask(factory.openArrowStream(partitions.head, 0)))
    assert(Utils.exceptionString(noStream).contains("whose release callback is NULL"))
    // The arguments are checked before the partition, which must be of the kind of the scan of the
    // factory: a partition of a micro-batch fails for a batch scan, and the other way around.
    val outsideWithOtherKind = intercept[IllegalStateException] {
      streamFactory.openArrowStream(partitions.head, 0)
    }
    assert(outsideWithOtherKind.getMessage.contains("must be called in the running task"))
    val otherKinds = checkContexts(0) {
      inTask {
        Seq(streamFactory -> partitions.head, factory -> streamPartitions.head).map {
          case (kindFactory, partition) =>
            val stream = Platform.allocateMemory(streamSize)
            try {
              Platform.setMemory(stream, 0.toByte, streamSize)
              kindFactory.openArrowStream(partition, stream)
              None
            } catch {
              case e: IllegalArgumentException => Some(e.getMessage)
            } finally {
              Platform.freeMemory(stream)
            }
        }
      }
    }
    assert(otherKinds == Seq(
      Some("openArrowStream needs a partition of the scan of the reader factory, a " +
        s"NativeMicroBatchPartition, but got ${classOf[NativeInputPartition].getName}."),
      Some("openArrowStream needs a partition of the scan of the reader factory, a " +
        s"NativeInputPartition, but got ${classOf[NativeMicroBatchPartition].getName}.")))
    // A stream whose release callback is not NULL, such as an open one, is left as it is.
    val (rejected, unchanged, ids) = checkContexts(0) {
      inTask {
        val allocator = ArrowUtils.rootAllocator.newChildAllocator("engine", 0, Long.MaxValue)
        try {
          val stream = ArrowArrayStream.allocateNew(allocator)
          factory.openArrowStream(partitions.head, stream.memoryAddress())
          val before = callbacks(stream)
          val rejected = try {
            factory.openArrowStream(partitions.head, stream.memoryAddress())
            None
          } catch {
            case e: IllegalArgumentException => Some(e.getMessage)
          }
          (rejected, callbacks(stream) == before, readIds(stream, allocator))
        } finally {
          allocator.close()
        }
      }
    }
    assert(rejected.exists(_.contains("whose release callback is NULL")), rejected)
    assert(unchanged && ids == (0L until 3L))

    // If it fails, the stream is released, including the stream of the library that Spark rejects.
    Seq(
      "open" -> "injected failure in open",
      "uninitialized" ->
        "The library did not initialize the stream, or a callback of the stream is NULL.",
      "null_get_next" ->
        "The library did not initialize the stream, or a callback of the stream is NULL."
    ).foreach { case (fail, msg) =>
      val failReleased = new File(Utils.createTempDir(), "released")
      val (failing, failingPartitions) =
        plannedScan(range(session, "fail" -> fail, "released_path" -> failReleased.getPath))
      val e = checkContexts(0) {
        intercept[Exception] {
          inTask {
            val stream = ArrowArrayStream.allocateNew(ArrowUtils.rootAllocator)
            try {
              failing.openArrowStream(failingPartitions.head, stream.memoryAddress())
            } catch {
              // Keeps the failure, but not as the cause, which findError would find.
              case e: Throwable if stream.snapshot().release != 0 =>
                val notReleased = new IllegalStateException(
                  s"The stream is not released after the failure $fail.")
                notReleased.addSuppressed(e)
                throw notReleased
            } finally {
              stream.close()
            }
          }
        }
      }
      checkError(
        exception = findError(e, "NATIVE_DATA_SOURCE_ERROR"),
        condition = "NATIVE_DATA_SOURCE_ERROR",
        parameters = Map("action" -> "read", "name" -> "native_range", "msg" -> msg))
      assert(failReleased.exists() == (fail == "null_get_next"), fail)
      assert(NativeLibrary.openEngineStreams() == 0)
    }
  }

  nativeTest("tell a read that its task was killed") {
    val session = newSession(defaultPackage)
    val marker = new File(Utils.createTempDir(), "interrupted")
    val df = range(session, "end" -> "10", "partitions" -> "1",
      "wait_for_interrupt" -> marker.getPath)
    val jobGroup = s"native-data-source-${UUID.randomUUID()}"
    val error = new AtomicReference[Throwable]()
    val contexts = NativeLibrary.openTaskContexts()
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
    // The job fails right away, and its task ends once the library sees that it was killed. The
    // test also waits until the task closed the context of its read, which the tests that count
    // the contexts would otherwise count.
    eventually(timeout(1.minute)) {
      assert(marker.isFile && Files.readString(marker.toPath) == "interrupted")
      assert(NativeLibrary.openTaskContexts() <= contexts)
    }
  }

  nativeTest("release a stream only once get_next returns") {
    assume(TestUtils.testCommandAvailable("cat"))
    val session = newSession(defaultPackage)
    val marker = new File(Utils.createTempDir(), "released")
    val reading = marker.getPath + ".reading"
    val df = range(session, "end" -> "10", "partitions" -> "1", "slow_read" -> marker.getPath)
    // The contexts of the tasks of earlier tests are closed first, so that the counts below are
    // exactly those of this test, also the one of the context that stays open.
    eventually(timeout(1.minute))(assert(NativeLibrary.openTaskContexts() == 0))
    val contexts = 0
    try {
      // PipedRDD reads the scan on a thread of its own, which the task does not wait for. The
      // task completes without reading the output of the command, once that thread is in
      // get_next, which waits for the file .continue, and closes the reader then. The job runs
      // with the artifacts of the session, as in a SQL execution: see checkRows.
      val count = session.artifactManager.withResources {
        df.rdd.pipe("cat").mapPartitions { _ =>
          val deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(1)
          while (!new File(reading).isFile && System.nanoTime() < deadline) {
            Thread.sleep(10)
          }
          Iterator.empty[String]
        }.count()
      }
      assert(count == 0)
      assert(new File(reading).isFile, "get_next was not called.")
      if (marker.isFile) fail(s"The stream was released: ${Files.readString(marker.toPath)}.")
      // The task completed, but its context stays valid until the stream is released.
      assert(NativeLibrary.openTaskContexts() == contexts + 1)
    } finally {
      new File(marker.getPath + ".continue").createNewFile()
    }
    // The thread in get_next releases the stream once get_next returns, and then the context.
    eventually(timeout(1.minute)) {
      assert(marker.isFile && Files.readString(marker.toPath) == "after get_next")
      assert(NativeLibrary.openTaskContexts() == contexts)
    }
    // The read learned that its task ended, so that it may stop reading.
    assert(Files.readString(new File(marker.getPath + ".interrupted").toPath) == "true")
  }

  nativeTest("close a reader while another thread reads it") {
    val session = newSession(defaultPackage)
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("native-reader")
    // Closes a reader while another thread is in get_next. The reader releases the stream once
    // get_next returns, also when it fails, and then next throws: the error of its read, or the
    // failure of a next once the reader is closed, so that the thread does not take the close for
    // the end of the data.
    def closeWhileReading(failing: Boolean): Either[Throwable, Boolean] = {
      val marker = new File(Utils.createTempDir(), "released")
      val options = Seq("end" -> "10", "partitions" -> "1", "slow_read" -> marker.getPath) ++
        (if (failing) Seq("fail" -> "slow_read") else Nil)
      val batch = scan(range(session, options: _*)).toBatch
      val reader =
        batch.createReaderFactory().createColumnarReader(batch.planInputPartitions().head)
      try {
        val read = pool.submit(new Callable[Boolean] {
          override def call(): Boolean = reader.next()
        })
        eventually(timeout(1.minute)) {
          assert(new File(marker.getPath + ".reading").isFile)
        }
        reader.close()
        if (marker.isFile) fail(s"The stream was released: ${Files.readString(marker.toPath)}.")
        new File(marker.getPath + ".continue").createNewFile()
        val result = try Right(read.get(1, TimeUnit.MINUTES)) catch {
          case e: ExecutionException => Left(e.getCause)
        }
        assert(Files.readString(marker.toPath) == "after get_next")
        // The context was still valid once get_next continued: Spark answers that a context that
        // it freed is interrupted, and a read outside of a task never is.
        assert(Files.readString(new File(marker.getPath + ".interrupted").toPath) == "false")
        // A reader that was closed reads nothing more, and closing it again does not fail.
        checkClosed(intercept[SparkRuntimeException](reader.next()))
        reader.close()
        result
      } finally {
        // Lets get_next continue, and does not hide a failure of the test if closing fails.
        new File(marker.getPath + ".continue").createNewFile()
        Utils.tryLogNonFatalError(reader.close())
      }
    }
    try {
      checkClosed(closeWhileReading(failing = false).swap.getOrElse(fail("next did not fail")))
      val error = closeWhileReading(failing = true).swap.getOrElse(fail("next did not fail"))
      assert(error.getMessage.contains("injected failure after a slow get_next"))
    } finally {
      pool.shutdown()
    }
  }

  nativeTest("close a reader while another thread reads the batch that it returned") {
    val session = newSession(defaultPackage)
    val pool = ThreadUtils.newDaemonSingleThreadExecutor("native-reader")
    def onPool[T](f: => T): T = pool.submit(new Callable[T] {
      override def call(): T = f
    }).get(1, TimeUnit.MINUTES)
    // The contexts of the tasks of earlier tests are closed first, so that the counts below are
    // exactly those of this test.
    eventually(timeout(1.minute))(assert(NativeLibrary.openTaskContexts() == 0))
    // Closes a reader on this thread once the other thread read a batch. With `nextAgain`, the
    // other thread then calls next, which tells that it no longer reads the batch. Otherwise
    // nativeTest checks that a Cleaner frees the buffers once the reader is unreachable.
    def closeWhileRead(nextAgain: Boolean): Unit = {
      val marker = new File(Utils.createTempDir(), "released")
      // get_next does not wait: the file .continue exists.
      new File(marker.getPath + ".continue").createNewFile()
      val (reader, allocator) = readerAndAllocator(
        range(session, "end" -> "10", "partitions" -> "1", "slow_read" -> marker.getPath)
          .select("id"))
      try {
        assert(onPool(reader.next()))
        assert(NativeLibrary.openTaskContexts() == 1)
        reader.close()
        // The stream and the task are released at once, but not the buffers of the batch, which
        // the other thread may still read.
        assert(Files.readString(marker.toPath) == "after get_next")
        assert(NativeLibrary.openTaskContexts() == 0)
        assert(readerAllocators().contains(allocator) && allocator.getAllocatedMemory > 0)
        val rows = onPool((0 until reader.get().numRows()).map(reader.get().column(0).getLong))
        assert(rows === (0L until 10L))
        // Closing the reader again does nothing.
        reader.close()
        assert(readerAllocators().contains(allocator))
        if (nextAgain) {
          // A next then fails, and frees the buffers of the batch and the allocator at once: the
          // test still references the reader, which a Cleaner would wait for.
          checkClosed(intercept[ExecutionException](onPool(reader.next())).getCause)
          assert(!readerAllocators().contains(allocator))
          // The batch then has no rows, so that it is not read at the freed addresses.
          assert(reader.get().numRows() == 0)
          // So does a next on any thread after that.
          checkClosed(intercept[SparkRuntimeException](reader.next()))
        }
      } finally {
        reader.close()
      }
    }
    try {
      closeWhileRead(nextAgain = false)
      closeWhileRead(nextAgain = true)
    } finally {
      pool.shutdown()
    }
  }

  nativeTest("keep the buffers of a closed reader while its batch is held without the reader") {
    val session = newSession(defaultPackage)
    // Closes a reader once it returned a batch, and returns the batch and the allocator of the
    // reader, but not the reader, as `DataSourceRDD` drops it once it closes it.
    def closedBatch(): (ColumnarBatch, BufferAllocator) = {
      val (reader, allocator) = readerAndAllocator(
        range(session, "end" -> "10", "partitions" -> "1").select("id"))
      try {
        assert(reader.next())
      } finally {
        reader.close()
      }
      (reader.get(), allocator)
    }
    // Reads the batch after garbage collections.
    def readAfterGC(): Unit = {
      val (batch, allocator) = closedBatch()
      // An object that a Cleaner action marks once a garbage collection found it unreachable,
      // after which the reader would be freed too if only the reader referred to it.
      val collected = new CountDownLatch(1)
      NativeHandle.cleaner.register(new Object, () => collected.countDown())
      eventually(timeout(1.minute), interval(100.milliseconds)) {
        System.gc()
        assert(collected.getCount == 0)
      }
      // The column vectors of the batch keep the reader reachable, so that the Cleaner does not
      // free the buffers that a consumer still reads.
      assert(readerAllocators().contains(allocator) && allocator.getAllocatedMemory > 0)
      assert((0 until batch.numRows()).map(batch.column(0).getLong) === (0L until 10L))
    }
    // nativeTest checks that the Cleaner frees them once the batch is dropped too.
    readAfterGC()
  }

  nativeTest("free the buffers of a reader closed at the end of its stream without a GC") {
    val session = newSession(defaultPackage)
    // The ids 0-3, 4-7 and 8-9.
    val (reader, allocator) = readerAndAllocator(
      range(session, "end" -> "10", "partitions" -> "1", "batch_size" -> "4").select("id"))
    try {
      val ids = ArrayBuffer.empty[Long]
      while (reader.next()) {
        ids ++= (0 until reader.get().numRows()).map(reader.get().column(0).getLong)
      }
      assert(ids.toSeq === (0L until 10L))
      // The buffers of the last batch stay allocated until the reader is closed.
      assert(allocator.getAllocatedMemory > 0)
      reader.close()
      // Consumers no longer read a batch once next was called again, here at the end of the
      // stream, so the close frees its buffers and the allocator at once: the test still
      // references the reader, which a Cleaner would wait for.
      assert(!readerAllocators().contains(allocator))
      // The batch then has no rows, so that it is not read at the freed addresses.
      assert(reader.get().numRows() == 0)
    } finally {
      reader.close()
    }
    // So does the close of a reader whose next failed, also after an import: the name of id 10, in
    // the third batch of the second partition, is null.
    val (failed, failedAllocator) = readerAndAllocator(
      session.read.schema("id BIGINT, name STRING NOT NULL").format("native_range")
        .options(Map("end" -> "12", "partitions" -> "2", "batch_size" -> "2")).load(),
      partition = 1)
    try {
      assert(failed.next() && failed.next())
      findError(intercept[Exception](failed.next()), "NATIVE_DATA_SOURCE_ERROR")
      failed.close()
      assert(!readerAllocators().contains(failedAllocator))
    } finally {
      failed.close()
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
    // The same package found twice is not a conflict: here, listed both as a file and through
    // its directory.
    val only = Utils.createTempDir()
    val pkg = createPackage(library, dataSources(""), dir = only, fileName = "c.sparkpkg")
    checkRows(range(newSession(pkg, only), "end" -> "2"), Seq(expectedRow(0L), expectedRow(1L)))
    // Nor is the same package, configured and also added by the session.
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

  nativeTest("reject a package that a session adds when Spark cannot use it") {
    // The package that a session added last takes precedence, so Spark rejects a package that it
    // cannot use when the session adds it, rather than keep using what the session used before.
    val session = newSession(defaultPackage)
    def ids(): DataFrame = range(session, "end" -> "3").select("id")
    val dir = Utils.createTempDir()
    def check(pkg: File, subClass: String, params: Map[String, String]): Unit = {
      // Also when the same file is added again.
      (1 to 2).foreach { _ =>
        checkError(
          exception = intercept[SparkRuntimeException](session.addArtifact(pkg.getPath)),
          condition = s"INVALID_NATIVE_DATA_SOURCE_PACKAGE.$subClass",
          parameters = Map("path" -> pkg.getPath) ++ params)
      }
      // The session does not get it, and keeps using the configured package.
      assert(session.artifactManager.getNativeDataSourcePackages._1.isEmpty)
      assert(!session.artifactManager.hasFileArtifact(pkg.getName))
      assert(!session.sparkContext.listFiles().exists(_.endsWith("/" + pkg.getName)))
      checkRows(ids(), Seq(Row(0L), Row(1L), Row(2L)))
    }
    check(createPackage(renamedNegatingLibrary, dataSources(""), dir = dir,
        fileName = "range-2.0.sparkpkg", abiVersion = 2),
      "UNSUPPORTED_ABI_VERSION", Map("version" -> "2", "supported" -> "1"))
    check(createZip(dir, "range-2.1.sparkpkg",
        NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
          dataSources(""), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8)),
      "INVALID_MANIFEST", Map("reason" -> "The package does not contain the library lib.so."))
    // ZipFile.getEntry("lib") also finds the directory entry "lib/", as for the manifest below.
    check(createZip(dir, "range-2.7.sparkpkg",
        NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
          dataSources(""), Map(NativePlatform.current -> "lib")).getBytes(UTF_8),
        "lib/" -> Array.emptyByteArray, "lib/x" -> "x".getBytes(UTF_8)),
      "INVALID_MANIFEST", Map("reason" -> "The package does not contain the library lib."))
    check(createZip(dir, "range-2.8.sparkpkg",
        s"${NativeDataSourcePackage.MANIFEST_NAME}/" -> Array.emptyByteArray),
      "INVALID_MANIFEST", Map("reason" ->
        s"The package does not contain the manifest ${NativeDataSourcePackage.MANIFEST_NAME}."))
    val notAZip = new File(dir, "range-2.2.sparkpkg")
    Files.writeString(notAZip.toPath, "not a zip")
    // The message of the ZipException names no file.
    val notAZipReason =
      s"The package is not a valid zip file: ${intercept[ZipException](new ZipFile(notAZip))}"
    check(notAZip, "INVALID_MANIFEST", Map("reason" -> notAZipReason))
    val directory = new File(dir, "range-2.3.sparkpkg")
    assert(directory.mkdir())
    check(directory, "INVALID_MANIFEST", Map("reason" -> "The package is a directory."))
    // The driver loads the library to plan a query.
    check(createPackage(library, dataSources(""), dir = dir, fileName = "range-2.4.sparkpkg",
        platform = "other-platform"),
      "UNSUPPORTED_PLATFORM", Map("platform" -> NativePlatform.current,
        "platforms" -> "other-platform"))
    // A file that does not exist, or that cannot be read, fails as for the other artifacts.
    intercept[NoSuchFileException](session.addArtifact(new File(dir, "missing.sparkpkg").getPath))
    val unreadable = createPackage(library, dataSources(""), dir = dir,
      fileName = "range-2.5.sparkpkg")
    try {
      // Not when the user can read any file, as root.
      if (unreadable.setReadable(false) && !Files.isReadable(unreadable.toPath)) {
        intercept[AccessDeniedException](session.addArtifact(unreadable.getPath))
      }
    } finally {
      unreadable.setReadable(true)
    }
    // A package that Spark staged, such as one added from bytes, is named after the artifact.
    val staged = intercept[SparkRuntimeException] {
      session.addArtifact(Files.readAllBytes(createPackage(library, dataSources(""),
        abiVersion = 2).toPath), "range-2.6.sparkpkg")
    }
    checkError(
      exception = staged,
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.UNSUPPORTED_ABI_VERSION",
      parameters = Map(
        "path" -> Paths.get("files", "range-2.6.sparkpkg").toString,
        "version" -> "2",
        "supported" -> "1"))
    assert(session.artifactManager.getNativeDataSourcePackages._1.isEmpty)

    // A valid package with the same file name as a rejected one can be added then.
    val added = createPackage(renamedNegatingLibrary, dataSources(""),
      fileName = "range-2.0.sparkpkg")
    session.addArtifact(added.getPath)
    checkRows(ids(), Seq(Row(0L), Row(-1L), Row(-2L)))
    // Adding it again, from the file or from the same bytes, does nothing.
    session.addArtifact(added.getPath)
    session.addArtifact(Files.readAllBytes(added.toPath), added.getName)
    assert(session.artifactManager.getNativeDataSourcePackages._1.map(_.getName) ==
      Seq("range-2.0.sparkpkg"))
    checkRows(ids(), Seq(Row(0L), Row(-1L), Row(-2L)))
    // Another file with the name of an artifact fails as such, also if it is not a package, and
    // the error names the artifact.
    def checkExisting(e: SparkRuntimeException, fileName: String = "range-2.0.sparkpkg"): Unit = {
      checkError(
        exception = e,
        condition = "ARTIFACT_ALREADY_EXISTS",
        parameters = Map("normalizedRemoteRelativePath" -> Paths.get("files", fileName).toString))
    }
    val sameName = new File(Utils.createTempDir(), "range-2.0.sparkpkg")
    Files.copy(notAZip.toPath, sameName.toPath)
    checkExisting(intercept[SparkRuntimeException](session.addArtifact(sameName.getPath)))
    val sameNameDirectory = new File(Utils.createTempDir(), "range-2.0.sparkpkg")
    assert(sameNameDirectory.mkdir())
    checkExisting(intercept[SparkRuntimeException](session.addArtifact(sameNameDirectory.getPath)))
    val missing = new File(Utils.createTempDir(), "range-2.0.sparkpkg")
    checkExisting(intercept[SparkRuntimeException](session.addArtifact(missing.getPath)))
    // Also for a package that Spark staged, which is compared with the artifact before the check.
    checkExisting(intercept[SparkRuntimeException] {
      session.addArtifact("not a zip".getBytes(UTF_8), "range-2.0.sparkpkg")
    })
    // And for an artifact that is a directory, as files/range-5.0.sparkpkg is after this.
    val inner = new File(Utils.createTempDir(), "inner.txt")
    Files.writeString(inner.toPath, "inner")
    session.artifactManager.addArtifact(Paths.get("files", "range-5.0.sparkpkg", inner.getName),
      inner.toPath, None, deleteStagedFile = false)
    checkExisting(intercept[SparkRuntimeException] {
      session.addArtifact(createPackage(library, dataSources(""),
        fileName = "range-5.0.sparkpkg").getPath)
    }, "range-5.0.sparkpkg")

    // A call that adds several artifacts adds the others, and reports all the rejected ones.
    val valid = createPackage(library, dataSources(""), dir = dir, fileName = "range-3.0.sparkpkg")
    val rejected = intercept[SparkRuntimeException] {
      session.addArtifacts(sameName.toURI, notAZip.toURI, valid.toURI)
    }
    checkExisting(rejected)
    assert(rejected.getSuppressed.length == 1)
    checkError(
      exception = rejected.getSuppressed.head.asInstanceOf[SparkRuntimeException],
      condition = "INVALID_NATIVE_DATA_SOURCE_PACKAGE.INVALID_MANIFEST",
      parameters = Map("path" -> notAZip.getPath, "reason" -> notAZipReason))
    checkRows(ids(), Seq(Row(0L), Row(1L), Row(2L)))

    // Only the file artifacts (files/) with the extension are packages, which Spark checks, and
    // not other kinds of artifacts with it, which a Spark Connect client can add.
    val pyFile = createPackage(library, dataSources(""), dir = dir,
      fileName = "range-4.0.sparkpkg", abiVersion = 2)
    session.artifactManager.addArtifact(
      Paths.get("pyfiles", pyFile.getName), pyFile.toPath, None, deleteStagedFile = false)
    assert(session.artifactManager.getNativeDataSourcePackages._1.map(_.getName) ==
      Seq("range-2.0.sparkpkg", "range-3.0.sparkpkg"))
  }

  test("add any file with the extension when native data sources are disabled") {
    val session = spark.newSession()
    sessions += session
    // A static configuration, which a session cannot set otherwise.
    session.sessionState.conf.setConf(StaticSQLConf.NATIVE_DATA_SOURCE_ENABLED, false)
    val file = new File(Utils.createTempDir(), "not_a_package.sparkpkg")
    Files.writeString(file.toPath, "not a zip")
    session.addArtifact(file.getPath)
    assert(session.artifactManager.hasFileArtifact(file.getName))
  }

  test("add a read-only package as a writable artifact") {
    // Spark checks a copy of the file of the user, which keeps its permissions, or on Windows its
    // read-only attribute, so Spark makes it writable: the artifact, moved from it, is writable.
    val session = spark.newSession()
    sessions += session
    val pkg = createZip(Utils.createTempDir(), "read_only.sparkpkg",
      NativeDataSourcePackage.MANIFEST_NAME -> manifestJson(
        dataSources(""), Map(NativePlatform.current -> "lib.so")).getBytes(UTF_8),
      "lib.so" -> "lib".getBytes(UTF_8))
    assert(pkg.setWritable(false))
    try {
      session.addArtifact(pkg.getPath)
      val artifact = session.artifactManager.getNativeDataSourcePackages._1.head.toPath
      // The permissions rather than Files.isWritable, which is true for root whatever they are.
      Option(Files.getFileAttributeView(artifact, classOf[PosixFileAttributeView])) match {
        case Some(view) =>
          assert(view.readAttributes().permissions().contains(PosixFilePermission.OWNER_WRITE))
        case None =>
          // On Windows, Files.isWritable reads the read-only attribute.
          assert(Files.isWritable(artifact))
      }
    } finally {
      pkg.setWritable(true)
    }
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

/** What an engine does with the Arrow C stream of a partition of a native scan. */
object TestArrowEngine {
  /** The callbacks and the private data of a stream. */
  def callbacks(stream: ArrowArrayStream): Seq[Long] = {
    val snapshot = stream.snapshot()
    Seq(snapshot.get_schema, snapshot.get_next, snapshot.get_last_error, snapshot.release,
      snapshot.private_data)
  }

  /**
   * Reads the ids of a stream to its end on the calling thread, without moving or releasing it, as
   * an engine that releases the stream later does.
   */
  def readIdsInPlace(stream: ArrowArrayStream): Seq[Long] = readNamesAndIdsInPlace(stream)._2

  /** The names of the columns of a stream and its ids, read as `readIdsInPlace` reads them. */
  def readNamesAndIdsInPlace(stream: ArrowArrayStream): (Seq[String], Seq[Long]) = {
    val allocator = ArrowUtils.rootAllocator.newChildAllocator("engine", 0, Long.MaxValue)
    val dictionaries = new CDataDictionaryProvider()
    try {
      val schema = ArrowSchema.allocateNew(allocator)
      val root = try {
        stream.getSchema(schema)
        VectorSchemaRoot.create(Data.importSchema(allocator, schema, dictionaries), allocator)
      } finally {
        schema.close()
      }
      try {
        val ids = ArrayBuffer.empty[Long]
        var more = true
        while (more) {
          // A new array for each batch, since the import closes it, as in NativePartitionReader.
          val array = ArrowArray.allocateNew(allocator)
          try {
            stream.getNext(array)
            // The stream ends with a released array.
            more = array.snapshot().release != 0
            if (more) {
              Data.importIntoVectorSchemaRoot(allocator, array, root, dictionaries)
              val vector = root.getVector("id").asInstanceOf[BigIntVector]
              (0 until root.getRowCount).foreach(i => ids += vector.get(i))
            }
          } finally {
            array.close()
          }
        }
        (root.getSchema.getFields.asScala.map(_.getName).toSeq, ids.toSeq)
      } finally {
        root.close()
      }
    } finally {
      dictionaries.close()
      allocator.close()
    }
  }

  /** Reads the ids of a stream on another thread, as an engine may, and releases the stream. */
  def readIds(stream: ArrowArrayStream, allocator: BufferAllocator): Seq[Long] = {
    // Moves the stream into a reader, which releases it once closed.
    val reader = Data.importArrayStream(allocator, stream)
    try {
      ThreadUtils.runInNewThread("engine") {
        val ids = ArrayBuffer.empty[Long]
        while (reader.loadNextBatch()) {
          val root = reader.getVectorSchemaRoot
          val vector = root.getVector("id").asInstanceOf[BigIntVector]
          (0 until root.getRowCount).foreach(i => ids += vector.get(i))
        }
        ids.toSeq
      }
    } finally {
      reader.close()
    }
  }
}
