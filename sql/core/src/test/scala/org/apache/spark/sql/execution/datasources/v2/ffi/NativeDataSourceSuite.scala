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
import java.sql.{Date, Timestamp}
import java.time.{Instant, LocalDate}
import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkClassNotFoundException, SparkFiles, SparkRuntimeException, SparkThrowable, SparkUnsupportedOperationException}
import org.apache.spark.api.python.{PythonBroadcast, SimplePythonFunction}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.classic.{DataFrame, SparkSession}
import org.apache.spark.sql.connector.expressions.{Expression, FieldReference, GeneralScalarExpression, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And, Predicate}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.columnar.ColumnarScan
import org.apache.spark.sql.execution.datasources.v2.python.{PythonDataSourceV2, UserDefinedPythonDataSource}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Tests native data sources with the library in `native-datasource/test_native_datasource.cc`,
 * compiled with the C++ compiler of the machine by [[NativeDataSourceTestUtils]]. The tests that
 * load a library are skipped when there is no C++ compiler or no JNI headers.
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
  private lazy val abiV2Library =
    compile("test_native_datasource_v2", "-DABI_VERSION=2", "-DNAME_PREFIX=\"v2_\"")
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

  private def nativeTest(name: String)(f: => Unit): Unit = test(name) {
    assume(toolchain.isDefined, "A C++ compiler and the JNI headers are required.")
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
    assert(NativePlatform.current == NativePlatform.name(
      System.getProperty("os.name"), System.getProperty("os.arch")))
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

  test("encode predicates as JSON") {
    def predicate(name: String, children: Expression*): Predicate = {
      new Predicate(name, children.toArray)
    }
    val id = FieldReference(Seq("id"))
    val schema = StructType.fromDDL("id BIGINT, a STRUCT<b: INT>, s STRING, " +
      "c STRING COLLATE UTF8_LCASE, d STRUCT<e: ARRAY<STRING COLLATE UTF8_LCASE>>")
    def toJson(predicate: Predicate): Option[String] = NativePredicates.toJson(predicate, schema)
    assert(toJson(predicate(">", id, LiteralValue(5L, LongType))).contains(
      """{"type":"function","name":">","children":[{"type":"column","name":["id"]},""" +
        """{"type":"literal","dataType":"bigint","value":5}]}"""))
    assert(toJson(new And(
      predicate("IS_NULL", FieldReference(Seq("a", "b"))),
      predicate("=", id, new GeneralScalarExpression("+", Array(id, LiteralValue(1, IntegerType))))
    )).contains(
      """{"type":"function","name":"AND","children":[{"type":"function","name":"IS_NULL",""" +
        """"children":[{"type":"column","name":["a","b"]}]},{"type":"function","name":"=",""" +
        """"children":[{"type":"column","name":["id"]},{"type":"function","name":"+",""" +
        """"children":[{"type":"column","name":["id"]},""" +
        """{"type":"literal","dataType":"int","value":1}]}]}]}"""))

    def literal(value: Any, dataType: DataType): Option[String] = {
      toJson(predicate("=", id, LiteralValue(value, dataType)))
        .map(json => json.substring(json.indexOf("\"dataType\""), json.length - 3))
    }
    assert(literal(null, LongType).contains(""""dataType":"bigint","value":null"""))
    assert(literal(true, BooleanType).contains(""""dataType":"boolean","value":true"""))
    assert(literal(1.toByte, ByteType).contains(""""dataType":"tinyint","value":1"""))
    assert(literal(2.toShort, ShortType).contains(""""dataType":"smallint","value":2"""))
    assert(literal(1.5f, FloatType).contains(""""dataType":"float","value":1.5"""))
    assert(literal(2.5, DoubleType).contains(""""dataType":"double","value":2.5"""))
    assert(literal(UTF8String.fromString("a\"b"), StringType)
      .contains(""""dataType":"string","value":"a\"b""""))
    assert(literal(Array[Byte](1, 2), BinaryType)
      .contains(""""dataType":"binary","value":"AQI=""""))
    assert(literal(Decimal("12.30"), DecimalType(10, 2))
      .contains(""""dataType":"decimal(10,2)","value":"12.30""""))
    assert(literal(10, DateType).contains(""""dataType":"date","value":10"""))
    assert(literal(10L, TimestampType).contains(""""dataType":"timestamp","value":10"""))
    assert(literal(10L, TimestampNTZType).contains(""""dataType":"timestamp_ntz","value":10"""))
    // Values that JSON cannot represent, and types without an encoding, are not pushed down.
    assert(literal(Double.NaN, DoubleType).isEmpty)
    assert(literal(Float.PositiveInfinity, FloatType).isEmpty)
    assert(literal(UTF8String.fromString("a"), StringType("UTF8_LCASE")).isEmpty)
    assert(literal(null, ArrayType(IntegerType)).nonEmpty)
    assert(literal(Array(1), ArrayType(IntegerType)).isEmpty)

    // A source compares strings by their bytes, so the columns with strings of a non-binary
    // collation are not pushed down, and neither are the columns that are not in the schema.
    def compare(left: String, right: String): Option[String] = {
      toJson(predicate("=", FieldReference(left.split('.').toSeq),
        FieldReference(right.split('.').toSeq)))
    }
    assert(compare("s", "s").nonEmpty)
    assert(compare("a.b", "id").nonEmpty)
    assert(compare("c", "c").isEmpty)
    assert(compare("s", "c").isEmpty)
    assert(compare("d", "d").isEmpty)
    assert(compare("d.e", "d.e").isEmpty)
    assert(compare("missing", "id").isEmpty)
    assert(compare("a.missing", "id").isEmpty)
    assert(compare("id.b", "id").isEmpty)
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
    assert(scan(collated.filter("name = 'N1'")).getMetaData()("PushedPredicates") == "[]")

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

  nativeTest("push down predicates") {
    val df = range(newSession(defaultPackage), "end" -> "20").filter("id > 5 AND id <= 12")
    checkRows(df, (6L to 12L).map(expectedRow))
    val pushed = scan(df).getMetaData()("PushedPredicates")
    assert(pushed.contains("id > 5") && pushed.contains("id <= 12"), pushed)
    // The scan is described by the name of the data source.
    assert(scan(df).description() == "native_range")
    assert(!df.queryExecution.executedPlan.toString.contains("NativeDataSource"))

    // A predicate that the library does not accept is evaluated by Spark.
    val partly = range(newSession(defaultPackage), "end" -> "20")
      .filter("id < 10 AND name LIKE 'n1%'")
    checkRows(partly, Seq(1L).map(expectedRow))
    assert(scan(partly).getMetaData()("PushedPredicates").contains("id < 10"))
  }

  nativeTest("prune columns") {
    val df = range(newSession(defaultPackage), "end" -> "10").select("name", "id")
    checkRows(df, (0L until 10L).map(id => Row(if (id % 5 == 0) null else s"n$id", id)))
    assert(scan(df).readSchema() == StructType.fromDDL("id BIGINT, name STRING"))

    // A count reads no column at all.
    assert(range(newSession(defaultPackage), "end" -> "37", "batch_size" -> "10").count() == 37)
  }

  nativeTest("push down a limit") {
    val df = range(newSession(defaultPackage), "end" -> "100", "partitions" -> "5").limit(3)
    assert(df.collect().length == 3)
    assert(scan(df).getMetaData()("PushedLimit") == "LIMIT 3")
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
    // The writer is closed after the commit.
    assert(new File(dir, "_CLOSED").isFile)
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
    // abortDataWriter removed the temporary files of the tasks.
    assert(new File(dir, "_ABORTED").isFile)
    assert(new File(dir, "_temporary").listFiles().isEmpty)
    assert(!dir.listFiles().exists(_.getName.startsWith("part-")))
  }

  nativeTest("streaming read and write") {
    val session = newSession(defaultPackage)
    val dir = Utils.createTempDir()
    val commitPath = new File(Utils.createTempDir(), "committed")
    val query = session.readStream.format("native_counter")
      .option("max_offset", "12").option("step", "5").option("batch_size", "2")
      .option("commit_path", commitPath.getPath)
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
    // An error of the library is not an unsupported operation.
    check("plan a scan of", "native_sink does not support batch reads", "native_sink") {
      session => session.read.schema("id BIGINT").format("native_sink").load().collect()
    }
  }

  nativeTest("operations that the library does not implement") {
    val session = newSession(createPackage(readOnlyLibrary, dataSources("ro_")))
    // The library does not push anything down, so Spark evaluates the filter.
    val df = session.read.format("ro_native_range").option("end", "10").load().filter("id > 6")
    checkRows(df, (7L until 10L).map(expectedRow))
    assert(scan(df).getMetaData()("PushedPredicates") == "[]")

    checkError(
      exception = intercept[SparkUnsupportedOperationException] {
        session.range(1).write.format("ro_native_sink").mode("append").save()
      },
      condition = "DATA_SOURCE_BATCH_WRITE_NOT_SUPPORTED",
      parameters = Map("description" -> "ro_native_sink"))
    def checkStreaming(condition: String, name: String)(start: => StreamingQuery): Unit = {
      val e = intercept[Exception] {
        val query = start
        try query.processAllAvailable() finally query.stop()
      }
      checkError(
        exception = findError(e, condition),
        condition = condition,
        parameters = Map("description" -> name))
    }
    checkStreaming("DATA_SOURCE_MICRO_BATCH_SCAN_NOT_SUPPORTED", "ro_native_counter") {
      session.readStream.format("ro_native_counter").load().writeStream.format("noop").start()
    }
    checkStreaming("DATA_SOURCE_STREAMING_WRITE_NOT_SUPPORTED", "ro_native_sink") {
      session.readStream.format("rate").load().writeStream.format("ro_native_sink")
        .option("checkpointLocation", Utils.createTempDir().getPath).start()
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
      val elsewhere = new File(Utils.createTempDir(), installed.getName).getPath
      assert(NativeLibraries.get(InstalledNativeLibrary(elsewhere)) eq
        NativeLibraries.get(InstalledNativeLibrary(installed.getPath)))
    }

    val missing = new File(Utils.createTempDir(), installed.getName).getPath
    checkError(
      exception = intercept[SparkRuntimeException] {
        NativeLibraries.get(InstalledNativeLibrary(missing))
      },
      condition = "NATIVE_DATA_SOURCE_LIBRARY_NOT_FOUND",
      parameters = Map("fileName" -> installed.getName, "path" -> missing))
    checkError(
      exception = intercept[SparkClassNotFoundException] {
        newSession().read.format("installed_native_range").load()
      },
      condition = "DATA_SOURCE_NOT_FOUND",
      parameters = Map("provider" -> "installed_native_range"))
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
}
