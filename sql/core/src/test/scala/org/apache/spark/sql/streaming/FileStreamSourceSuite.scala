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

package org.apache.spark.sql.streaming

import java.io.File
import java.net.URI
import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable
import scala.util.Random

import org.apache.hadoop.fs._
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable
import org.scalatest.PrivateMethodTester
import org.scalatest.time.SpanSugar._

import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.connector.read.streaming.ReadLimit
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.FileStreamSource.{FileEntry, SeenFilesMap, SourceFileArchiver}
import org.apache.spark.sql.execution.streaming.sources.MemorySink
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.util.StreamManualClock
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{StructType, _}
import org.apache.spark.util.Utils

abstract class FileStreamSourceTest
  extends StreamTest with SharedSparkSession with PrivateMethodTester {

  import testImplicits._

  /**
   * A subclass `AddData` for adding data to files. This is meant to use the
   * `FileStreamSource` actually being used in the execution.
   */
  abstract class AddFileData extends AddData {
    private val _qualifiedBasePath = PrivateMethod[Path](Symbol("qualifiedBasePath"))

    private def isSamePath(fileSource: FileStreamSource, srcPath: File): Boolean = {
      val path = (fileSource invokePrivate _qualifiedBasePath()).toString.stripPrefix("file:")
      path == srcPath.getCanonicalPath
    }

    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active file stream source")

      val sources = getSourcesFromStreamingQuery(query.get)
      val source = if (sources.isEmpty) {
        throw new Exception(
          "Could not find file source in the StreamExecution logical plan to add data to")
      } else if (sources.size == 1) {
        sources.head
      } else {
        val matchedSources = sources.filter(isSamePath(_, src))
        if (matchedSources.size != 1) {
          throw new Exception(
            "Could not select the file source in StreamExecution as there are multiple" +
              s" file sources and none / more than one matches $src:\n" + sources.mkString("\n"))
        }
        matchedSources.head
      }
      val newOffset = source.withBatchingLocked {
        addData(source)
        new FileStreamSourceOffset(source.currentLogOffset + 1)
      }
      logInfo(s"Added file to $source at offset $newOffset")
      (source, newOffset)
    }

    /** Source directory to add file data to */
    protected def src: File

    protected def addData(source: FileStreamSource): Unit
  }

  case class AddTextFileData(content: String, src: File, tmp: File, tmpFilePrefix: String = "text")
    extends AddFileData {

    override def addData(source: FileStreamSource): Unit = {
      val tempFile = Utils.tempFileWith(new File(tmp, tmpFilePrefix))
      val finalFile = new File(src, tempFile.getName)
      src.mkdirs()
      require(stringToFile(tempFile, content).renameTo(finalFile))
      logInfo(s"Written text '$content' to file $finalFile")
    }
  }

  case class AddOrcFileData(data: DataFrame, src: File, tmp: File) extends AddFileData {
    override def addData(source: FileStreamSource): Unit = {
      AddOrcFileData.writeToFile(data, src, tmp)
    }
  }

  object AddOrcFileData {
    def apply(seq: Seq[String], src: File, tmp: File): AddOrcFileData = {
      AddOrcFileData(seq.toDS().toDF(), src, tmp)
    }

    /** Write orc files in a temp dir, and move the individual files to the 'src' dir */
    def writeToFile(df: DataFrame, src: File, tmp: File): Unit = {
      val tmpDir = Utils.tempFileWith(new File(tmp, "orc"))
      df.write.orc(tmpDir.getCanonicalPath)
      src.mkdirs()
      tmpDir.listFiles().foreach { f =>
        f.renameTo(new File(src, s"${f.getName}"))
      }
    }
  }

  case class AddParquetFileData(data: DataFrame, src: File, tmp: File) extends AddFileData {
    override def addData(source: FileStreamSource): Unit = {
      AddParquetFileData.writeToFile(data, src, tmp)
    }
  }

  object AddParquetFileData {
    def apply(seq: Seq[String], src: File, tmp: File): AddParquetFileData = {
      AddParquetFileData(seq.toDS().toDF(), src, tmp)
    }

    /** Write parquet files in a temp dir, and move the individual files to the 'src' dir */
    def writeToFile(df: DataFrame, src: File, tmp: File): Unit = {
      val tmpDir = Utils.tempFileWith(new File(tmp, "parquet"))
      df.write.parquet(tmpDir.getCanonicalPath)
      src.mkdirs()
      tmpDir.listFiles().foreach { f =>
        f.renameTo(new File(src, s"${f.getName}"))
      }
    }
  }

  case class AddFilesToFileStreamSinkLog(
      fs: FileSystem,
      srcDir: Path,
      sinkLog: FileStreamSinkLog,
      batchId: Int)(
      pathFilter: Path => Boolean) extends ExternalAction {
    override def runAction(): Unit = {
      val statuses = fs.listStatus(srcDir, new PathFilter {
        override def accept(path: Path): Boolean = pathFilter(path)
      })
      sinkLog.add(batchId, statuses.map(SinkFileStatus(_)))
    }
  }

  /** Use `format` and `path` to create FileStreamSource via DataFrameReader */
  def createFileStream(
      format: String,
      path: String,
      schema: Option[StructType] = None,
      options: Map[String, String] = Map.empty): DataFrame = {
    val reader =
      if (schema.isDefined) {
        spark.readStream.format(format).schema(schema.get).options(options)
      } else {
        spark.readStream.format(format).options(options)
      }
    reader.load(path)
  }

  protected def getSourceFromFileStream(df: DataFrame): FileStreamSource = {
    val checkpointLocation = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    df.queryExecution.analyzed
      .collect { case StreamingRelation(dataSource, _, _) =>
        // There is only one source in our tests so just set sourceId to 0
        dataSource.createSource(s"$checkpointLocation/sources/0").asInstanceOf[FileStreamSource]
      }.head
  }

  protected def getSourcesFromStreamingQuery(query: StreamExecution): Seq[FileStreamSource] = {
    query.logicalPlan.collect {
      case StreamingExecutionRelation(source, _) if source.isInstanceOf[FileStreamSource] =>
        source.asInstanceOf[FileStreamSource]
    }
  }

  protected def withTempDirs(body: (File, File) => Unit): Unit = {
    val src = Utils.createTempDir(namePrefix = "streaming.src")
    val tmp = Utils.createTempDir(namePrefix = "streaming.tmp")
    try {
      body(src, tmp)
    } finally {
      Utils.deleteRecursively(src)
      Utils.deleteRecursively(tmp)
    }
  }

  protected def withThreeTempDirs(body: (File, File, File) => Unit): Unit = {
    val src = Utils.createTempDir(namePrefix = "streaming.src")
    val tmp = Utils.createTempDir(namePrefix = "streaming.tmp")
    val archive = Utils.createTempDir(namePrefix = "streaming.archive")
    try {
      body(src, tmp, archive)
    } finally {
      Utils.deleteRecursively(src)
      Utils.deleteRecursively(tmp)
      Utils.deleteRecursively(archive)
    }
  }

  val valueSchema = new StructType().add("value", StringType)
}

class FileStreamSourceSuite extends FileStreamSourceTest {

  import testImplicits._

  override val streamingTimeout = 80.seconds

  /** Use `format` and `path` to create FileStreamSource via DataFrameReader */
  private def createFileStreamSource(
      format: String,
      path: String,
      schema: Option[StructType] = None): FileStreamSource = {
    getSourceFromFileStream(createFileStream(format, path, schema))
  }

  private def createFileStreamSourceAndGetSchema(
      format: Option[String],
      path: Option[String],
      schema: Option[StructType] = None): StructType = {
    val reader = spark.readStream
    format.foreach(reader.format)
    schema.foreach(reader.schema)
    val df =
      if (path.isDefined) {
        reader.load(path.get)
      } else {
        reader.load()
      }
    df.queryExecution.analyzed
      .collect { case s @ StreamingRelation(dataSource, _, _) => s.schema }.head
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.sessionState.conf.setConf(SQLConf.ORC_IMPLEMENTATION, "native")
  }

  override def afterAll(): Unit = {
    try {
      spark.sessionState.conf.unsetConf(SQLConf.ORC_IMPLEMENTATION)
    } finally {
      super.afterAll()
    }
  }

  // ============= Basic parameter exists tests ================

  test("FileStreamSource schema: no path") {
    def testError(): Unit = {
      val e = intercept[IllegalArgumentException] {
        createFileStreamSourceAndGetSchema(format = None, path = None, schema = None)
      }
      assert(e.getMessage.contains("path")) // reason is path, not schema
    }
    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") { testError() }
    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") { testError() }
  }

  test("FileStreamSource schema: path doesn't exist (without schema) should throw exception") {
    withTempDir { dir =>
      intercept[AnalysisException] {
        val userSchema = new StructType().add(new StructField("value", IntegerType))
        val schema = createFileStreamSourceAndGetSchema(
          format = None, path = Some(new File(dir, "1").getAbsolutePath), schema = None)
      }
    }
  }

  test("FileStreamSource schema: path doesn't exist (with schema) should throw exception") {
    withTempDir { dir =>
      intercept[AnalysisException] {
        val userSchema = new StructType().add(new StructField("value", IntegerType))
        val schema = createFileStreamSourceAndGetSchema(
          format = None, path = Some(new File(dir, "1").getAbsolutePath), schema = Some(userSchema))
      }
    }
  }


  // =============== Text file stream schema tests ================

  test("FileStreamSource schema: text, no existing files, no schema") {
    withTempDir { src =>
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("text"), path = Some(src.getCanonicalPath), schema = None)
      assert(schema === new StructType().add("value", StringType))
    }
  }

  test("FileStreamSource schema: text, existing files, no schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "a\nb\nc")
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("text"), path = Some(src.getCanonicalPath), schema = None)
      assert(schema === new StructType().add("value", StringType))
    }
  }

  test("FileStreamSource schema: text, existing files, schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "a\nb\nc")
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("text"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== ORC file stream schema tests ================

  test("FileStreamSource schema: orc, existing files, no schema") {
    withTempDir { src =>
      Seq("a", "b", "c").toDS().as("userColumn").toDF().write
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .orc(src.getCanonicalPath)

      // Without schema inference, should throw error
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") {
        intercept[IllegalArgumentException] {
          createFileStreamSourceAndGetSchema(
            format = Some("orc"), path = Some(src.getCanonicalPath), schema = None)
        }
      }

      // With schema inference, should infer correct schema
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        val schema = createFileStreamSourceAndGetSchema(
          format = Some("orc"), path = Some(src.getCanonicalPath), schema = None)
        assert(schema === new StructType().add("value", StringType))
      }
    }
  }

  test("FileStreamSource schema: orc, existing files, schema") {
    withTempPath { src =>
      Seq("a", "b", "c").toDS().as("oldUserColumn").toDF()
        .write.orc(new File(src, "1").getCanonicalPath)
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("orc"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== Parquet file stream schema tests ================

  test("FileStreamSource schema: parquet, existing files, no schema") {
    withTempDir { src =>
      Seq("a", "b", "c").toDS().as("userColumn").toDF().write
        .mode(org.apache.spark.sql.SaveMode.Overwrite)
        .parquet(src.getCanonicalPath)

      // Without schema inference, should throw error
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") {
        intercept[IllegalArgumentException] {
          createFileStreamSourceAndGetSchema(
            format = Some("parquet"), path = Some(src.getCanonicalPath), schema = None)
        }
      }

      // With schema inference, should infer correct schema
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        val schema = createFileStreamSourceAndGetSchema(
          format = Some("parquet"), path = Some(src.getCanonicalPath), schema = None)
        assert(schema === new StructType().add("value", StringType))
      }
    }
  }

  test("FileStreamSource schema: parquet, existing files, schema") {
    withTempPath { src =>
      Seq("a", "b", "c").toDS().as("oldUserColumn").toDF()
        .write.parquet(new File(src, "1").getCanonicalPath)
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("parquet"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== JSON file stream schema tests ================

  test("FileStreamSource schema: json, no existing files, no schema") {
    withTempDir { src =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        val e = intercept[AnalysisException] {
          createFileStreamSourceAndGetSchema(
            format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
        }
        assert("Unable to infer schema for JSON. It must be specified manually." === e.getMessage)
      }
    }
  }

  test("FileStreamSource schema: json, existing files, no schema") {
    withTempDir { src =>

      // Without schema inference, should throw error
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "false") {
        intercept[IllegalArgumentException] {
          createFileStreamSourceAndGetSchema(
            format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
        }
      }

      // With schema inference, should infer correct schema
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        stringToFile(new File(src, "1"), "{'c': '1'}\n{'c': '2'}\n{'c': '3'}")
        val schema = createFileStreamSourceAndGetSchema(
          format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
        assert(schema === new StructType().add("c", StringType))
      }
    }
  }

  test("FileStreamSource schema: json, existing files, schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "{'c': '1'}\n{'c': '2'}\n{'c', '3'}")
      val userSchema = new StructType().add("userColumn", StringType)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("json"), path = Some(src.getCanonicalPath), schema = Some(userSchema))
      assert(schema === userSchema)
    }
  }

  // =============== Text file stream tests ================

  test("read from text files") {
    withTempDirs { case (src, tmp) =>
      val textStream = createFileStream("text", src.getCanonicalPath)
      val filtered = textStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("SPARK-21996 read from text files -- file name has space") {
    withTempDirs { case (src, tmp) =>
      val textStream = createFileStream("text", src.getCanonicalPath)
      val filtered = textStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp, "text text"),
        CheckAnswer("keep2", "keep3")
      )
    }
  }

  test("SPARK-21996 read from text files generated by file sink -- file name has space") {
    val testTableName = "FileStreamSourceTest"
    withTable(testTableName) {
      withTempDirs { case (src, checkpoint) =>
        val output = new File(src, "text text")
        val inputData = MemoryStream[String]
        val ds = inputData.toDS()

        val query = ds.writeStream
          .option("checkpointLocation", checkpoint.getCanonicalPath)
          .format("text")
          .start(output.getCanonicalPath)

        try {
          inputData.addData("foo")
          failAfter(streamingTimeout) {
            query.processAllAvailable()
          }
        } finally {
          query.stop()
        }

        val df2 = spark.readStream.format("text").load(output.getCanonicalPath)
        val query2 = df2.writeStream.format("memory").queryName(testTableName).start()
        try {
          query2.processAllAvailable()
          checkDatasetUnorderly(spark.table(testTableName).as[String], "foo")
        } finally {
          query2.stop()
        }
      }
    }
  }

  test("Option pathGlobFilter") {
    val testTableName = "FileStreamSourceTest"
    withTable(testTableName) {
      withTempPath { output =>
        Seq("foo").toDS().write.text(output.getCanonicalPath)
        Seq("bar").toDS().write.mode("append").orc(output.getCanonicalPath)
        val df = spark.readStream.option("pathGlobFilter", "*.txt")
          .format("text").load(output.getCanonicalPath)
        val query = df.writeStream.format("memory").queryName(testTableName).start()
        try {
          query.processAllAvailable()
          checkDatasetUnorderly(spark.table(testTableName).as[String], "foo")
        } finally {
          query.stop()
        }
      }
    }
  }

  test("SPARK-31935: Hadoop file system config should be effective in data source options") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath
      val defaultFs = "nonexistFS://nonexistFS"
      val expectMessage = "No FileSystem for scheme nonexistFS"
      val message = intercept[java.io.IOException] {
        spark.readStream.option("fs.defaultFS", defaultFs).text(path)
      }.getMessage
      assert(message.filterNot(Set(':', '"').contains) == expectMessage)
    }
  }

  test("read from textfile") {
    withTempDirs { case (src, tmp) =>
      val textStream = spark.readStream.textFile(src.getCanonicalPath)
      val filtered = textStream.filter(_.contains("keep"))

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("SPARK-17165 should not track the list of seen files indefinitely") {
    // This test works by:
    // 1. Create a file
    // 2. Get it processed
    // 3. Sleeps for a very short amount of time (larger than maxFileAge
    // 4. Add another file (at this point the original file should have been purged
    // 5. Test the size of the seenFiles internal data structure

    // Note that if we change maxFileAge to a very large number, the last step should fail.
    withTempDirs { case (src, tmp) =>
      val textStream: DataFrame =
        createFileStream("text", src.getCanonicalPath, options = Map("maxFileAge" -> "5ms"))

      testStream(textStream)(
        AddTextFileData("a\nb", src, tmp),
        CheckAnswer("a", "b"),

        // SLeeps longer than 5ms (maxFileAge)
        // Unfortunately since a lot of file system does not have modification time granularity
        // finer grained than 1 sec, we need to use 1 sec here.
        AssertOnQuery { _ => Thread.sleep(1000); true },

        AddTextFileData("c\nd", src, tmp),
        CheckAnswer("a", "b", "c", "d"),

        AssertOnQuery("seen files should contain only one entry") { streamExecution =>
          val source = getSourcesFromStreamingQuery(streamExecution).head
          assert(source.seenFiles.size == 1)
          true
        }
      )
    }
  }

  // =============== JSON file stream tests ================

  test("read from json files") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("json", src.getCanonicalPath, Some(valueSchema))
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData(
          "{'value': 'drop1'}\n{'value': 'keep2'}\n{'value': 'keep3'}",
          src,
          tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData(
          "{'value': 'drop4'}\n{'value': 'keep5'}\n{'value': 'keep6'}",
          src,
          tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData(
          "{'value': 'drop7'}\n{'value': 'keep8'}\n{'value': 'keep9'}",
          src,
          tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("read from json files with inferring schema") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        stringToFile(new File(src, "existing"), "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}")

        val fileStream = createFileStream("json", src.getCanonicalPath)
        assert(fileStream.schema === StructType(Seq(StructField("c", StringType))))

        // FileStreamSource should infer the column "c"
        val filtered = fileStream.filter($"c" contains "keep")

        testStream(filtered)(
          AddTextFileData("{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6")
        )
      }
    }
  }

  test("reading from json files inside partitioned directory") {
    withTempDirs { case (baseSrc, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
        val src = new File(baseSrc, "type=X")
        src.mkdirs()

        // Add a file so that we can infer its schema
        stringToFile(new File(src, "existing"), "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}")

        val fileStream = createFileStream("json", src.getCanonicalPath)

        // FileStreamSource should infer the column "c"
        val filtered = fileStream.filter($"c" contains "keep")

        testStream(filtered)(
          AddTextFileData("{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6")
        )
      }
    }
  }

  test("reading from json files with changing schema") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        stringToFile(new File(src, "existing"), "{'k': 'value0'}")

        val fileStream = createFileStream("json", src.getCanonicalPath)

        // FileStreamSource should infer the column "k"
        assert(fileStream.schema === StructType(Seq(StructField("k", StringType))))

        // After creating DF and before starting stream, add data with different schema
        // Should not affect the inferred schema any more
        stringToFile(new File(src, "existing2"), "{'k': 'value1', 'v': 'new'}")

        testStream(fileStream)(

          // Should not pick up column v in the file added before start
          AddTextFileData("{'k': 'value2'}", src, tmp),
          CheckAnswer("value0", "value1", "value2"),

          // Should read data in column k, and ignore v
          AddTextFileData("{'k': 'value3', 'v': 'new'}", src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3"),

          // Should ignore rows that do not have the necessary k column
          AddTextFileData("{'v': 'value4'}", src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3", null))
      }
    }
  }

  // =============== ORC file stream tests ================

  test("read from orc files") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("orc", src.getCanonicalPath, Some(valueSchema))
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddOrcFileData(Seq("drop1", "keep2", "keep3"), src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddOrcFileData(Seq("drop4", "keep5", "keep6"), src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddOrcFileData(Seq("drop7", "keep8", "keep9"), src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("read from orc files with changing schema") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        AddOrcFileData.writeToFile(Seq("value0").toDF("k"), src, tmp)

        val fileStream = createFileStream("orc", src.getCanonicalPath)

        // FileStreamSource should infer the column "k"
        assert(fileStream.schema === StructType(Seq(StructField("k", StringType))))

        // After creating DF and before starting stream, add data with different schema
        // Should not affect the inferred schema any more
        AddOrcFileData.writeToFile(Seq(("value1", 0)).toDF("k", "v"), src, tmp)

        testStream(fileStream)(
          // Should not pick up column v in the file added before start
          AddOrcFileData(Seq("value2").toDF("k"), src, tmp),
          CheckAnswer("value0", "value1", "value2"),

          // Should read data in column k, and ignore v
          AddOrcFileData(Seq(("value3", 1)).toDF("k", "v"), src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3"),

          // Should ignore rows that do not have the necessary k column
          AddOrcFileData(Seq("value5").toDF("v"), src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3", null)
        )
      }
    }
  }

  // =============== Parquet file stream tests ================

  test("read from parquet files") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("parquet", src.getCanonicalPath, Some(valueSchema))
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddParquetFileData(Seq("drop1", "keep2", "keep3"), src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddParquetFileData(Seq("drop4", "keep5", "keep6"), src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddParquetFileData(Seq("drop7", "keep8", "keep9"), src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("read from parquet files with changing schema") {

    withTempDirs { case (src, tmp) =>
      withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {

        // Add a file so that we can infer its schema
        AddParquetFileData.writeToFile(Seq("value0").toDF("k"), src, tmp)

        val fileStream = createFileStream("parquet", src.getCanonicalPath)

        // FileStreamSource should infer the column "k"
        assert(fileStream.schema === StructType(Seq(StructField("k", StringType))))

        // After creating DF and before starting stream, add data with different schema
        // Should not affect the inferred schema any more
        AddParquetFileData.writeToFile(Seq(("value1", 0)).toDF("k", "v"), src, tmp)

        testStream(fileStream)(
          // Should not pick up column v in the file added before start
          AddParquetFileData(Seq("value2").toDF("k"), src, tmp),
          CheckAnswer("value0", "value1", "value2"),

          // Should read data in column k, and ignore v
          AddParquetFileData(Seq(("value3", 1)).toDF("k", "v"), src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3"),

          // Should ignore rows that do not have the necessary k column
          AddParquetFileData(Seq("value5").toDF("v"), src, tmp),
          CheckAnswer("value0", "value1", "value2", "value3", null)
        )
      }
    }
  }

  // =============== file stream globbing tests ================

  test("read new files in nested directories with globbing") {
    withTempDirs { case (dir, tmp) =>

      // src/*/* should consider all the files and directories that matches that glob.
      // So any files that matches the glob as well as any files in directories that matches
      // this glob should be read.
      val fileStream = createFileStream("text", s"${dir.getCanonicalPath}/*/*")
      val filtered = fileStream.filter($"value" contains "keep")
      val subDir = new File(dir, "subdir")
      val subSubDir = new File(subDir, "subsubdir")
      val subSubSubDir = new File(subSubDir, "subsubsubdir")

      require(!subDir.exists())
      require(!subSubDir.exists())

      testStream(filtered)(
        // Create new dir/subdir and write to it, should read
        AddTextFileData("drop1\nkeep2", subDir, tmp),
        CheckAnswer("keep2"),

        // Add files to dir/subdir, should read
        AddTextFileData("keep3", subDir, tmp),
        CheckAnswer("keep2", "keep3"),

        // Create new dir/subdir/subsubdir and write to it, should read
        AddTextFileData("keep4", subSubDir, tmp),
        CheckAnswer("keep2", "keep3", "keep4"),

        // Add files to dir/subdir/subsubdir, should read
        AddTextFileData("keep5", subSubDir, tmp),
        CheckAnswer("keep2", "keep3", "keep4", "keep5"),

        // 1. Add file to src dir, should not read as globbing src/*/* does not capture files in
        //    dir, only captures files in dir/subdir/
        // 2. Add files to dir/subDir/subsubdir/subsubsubdir, should not read as src/*/* should
        //    not capture those files
        AddTextFileData("keep6", dir, tmp),
        AddTextFileData("keep7", subSubSubDir, tmp),
        AddTextFileData("keep8", subDir, tmp), // needed to make query detect new data
        CheckAnswer("keep2", "keep3", "keep4", "keep5", "keep8")
      )
    }
  }

  test("read new files in partitioned table with globbing, should not read partition data") {
    withTempDirs { case (dir, tmp) =>
      val partitionFooSubDir = new File(dir, "partition=foo")
      val partitionBarSubDir = new File(dir, "partition=bar")

      val schema = new StructType().add("value", StringType).add("partition", StringType)
      val fileStream = createFileStream("json", s"${dir.getCanonicalPath}/*/*", Some(schema))
      val filtered = fileStream.filter($"value" contains "keep")
      val nullStr = null.asInstanceOf[String]
      testStream(filtered)(
        // Create new partition=foo sub dir and write to it, should read only value, not partition
        AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", nullStr)),

        // Append to same partition=1 sub dir, should read only value, not partition
        AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", nullStr), ("keep3", nullStr)),

        // Create new partition sub dir and write to it, should read only value, not partition
        AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", nullStr), ("keep3", nullStr), ("keep4", nullStr)),

        // Append to same partition=2 sub dir, should read only value, not partition
        AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", nullStr), ("keep3", nullStr), ("keep4", nullStr), ("keep5", nullStr))
      )
    }
  }

  // =============== other tests ================

  test("read new files in partitioned table without globbing, should read partition data") {
    withTempDirs { case (dir, tmp) =>
      val partitionFooSubDir = new File(dir, "partition=foo")
      val partitionBarSubDir = new File(dir, "partition=bar")

      val schema = new StructType().add("value", StringType).add("partition", StringType)
      val fileStream = createFileStream("json", s"${dir.getCanonicalPath}", Some(schema))
      val filtered = fileStream.filter($"value" contains "keep")
      testStream(filtered)(
        // Create new partition=foo sub dir and write to it
        AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", "foo")),

        // Append to same partition=foo sub dir
        AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo")),

        // Create new partition sub dir and write to it
        AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar")),

        // Append to same partition=bar sub dir
        AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar"))
      )
    }
  }

  test("read data from outputs of another streaming query") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3") {
      withTempDirs { case (outputDir, checkpointDir) =>
        // q1 is a streaming query that reads from memory and writes to text files
        val q1Source = MemoryStream[String]
        val q1 =
          q1Source
            .toDF()
            .writeStream
            .option("checkpointLocation", checkpointDir.getCanonicalPath)
            .format("text")
            .start(outputDir.getCanonicalPath)

        // q2 is a streaming query that reads q1's text outputs
        val q2 =
          createFileStream("text", outputDir.getCanonicalPath).filter($"value" contains "keep")

        def q1AddData(data: String*): StreamAction =
          Execute { _ =>
            q1Source.addData(data)
            q1.processAllAvailable()
          }
        def q2ProcessAllAvailable(): StreamAction = Execute { q2 => q2.processAllAvailable() }

        testStream(q2)(
          // batch 0
          q1AddData("drop1", "keep2"),
          q2ProcessAllAvailable(),
          CheckAnswer("keep2"),

          // batch 1
          Assert {
            // create a text file that won't be on q1's sink log
            // thus even if its content contains "keep", it should NOT appear in q2's answer
            val shouldNotKeep = new File(outputDir, "should_not_keep.txt")
            stringToFile(shouldNotKeep, "should_not_keep!!!")
            shouldNotKeep.exists()
          },
          q1AddData("keep3"),
          q2ProcessAllAvailable(),
          CheckAnswer("keep2", "keep3"),

          // batch 2: check that things work well when the sink log gets compacted
          q1AddData("keep4"),
          Assert {
            // compact interval is 3, so file "2.compact" should exist
            new File(outputDir, s"${FileStreamSink.metadataDir}/2.compact").exists()
          },
          q2ProcessAllAvailable(),
          CheckAnswer("keep2", "keep3", "keep4"),

          Execute { _ => q1.stop() }
        )
      }
    }
  }

  test("SPARK-35565: read data from outputs of another streaming query but ignore its metadata") {
    withSQLConf(SQLConf.FILE_SINK_LOG_COMPACT_INTERVAL.key -> "3",
        SQLConf.FILESTREAM_SINK_METADATA_IGNORED.key -> "true") {
      withTempDirs { case (outputDir, checkpointDir1) =>
        // q0 is a streaming query that reads from memory and writes to text files
        val q0Source = MemoryStream[String]
        val q0 =
          q0Source
            .toDF()
            .writeStream
            .option("checkpointLocation", checkpointDir1.getCanonicalPath)
            .format("text")
            .start(outputDir.getCanonicalPath)

        q0Source.addData("keep0")
        q0.processAllAvailable()
        q0.stop()
        Utils.deleteRecursively(new File(outputDir.getCanonicalPath + "/" +
          FileStreamSink.metadataDir))

        withTempDir { checkpointDir2 =>
          // q1 is a streaming query that reads from memory and writes to text files too
          val q1Source = MemoryStream[String]
          val q1 =
            q1Source
              .toDF()
              .writeStream
              .option("checkpointLocation", checkpointDir2.getCanonicalPath)
              .format("text")
              .start(outputDir.getCanonicalPath)

          // q2 is a streaming query that reads both q0 and q1's text outputs
          val q2 =
            createFileStream("text", outputDir.getCanonicalPath).filter($"value" contains "keep")

          def q1AddData(data: String*): StreamAction =
            Execute { _ =>
              q1Source.addData(data)
              q1.processAllAvailable()
            }

          def q2ProcessAllAvailable(): StreamAction = Execute { q2 => q2.processAllAvailable() }

          testStream(q2)(
            // batch 0
            q1AddData("drop1", "keep2"),
            q2ProcessAllAvailable(),
            CheckAnswer("keep0", "keep2"),

            q1AddData("keep3"),
            q2ProcessAllAvailable(),
            CheckAnswer("keep0", "keep2", "keep3"),

            // batch 2: check that things work well when the sink log gets compacted
            q1AddData("keep4"),
            Assert {
              // compact interval is 3, so file "2.compact" should exist
              new File(outputDir, s"${FileStreamSink.metadataDir}/2.compact").exists()
            },
            q2ProcessAllAvailable(),
            CheckAnswer("keep0", "keep2", "keep3", "keep4"),

            Execute { _ => q1.stop() }
          )
        }
      }
    }
  }

  test("start before another streaming query, and read its output") {
    withTempDirs { case (outputDir, checkpointDir) =>
      // q1 is a streaming query that reads from memory and writes to text files
      val q1Source = MemoryStream[String]
      // define q1, but don't start it for now
      val q1Write =
        q1Source
          .toDF()
          .writeStream
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .format("text")
      var q1: StreamingQuery = null

      val q2 = createFileStream("text", outputDir.getCanonicalPath).filter($"value" contains "keep")

      testStream(q2)(
        AssertOnQuery { q2 =>
          val fileSource = getSourcesFromStreamingQuery(q2).head
          // q1 has not started yet, verify that q2 doesn't know whether q1 has metadata
          fileSource.sourceHasMetadata === None
        },
        Execute { _ =>
          q1 = q1Write.start(outputDir.getCanonicalPath)
          q1Source.addData("drop1", "keep2")
          q1.processAllAvailable()
        },
        AssertOnQuery { q2 =>
          q2.processAllAvailable()
          val fileSource = getSourcesFromStreamingQuery(q2).head
          // q1 has started, verify that q2 knows q1 has metadata by now
          fileSource.sourceHasMetadata === Some(true)
        },
        CheckAnswer("keep2"),
        Execute { _ => q1.stop() }
      )
    }
  }

  test("when schema inference is turned on, should read partition data") {
    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
      withTempDirs { case (dir, tmp) =>
        val partitionFooSubDir = new File(dir, "partition=foo")
        val partitionBarSubDir = new File(dir, "partition=bar")

        // Create file in partition, so we can infer the schema.
        createFile("{'value': 'drop0'}", partitionFooSubDir, tmp)

        val fileStream = createFileStream("json", s"${dir.getCanonicalPath}")
        val filtered = fileStream.filter($"value" contains "keep")
        testStream(filtered)(
          // Append to same partition=foo sub dir
          AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
          CheckAnswer(("keep2", "foo")),

          // Append to same partition=foo sub dir
          AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo")),

          // Create new partition sub dir and write to it
          AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar")),

          // Append to same partition=bar sub dir
          AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar")),

          AddTextFileData("{'value': 'keep6'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar"),
            ("keep6", "bar"))
        )
      }
    }
  }

  test("fault tolerance") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("text", src.getCanonicalPath)
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream(),
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("max files per trigger") {
    withTempDir { case src =>
      var lastFileModTime: Option[Long] = None

      /** Create a text file with a single data item */
      def createFile(data: Int): File = {
        val file = stringToFile(new File(src, s"$data.txt"), data.toString)
        if (lastFileModTime.nonEmpty) file.setLastModified(lastFileModTime.get + 1000)
        lastFileModTime = Some(file.lastModified)
        file
      }

      createFile(1)
      createFile(2)
      createFile(3)

      // Set up a query to read text files 2 at a time
      val df = spark
        .readStream
        .option("maxFilesPerTrigger", 2)
        .text(src.getCanonicalPath)
      val q = df
        .writeStream
        .format("memory")
        .queryName("file_data")
        .start()
        .asInstanceOf[StreamingQueryWrapper]
        .streamingQuery
      q.processAllAvailable()
      val memorySink = q.sink.asInstanceOf[MemorySink]
      val fileSource = getSourcesFromStreamingQuery(q).head

      /** Check the data read in the last batch */
      def checkLastBatchData(data: Int*): Unit = {
        val schema = StructType(Seq(StructField("value", StringType)))
        val df = spark.createDataFrame(
          spark.sparkContext.makeRDD(memorySink.latestBatchData), schema)
        checkAnswer(df, data.map(_.toString).toDF("value"))
      }

      def checkAllData(data: Seq[Int]): Unit = {
        val schema = StructType(Seq(StructField("value", StringType)))
        val df = spark.createDataFrame(
          spark.sparkContext.makeRDD(memorySink.allData), schema)
        checkAnswer(df, data.map(_.toString).toDF("value"))
      }

      /** Check how many batches have executed since the last time this check was made */
      var lastBatchId = -1L
      def checkNumBatchesSinceLastCheck(numBatches: Int): Unit = {
        require(lastBatchId >= 0)
        assert(memorySink.latestBatchId.get === lastBatchId + numBatches)
        lastBatchId = memorySink.latestBatchId.get
      }

      checkLastBatchData(3)  // (1 and 2) should be in batch 1, (3) should be in batch 2 (last)
      checkAllData(1 to 3)
      lastBatchId = memorySink.latestBatchId.get

      fileSource.withBatchingLocked {
        createFile(4)
        createFile(5)   // 4 and 5 should be in a batch
        createFile(6)
        createFile(7)   // 6 and 7 should be in the last batch
      }
      q.processAllAvailable()
      checkNumBatchesSinceLastCheck(2)
      checkLastBatchData(6, 7)
      checkAllData(1 to 7)

      fileSource.withBatchingLocked {
        createFile(8)
        createFile(9)    // 8 and 9 should be in a batch
        createFile(10)
        createFile(11)   // 10 and 11 should be in a batch
        createFile(12)   // 12 should be in the last batch
      }
      q.processAllAvailable()
      checkNumBatchesSinceLastCheck(3)
      checkLastBatchData(12)
      checkAllData(1 to 12)

      q.stop()
    }
  }

  testQuietly("max files per trigger - incorrect values") {
    val testTable = "maxFilesPerTrigger_test"
    withTable(testTable) {
      withTempDir { case src =>
        def testMaxFilePerTriggerValue(value: String): Unit = {
          val df = spark.readStream.option("maxFilesPerTrigger", value).text(src.getCanonicalPath)
          val e = intercept[StreamingQueryException] {
            // Note: `maxFilesPerTrigger` is checked in the stream thread when creating the source
            val q = df.writeStream.format("memory").queryName(testTable).start()
            try {
              q.processAllAvailable()
            } finally {
              q.stop()
            }
          }
          assert(e.getCause.isInstanceOf[IllegalArgumentException])
          Seq("maxFilesPerTrigger", value, "positive integer").foreach { s =>
            assert(e.getMessage.contains(s))
          }
        }

        testMaxFilePerTriggerValue("not-a-integer")
        testMaxFilePerTriggerValue("-1")
        testMaxFilePerTriggerValue("0")
        testMaxFilePerTriggerValue("10.1")
      }
    }
  }

  test("SPARK-30669: maxFilesPerTrigger - ignored when using Trigger.Once") {
    withTempDirs { (src, target) =>
      val checkpoint = new File(target, "chk").getCanonicalPath
      val targetDir = new File(target, "data").getCanonicalPath
      var lastFileModTime: Option[Long] = None

      /** Create a text file with a single data item */
      def createFile(data: Int): File = {
        val file = stringToFile(new File(src, s"$data.txt"), data.toString)
        if (lastFileModTime.nonEmpty) file.setLastModified(lastFileModTime.get + 1000)
        lastFileModTime = Some(file.lastModified)
        file
      }

      createFile(1)
      createFile(2)
      createFile(3)

      // Set up a query to read text files one at a time
      val df = spark
        .readStream
        .option("maxFilesPerTrigger", 1)
        .text(src.getCanonicalPath)

      def startQuery(): StreamingQuery = {
        df.writeStream
          .format("parquet")
          .trigger(Trigger.Once)
          .option("checkpointLocation", checkpoint)
          .start(targetDir)
      }
      val q = startQuery()

      try {
        assert(q.awaitTermination(streamingTimeout.toMillis))
        assert(q.recentProgress.count(_.numInputRows != 0) == 1) // only one trigger was run
        checkAnswer(sql(s"SELECT * from parquet.`$targetDir`"), (1 to 3).map(_.toString).toDF)
      } finally {
        q.stop()
      }

      createFile(4)
      createFile(5)

      // run a second batch
      val q2 = startQuery()
      try {
        assert(q2.awaitTermination(streamingTimeout.toMillis))
        assert(q2.recentProgress.count(_.numInputRows != 0) == 1) // only one trigger was run
        checkAnswer(sql(s"SELECT * from parquet.`$targetDir`"), (1 to 5).map(_.toString).toDF)
      } finally {
        q2.stop()
      }
    }
  }

  test("explain") {
    withTempDirs { case (src, tmp) =>
      src.mkdirs()

      val df = spark.readStream.format("text").load(src.getCanonicalPath).map(_ + "-x")
      // Test `explain` not throwing errors
      df.explain()

      val q = df.writeStream.queryName("file_explain").format("memory").start()
        .asInstanceOf[StreamingQueryWrapper]
        .streamingQuery
      try {
        assert("No physical plan. Waiting for data." === q.explainInternal(false))
        assert("No physical plan. Waiting for data." === q.explainInternal(true))

        val tempFile = Utils.tempFileWith(new File(tmp, "text"))
        val finalFile = new File(src, tempFile.getName)
        require(stringToFile(tempFile, "foo").renameTo(finalFile))

        q.processAllAvailable()

        val explainWithoutExtended = q.explainInternal(false)
        // `extended = false` only displays the physical plan.
        assert("Relation.*text".r.findAllMatchIn(explainWithoutExtended).size === 0)
        assert(": Text".r.findAllMatchIn(explainWithoutExtended).size === 1)

        val explainWithExtended = q.explainInternal(true)
        // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
        // plan.
        assert("Relation.*text".r.findAllMatchIn(explainWithExtended).size === 3)
        assert(": Text".r.findAllMatchIn(explainWithExtended).size === 1)
      } finally {
        q.stop()
      }
    }
  }

  test("SPARK-17372 - write file names to WAL as Array[String]") {
    // Note: If this test takes longer than the timeout, then its likely that this is actually
    // running a Spark job with 10000 tasks. This test tries to avoid that by
    // 1. Setting the threshold for parallel file listing to very high
    // 2. Using a query that should use constant folding to eliminate reading of the files

    val numFiles = 10000

    // This is to avoid running a spark job to list of files in parallel
    // by the InMemoryFileIndex.
    spark.sessionState.conf.setConf(SQLConf.PARALLEL_PARTITION_DISCOVERY_THRESHOLD, numFiles * 2)

    withTempDirs { case (root, tmp) =>
      val src = new File(root, "a=1")
      src.mkdirs()

      (1 to numFiles).map { _.toString }.foreach { i =>
        val tempFile = Utils.tempFileWith(new File(tmp, "text"))
        val finalFile = new File(src, tempFile.getName)
        stringToFile(finalFile, i)
      }
      assert(src.listFiles().size === numFiles)

      val files = spark.readStream.text(root.getCanonicalPath).as[(String, Int)]

      // Note this query will use constant folding to eliminate the file scan.
      // This is to avoid actually running a Spark job with 10000 tasks
      val df = files.filter("1 == 0").groupBy().count()

      testStream(df, OutputMode.Complete)(
        AddTextFileData("0", src, tmp),
        CheckAnswer(0)
      )
    }
  }

  test("compact interval metadata log") {
    val _sources = PrivateMethod[Seq[Source]](Symbol("sources"))
    val _metadataLog = PrivateMethod[FileStreamSourceLog](Symbol("metadataLog"))

    def verify(
        execution: StreamExecution,
        batchId: Long,
        expectedBatches: Int,
        expectedCompactInterval: Int): Boolean = {
      import CompactibleFileStreamLog._

      val fileSource = getSourcesFromStreamingQuery(execution).head
      val metadataLog = fileSource invokePrivate _metadataLog()

      if (isCompactionBatch(batchId, expectedCompactInterval)) {
        val path = metadataLog.batchIdToPath(batchId)

        // Assert path name should be ended with compact suffix.
        assert(path.getName.endsWith(COMPACT_FILE_SUFFIX),
          "path does not end with compact file suffix")

        // Compacted batch should include all entries from start.
        val entries = metadataLog.get(batchId)
        assert(entries.isDefined, "Entries not defined")
        assert(entries.get.length === metadataLog.allFiles().length, "clean up check")
        assert(metadataLog.get(None, Some(batchId)).flatMap(_._2).length ===
          entries.get.length, "Length check")
      }

      assert(metadataLog.allFiles().sortBy(_.batchId) ===
        metadataLog.get(None, Some(batchId)).flatMap(_._2).sortBy(_.batchId),
        "Batch id mismatch")

      metadataLog.get(None, Some(batchId)).flatMap(_._2).length === expectedBatches
    }

    withTempDirs { case (src, tmp) =>
      withSQLConf(
        SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2"
      ) {
        val fileStream = createFileStream("text", src.getCanonicalPath)
        val filtered = fileStream.filter($"value" contains "keep")
        val updateConf = Map(SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "5")

        testStream(filtered)(
          AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
          CheckAnswer("keep2", "keep3"),
          AssertOnQuery(verify(_, 0L, 1, 2)),
          AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6"),
          AssertOnQuery(verify(_, 1L, 2, 2)),
          AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9"),
          AssertOnQuery(verify(_, 2L, 3, 2)),
          StopStream,
          StartStream(additionalConfs = updateConf),
          AssertOnQuery(verify(_, 2L, 3, 2)),
          AddTextFileData("drop10\nkeep11", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9", "keep11"),
          AssertOnQuery(verify(_, 3L, 4, 2)),
          AddTextFileData("drop12\nkeep13", src, tmp),
          CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9", "keep11", "keep13"),
          AssertOnQuery(verify(_, 4L, 5, 2))
        )
      }
    }
  }

  test("restore from file stream source log") {
    def createEntries(batchId: Long, count: Int): Array[FileEntry] = {
      (1 to count).map { idx =>
        FileEntry(s"path_${batchId}_$idx", 10000 * batchId + count, batchId)
      }.toArray
    }

    withSQLConf(SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "5") {
      def verifyBatchAvailabilityInCache(
          fileEntryCache: java.util.LinkedHashMap[Long, Array[FileEntry]],
          expectNotAvailable: Seq[Int],
          expectAvailable: Seq[Int]): Unit = {
        expectNotAvailable.foreach { batchId =>
          assert(!fileEntryCache.containsKey(batchId.toLong))
        }
        expectAvailable.foreach { batchId =>
          assert(fileEntryCache.containsKey(batchId.toLong))
        }
      }
      withTempDir { chk =>
        val _fileEntryCache = PrivateMethod[java.util.LinkedHashMap[Long, Array[FileEntry]]](
          Symbol("fileEntryCache"))

        val metadata = new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark,
          chk.getCanonicalPath)
        val fileEntryCache = metadata invokePrivate _fileEntryCache()

        (0 to 4).foreach { batchId =>
          metadata.add(batchId, createEntries(batchId, 100))
        }
        val allFiles = metadata.allFiles()

        // batch 4 is a compact batch which logs would be cached in fileEntryCache
        verifyBatchAvailabilityInCache(fileEntryCache, Seq(0, 1, 2, 3), Seq(4))

        val metadata2 = new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark,
          chk.getCanonicalPath)
        val fileEntryCache2 = metadata2 invokePrivate _fileEntryCache()

        // allFiles() doesn't restore the logs for the latest compact batch into file entry cache
        assert(metadata2.allFiles() === allFiles)
        verifyBatchAvailabilityInCache(fileEntryCache2, Seq(0, 1, 2, 3, 4), Seq.empty)

        // restore() will restore the logs for the latest compact batch into file entry cache
        assert(metadata2.restore() === allFiles)
        verifyBatchAvailabilityInCache(fileEntryCache2, Seq(0, 1, 2, 3), Seq(4))

        (5 to 5 + FileStreamSourceLog.PREV_NUM_BATCHES_TO_READ_IN_RESTORE).foreach { batchId =>
          metadata2.add(batchId, createEntries(batchId, 100))
        }

        val metadata3 = new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark,
          chk.getCanonicalPath)
        val fileEntryCache3 = metadata3 invokePrivate _fileEntryCache()

        // restore() will not restore the logs for the latest compact batch into file entry cache
        // if the latest batch is too far from latest compact batch, because it's unlikely Spark
        // will request the batch for the start point.
        assert(metadata3.restore() === metadata2.allFiles())
        verifyBatchAvailabilityInCache(fileEntryCache3, Seq(0, 1, 2, 3, 4), Seq.empty)
      }
    }
  }

  test("get arbitrary batch from FileStreamSource") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(
        SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2",
        // Force deleting the old logs
        SQLConf.FILE_SOURCE_LOG_CLEANUP_DELAY.key -> "1"
      ) {
        val fileStream = createFileStream("text", src.getCanonicalPath)
        val filtered = fileStream.filter($"value" contains "keep")

        testStream(filtered)(
          AddTextFileData("keep1", src, tmp),
          CheckAnswer("keep1"),
          AddTextFileData("keep2", src, tmp),
          CheckAnswer("keep1", "keep2"),
          AddTextFileData("keep3", src, tmp),
          CheckAnswer("keep1", "keep2", "keep3"),
          AssertOnQuery("check getBatch") { execution: StreamExecution =>
            val _sources = PrivateMethod[Seq[Source]](Symbol("sources"))
            val fileSource = getSourcesFromStreamingQuery(execution).head

            def verify(startId: Option[Int], endId: Int, expected: String*): Unit = {
              val start = startId.map(new FileStreamSourceOffset(_))
              val end = FileStreamSourceOffset(endId)

              withSQLConf(SQLConf.UNSUPPORTED_OPERATION_CHECK_ENABLED.key -> "false") {
                assert(fileSource.getBatch(start, end).as[String].collect().toSeq === expected)
              }
            }

            verify(startId = None, endId = 2, "keep1", "keep2", "keep3")
            verify(startId = Some(0), endId = 1, "keep2")
            verify(startId = Some(0), endId = 2, "keep2", "keep3")
            verify(startId = Some(1), endId = 2, "keep3")
            true
          }
        )
      }
    }
  }

  test("input row metrics") {
    withTempDirs { case (src, tmp) =>
      val input = spark.readStream.format("text").load(src.getCanonicalPath)
      testStream(input)(
        AddTextFileData("100", src, tmp),
        CheckAnswer("100"),
        AssertOnQuery { query =>
          val actualProgress = query.recentProgress
              .find(_.numInputRows > 0)
              .getOrElse(sys.error("Could not find records with data."))
          assert(actualProgress.numInputRows === 1)
          assert(actualProgress.sources(0).processedRowsPerSecond > 0.0)
          true
        }
      )
    }
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    val options = new FileStreamOptions(Map("maxfilespertrigger" -> "1"))
    assert(options.maxFilesPerTrigger == Some(1))
  }

  test("FileStreamSource offset - read Spark 2.1.0 offset json format") {
    val offset = readOffsetFromResource("file-source-offset-version-2.1.0-json.txt")
    assert(FileStreamSourceOffset(offset) === FileStreamSourceOffset(345))
  }

  test("FileStreamSource offset - read Spark 2.1.0 offset long format") {
    val offset = readOffsetFromResource("file-source-offset-version-2.1.0-long.txt")
    assert(FileStreamSourceOffset(offset) === FileStreamSourceOffset(345))
  }

  test("FileStreamSourceLog - read Spark 2.1.0 log format") {
    assert(readLogFromResource("file-source-log-version-2.1.0") === Seq(
      FileEntry("/a/b/0", 1480730949000L, 0L),
      FileEntry("/a/b/1", 1480730950000L, 1L),
      FileEntry("/a/b/2", 1480730950000L, 2L),
      FileEntry("/a/b/3", 1480730950000L, 3L),
      FileEntry("/a/b/4", 1480730951000L, 4L)
    ))
  }

  private def readLogFromResource(dir: String): Seq[FileEntry] = {
    val input = getClass.getResource(s"/structured-streaming/$dir")
    val log = new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark, input.toString)
    log.allFiles()
  }

  private def readOffsetFromResource(file: String): SerializedOffset = {
    import scala.io.Source
    Utils.tryWithResource(
      Source.fromFile(getClass.getResource(s"/structured-streaming/$file").toURI)) { source =>
      SerializedOffset(source.mkString.trim)
    }
  }

  private def runTwoBatchesAndVerifyResults(
      src: File,
      latestFirst: Boolean,
      firstBatch: String,
      secondBatch: String,
      maxFileAge: Option[String] = None,
      cleanSource: CleanSourceMode.Value = CleanSourceMode.OFF,
      archiveDir: Option[String] = None): Unit = {
    val srcOptions = Map("latestFirst" -> latestFirst.toString, "maxFilesPerTrigger" -> "1") ++
      maxFileAge.map("maxFileAge" -> _) ++
      Seq("cleanSource" -> cleanSource.toString) ++
      archiveDir.map("sourceArchiveDir" -> _)
    val fileStream = createFileStream(
      "text",
      src.getCanonicalPath,
      options = srcOptions)
    val clock = new StreamManualClock()
    testStream(fileStream)(
      StartStream(trigger = Trigger.ProcessingTime(10), triggerClock = clock),
      AssertOnQuery { _ =>
        // Block until the first batch finishes.
        eventually(timeout(streamingTimeout)) {
          assert(clock.isStreamWaitingAt(0))
        }
        true
      },
      CheckLastBatch(firstBatch),
      AdvanceManualClock(10),
      AssertOnQuery { _ =>
        // Block until the second batch finishes.
        eventually(timeout(streamingTimeout)) {
          assert(clock.isStreamWaitingAt(10))
        }
        true
      },
      CheckLastBatch(secondBatch)
    )
  }

  test("FileStreamSource - latestFirst") {
    withTempDir { src =>
      // Prepare two files: 1.txt, 2.txt, and make sure they have different modified time.
      val f1 = stringToFile(new File(src, "1.txt"), "1")
      val f2 = stringToFile(new File(src, "2.txt"), "2")
      f2.setLastModified(f1.lastModified + 1000)

      // Read oldest files first, so the first batch is "1", and the second batch is "2".
      runTwoBatchesAndVerifyResults(src, latestFirst = false, firstBatch = "1", secondBatch = "2")

      // Read latest files first, so the first batch is "2", and the second batch is "1".
      runTwoBatchesAndVerifyResults(src, latestFirst = true, firstBatch = "2", secondBatch = "1")
    }
  }

  test("SPARK-19813: Ignore maxFileAge when maxFilesPerTrigger and latestFirst is used") {
    withTempDir { src =>
      // Prepare two files: 1.txt, 2.txt, and make sure they have different modified time.
      val f1 = stringToFile(new File(src, "1.txt"), "1")
      val f2 = stringToFile(new File(src, "2.txt"), "2")
      f2.setLastModified(f1.lastModified + 3600 * 1000 /* 1 hour later */)

      runTwoBatchesAndVerifyResults(src, latestFirst = true, firstBatch = "2", secondBatch = "1",
        maxFileAge = Some("1m") /* 1 minute */)
    }
  }

  test("SeenFilesMap") {
    val map = new SeenFilesMap(maxAgeMs = 10, fileNameOnly = false)

    map.add("a", 5)
    assert(map.size == 1)
    map.purge()
    assert(map.size == 1)

    // Add a new entry and purge should be no-op, since the gap is exactly 10 ms.
    map.add("b", 15)
    assert(map.size == 2)
    map.purge()
    assert(map.size == 2)

    // Add a new entry that's more than 10 ms than the first entry. We should be able to purge now.
    map.add("c", 16)
    assert(map.size == 3)
    map.purge()
    assert(map.size == 2)

    // Override existing entry shouldn't change the size
    map.add("c", 25)
    assert(map.size == 2)

    // Not a new file because we have seen c before
    assert(!map.isNewFile("c", 20))

    // Not a new file because timestamp is too old
    assert(!map.isNewFile("d", 5))

    // Finally a new file: never seen and not too old
    assert(map.isNewFile("e", 20))
  }

  test("SeenFilesMap with fileNameOnly = true") {
    val map = new SeenFilesMap(maxAgeMs = 10, fileNameOnly = true)

    map.add("file:///a/b/c/d", 5)
    map.add("file:///a/b/c/e", 5)
    assert(map.size === 2)

    assert(!map.isNewFile("d", 5))
    assert(!map.isNewFile("file:///d", 5))
    assert(!map.isNewFile("file:///x/d", 5))
    assert(!map.isNewFile("file:///x/y/d", 5))

    map.add("s3:///bucket/d", 5)
    map.add("s3n:///bucket/d", 5)
    map.add("s3a:///bucket/d", 5)
    assert(map.size === 2)
  }

  test("SeenFilesMap should only consider a file old if it is earlier than last purge time") {
    val map = new SeenFilesMap(maxAgeMs = 10, fileNameOnly = false)

    map.add("a", 20)
    assert(map.size == 1)

    // Timestamp 5 should still considered a new file because purge time should be 0
    assert(map.isNewFile("b", 9))
    assert(map.isNewFile("b", 10))

    // Once purge, purge time should be 10 and then b would be a old file if it is less than 10.
    map.purge()
    assert(!map.isNewFile("b", 9))
    assert(map.isNewFile("b", 10))
  }

  test("do not recheck that files exist during getBatch") {
    val scheme = ExistsThrowsExceptionFileSystem.scheme
    withTempDir { temp =>
      spark.conf.set(
        s"fs.$scheme.impl",
        classOf[ExistsThrowsExceptionFileSystem].getName)
      // add the metadata entries as a pre-req
      val dir = new File(temp, "dir") // use non-existent directory to test whether log make the dir
    val metadataLog =
      new FileStreamSourceLog(FileStreamSourceLog.VERSION, spark, dir.getAbsolutePath)
      assert(metadataLog.add(0, Array(FileEntry(s"$scheme:///file1", 100L, 0))))
      assert(metadataLog.add(1, Array(FileEntry(s"$scheme:///file2", 200L, 0))))

      val newSource = new FileStreamSource(spark, s"$scheme:///", "parquet", StructType(Nil), Nil,
        dir.getAbsolutePath, Map.empty)
      // this method should throw an exception if `fs.exists` is called during resolveRelation
      newSource.getBatch(None, FileStreamSourceOffset(1))
    }
  }

  test("SPARK-26629: multiple file sources work with restarts when a source does not have data") {
    withTempDirs { case (dir, tmp) =>
      val sourceDir1 = new File(dir, "source1")
      val sourceDir2 = new File(dir, "source2")
      sourceDir1.mkdirs()
      sourceDir2.mkdirs()

      val source1 = createFileStream("text", s"${sourceDir1.getCanonicalPath}")
      val source2 = createFileStream("text", s"${sourceDir2.getCanonicalPath}")
      val unioned = source1.union(source2)

      def addMultiTextFileData(
          source1Content: String,
          source2Content: String): StreamAction = {
        val actions = Seq(
          AddTextFileData(source1Content, sourceDir1, tmp),
          AddTextFileData(source2Content, sourceDir2, tmp)
        ).filter(_.content != null) // don't write to a source dir if no content specified
        StreamProgressLockedActions(actions, desc = actions.mkString("[ ", " | ", " ]"))
      }

      testStream(unioned)(
        StartStream(),
        addMultiTextFileData(source1Content = "source1_0", source2Content = "source2_0"),
        CheckNewAnswer("source1_0", "source2_0"),
        StopStream,

        StartStream(),
        addMultiTextFileData(source1Content = "source1_1", source2Content = null),
        CheckNewAnswer("source1_1"),
        StopStream,

        // Restart after a batch with one file source having no new data.
        // This restart is needed to hit the issue in SPARK-26629.

        StartStream(),
        addMultiTextFileData(source1Content = null, source2Content = "source2_2"),
        CheckNewAnswer("source2_2"),
        StopStream,

        StartStream(),
        addMultiTextFileData(source1Content = "source1_3", source2Content = "source2_3"),
        CheckNewAnswer("source1_3", "source2_3"),
        StopStream
      )
    }
  }

  test("SPARK-28651: force streaming file source to be nullable") {
    withTempDir { temp =>
      val schema = StructType(Seq(StructField("foo", LongType, false)))
      val nullableSchema = StructType(Seq(StructField("foo", LongType, true)))
      val streamingSchema = spark.readStream.schema(schema).json(temp.getCanonicalPath).schema
      assert(nullableSchema === streamingSchema)

      // Verify we have the same behavior as batch DataFrame.
      val batchSchema = spark.read.schema(schema).json(temp.getCanonicalPath).schema
      assert(batchSchema === streamingSchema)

      // Verify the flag works
      withSQLConf(SQLConf.FILE_SOURCE_SCHEMA_FORCE_NULLABLE.key -> "false") {
        val streamingSchema = spark.readStream.schema(schema).json(temp.getCanonicalPath).schema
        assert(schema === streamingSchema)
      }
    }
  }

  test("remove completed files when remove option is enabled") {
    withTempDirs { case (src, tmp) =>
      withSQLConf(
        SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2",
        // Force deleting the old logs
        SQLConf.FILE_SOURCE_LOG_CLEANUP_DELAY.key -> "1",
        SQLConf.FILE_SOURCE_CLEANER_NUM_THREADS.key -> "0"
      ) {
        val option = Map("latestFirst" -> "false", "maxFilesPerTrigger" -> "1",
          "cleanSource" -> "delete")

        val fileStream = createFileStream("text", src.getCanonicalPath, options = option)
        val filtered = fileStream.filter($"value" contains "keep")

        testStream(filtered)(
          AddTextFileData("keep1", src, tmp, tmpFilePrefix = "keep1"),
          CheckAnswer("keep1"),
          AssertOnQuery("input file removed") { _: StreamExecution =>
            // it doesn't rename any file yet
            assertFileIsNotRemoved(src, "keep1")
            true
          },
          AddTextFileData("keep2", src, tmp, tmpFilePrefix = "ke ep2 %"),
          CheckAnswer("keep1", "keep2"),
          AssertOnQuery("input file removed") { _: StreamExecution =>
            // it renames input file for first batch, but not for second batch yet
            assertFileIsRemoved(src, "keep1")
            assertFileIsNotRemoved(src, "ke ep2 %")

            true
          },
          AddTextFileData("keep3", src, tmp, tmpFilePrefix = "keep3"),
          CheckAnswer("keep1", "keep2", "keep3"),
          AssertOnQuery("input file renamed") { _: StreamExecution =>
            // it renames input file for second batch, but not third batch yet
            assertFileIsRemoved(src, "ke ep2 %")
            assertFileIsNotRemoved(src, "keep3")

            true
          }
        )
      }
    }
  }

  test("move completed files to archive directory when archive option is enabled") {
    withThreeTempDirs { case (src, tmp, archiveDir) =>
      withSQLConf(
        SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2",
        // Force deleting the old logs
        SQLConf.FILE_SOURCE_LOG_CLEANUP_DELAY.key -> "1",
        SQLConf.FILE_SOURCE_CLEANER_NUM_THREADS.key -> "0"
      ) {
        val option = Map("latestFirst" -> "false", "maxFilesPerTrigger" -> "1",
          "cleanSource" -> "archive", "sourceArchiveDir" -> archiveDir.getAbsolutePath)

        val fileStream = createFileStream("text", s"${src.getCanonicalPath}/*/*",
          options = option)
        val filtered = fileStream.filter($"value" contains "keep")

        // src/k %1
        // file: src/k %1/keep1
        val dirForKeep1 = new File(src, "k %1")
        // src/k %1/k 2
        // file: src/k %1/k 2/keep2
        val dirForKeep2 = new File(dirForKeep1, "k 2")
        // src/k3
        // file: src/k3/keep3
        val dirForKeep3 = new File(src, "k3")

        val expectedMovedDir1 = new File(archiveDir.getAbsolutePath + dirForKeep1.toURI.getPath)
        val expectedMovedDir2 = new File(archiveDir.getAbsolutePath + dirForKeep2.toURI.getPath)
        val expectedMovedDir3 = new File(archiveDir.getAbsolutePath + dirForKeep3.toURI.getPath)

        testStream(filtered)(
          AddTextFileData("keep1", dirForKeep1, tmp, tmpFilePrefix = "keep1"),
          CheckAnswer("keep1"),
          AssertOnQuery("input file archived") { _: StreamExecution =>
            // it doesn't rename any file yet
            assertFileIsNotMoved(dirForKeep1, expectedMovedDir1, "keep1")
            true
          },
          AddTextFileData("keep2", dirForKeep2, tmp, tmpFilePrefix = "keep2 %"),
          CheckAnswer("keep1", "keep2"),
          AssertOnQuery("input file archived") { _: StreamExecution =>
            // it renames input file for first batch, but not for second batch yet
            assertFileIsMoved(dirForKeep1, expectedMovedDir1, "keep1")
            assertFileIsNotMoved(dirForKeep2, expectedMovedDir2, "keep2 %")
            true
          },
          AddTextFileData("keep3", dirForKeep3, tmp, tmpFilePrefix = "keep3"),
          CheckAnswer("keep1", "keep2", "keep3"),
          AssertOnQuery("input file archived") { _: StreamExecution =>
            // it renames input file for second batch, but not third batch yet
            assertFileIsMoved(dirForKeep2, expectedMovedDir2, "keep2 %")
            assertFileIsNotMoved(dirForKeep3, expectedMovedDir3, "keep3")

            true
          },
          AddTextFileData("keep4", dirForKeep3, tmp, tmpFilePrefix = "keep4"),
          CheckAnswer("keep1", "keep2", "keep3", "keep4"),
          AssertOnQuery("input file archived") { _: StreamExecution =>
            // it renames input file for third batch, but not fourth batch yet
            assertFileIsMoved(dirForKeep3, expectedMovedDir3, "keep3")
            assertFileIsNotMoved(dirForKeep3, expectedMovedDir3, "keep4")

            true
          }
        )
      }
    }
  }

  Seq("delete", "archive").foreach { cleanOption =>
    test(s"Throw UnsupportedOperationException on configuring $cleanOption when source path" +
      " refers the output dir of FileStreamSink") {
      withThreeTempDirs { case (src, tmp, archiveDir) =>
        withSQLConf(
          SQLConf.FILE_SOURCE_LOG_COMPACT_INTERVAL.key -> "2",
          // Force deleting the old logs
          SQLConf.FILE_SOURCE_LOG_CLEANUP_DELAY.key -> "1",
          SQLConf.FILE_SOURCE_CLEANER_NUM_THREADS.key -> "0"
        ) {
          val option = Map("latestFirst" -> "false", "maxFilesPerTrigger" -> "1",
            "cleanSource" -> cleanOption, "sourceArchiveDir" -> archiveDir.getAbsolutePath)

          val fileStream = createFileStream("text", src.getCanonicalPath, options = option)
          val filtered = fileStream.filter($"value" contains "keep")

          // create FileStreamSinkLog under source directory
          val sinkLog = new FileStreamSinkLog(FileStreamSinkLog.VERSION, spark,
            new File(src, FileStreamSink.metadataDir).getCanonicalPath)
          val hadoopConf = SparkHadoopUtil.newConfiguration(sparkConf)
          val srcPath = new Path(src.getCanonicalPath)
          val fileSystem = srcPath.getFileSystem(hadoopConf)

          // Here we will just check whether the source file is removed or not, as we cover
          // functionality test of "archive" in other UT.
          testStream(filtered)(
            AddTextFileData("keep1", src, tmp, tmpFilePrefix = "keep1"),
            AddFilesToFileStreamSinkLog(fileSystem, srcPath, sinkLog, 0) { path =>
              path.getName.startsWith("keep1")
            },
            ExpectFailure[UnsupportedOperationException](
              t => assert(t.getMessage.startsWith("Clean up source files is not supported")),
              isFatalError = false)
          )
        }
      }
    }
  }

  class FakeFileSystem(scheme: String) extends FileSystem {
    override def exists(f: Path): Boolean = true

    override def mkdirs(f: Path, permission: FsPermission): Boolean = true

    override def rename(src: Path, dst: Path): Boolean = true

    override def getUri: URI = URI.create(s"${scheme}:///")

    override def open(f: Path, bufferSize: Int): FSDataInputStream = throw new NotImplementedError

    override def create(
        f: Path,
        permission: FsPermission,
        overwrite: Boolean,
        bufferSize: Int,
        replication: Short,
        blockSize: Long,
        progress: Progressable): FSDataOutputStream = throw new NotImplementedError

    override def append(f: Path, bufferSize: Int, progress: Progressable): FSDataOutputStream =
      throw new NotImplementedError

    override def delete(f: Path, recursive: Boolean): Boolean = throw new NotImplementedError

    override def listStatus(f: Path): Array[FileStatus] = throw new NotImplementedError

    override def setWorkingDirectory(new_dir: Path): Unit = throw new NotImplementedError

    override def getWorkingDirectory: Path = new Path("/somewhere")

    override def getFileStatus(f: Path): FileStatus = throw new NotImplementedError
  }

  test("SourceFileArchiver - fail when base archive path matches source pattern") {
    val fakeFileSystem = new FakeFileSystem("fake")

    def assertThrowIllegalArgumentException(sourcePattern: Path, baseArchivePath: Path): Unit = {
      intercept[IllegalArgumentException] {
        new SourceFileArchiver(fakeFileSystem, sourcePattern, fakeFileSystem, baseArchivePath)
      }
    }

    // 1) prefix of base archive path matches source pattern (baseArchiveDirPath has more depths)
    val sourcePatternPath = new Path("/hello*/spar?")
    val baseArchiveDirPath = new Path("/hello/spark/structured/streaming")
    assertThrowIllegalArgumentException(sourcePatternPath, baseArchiveDirPath)

    // 2) prefix of source pattern matches base archive path (source pattern has more depths)
    val sourcePatternPath2 = new Path("/hello*/spar?/structured/streaming")
    val baseArchiveDirPath2 = new Path("/hello/spark/structured")
    assertThrowIllegalArgumentException(sourcePatternPath2, baseArchiveDirPath2)

    // 3) source pattern matches base archive path (both have same depth)
    val sourcePatternPath3 = new Path("/hello*/spar?/structured/*")
    val baseArchiveDirPath3 = new Path("/hello/spark/structured/streaming")
    assertThrowIllegalArgumentException(sourcePatternPath3, baseArchiveDirPath3)
  }

  test("SourceFileArchiver - different filesystems between source and archive") {
    val fakeFileSystem = new FakeFileSystem("fake")
    val fakeFileSystem2 = new FakeFileSystem("fake2")

    val sourcePatternPath = new Path("/hello*/h{e,f}ll?")
    val baseArchiveDirPath = new Path("/hello")

    intercept[IllegalArgumentException] {
      new SourceFileArchiver(fakeFileSystem, sourcePatternPath, fakeFileSystem2,
        baseArchiveDirPath)
    }
  }

  private def assertFileIsRemoved(sourceDir: File, fileName: String): Unit = {
    assert(!sourceDir.list().exists(_.startsWith(fileName)))
  }

  private def assertFileIsNotRemoved(sourceDir: File, fileName: String): Unit = {
    assert(sourceDir.list().exists(_.startsWith(fileName)))
  }

  private def assertFileIsNotMoved(sourceDir: File, expectedDir: File, filePrefix: String): Unit = {
    assert(sourceDir.exists())
    assert(sourceDir.list().exists(_.startsWith(filePrefix)))
    if (!expectedDir.exists()) {
      // OK
    } else {
      assert(!expectedDir.list().exists(_.startsWith(filePrefix)))
    }
  }

  private def assertFileIsMoved(sourceDir: File, expectedDir: File, filePrefix: String): Unit = {
    assert(sourceDir.exists())
    assert(!sourceDir.list().exists(_.startsWith(filePrefix)))
    assert(expectedDir.exists())
    assert(expectedDir.list().exists(_.startsWith(filePrefix)))
  }

  private def withCountListingLocalFileSystemAsLocalFileSystem(body: => Unit): Unit = {
    val optionKey = s"fs.${CountListingLocalFileSystem.scheme}.impl"
    val originClassForLocalFileSystem = spark.conf.getOption(optionKey)
    try {
      spark.conf.set(optionKey, classOf[CountListingLocalFileSystem].getName)
      body
    } finally {
      originClassForLocalFileSystem match {
        case Some(fsClazz) => spark.conf.set(optionKey, fsClazz)
        case _ => spark.conf.unset(optionKey)
      }
    }
  }

  test("Caches and leverages unread files") {
    withCountListingLocalFileSystemAsLocalFileSystem {
      withThreeTempDirs { case (src, meta, tmp) =>
        val options = Map("latestFirst" -> "false", "maxFilesPerTrigger" -> "10")
        val scheme = CountListingLocalFileSystem.scheme
        val source = new FileStreamSource(spark, s"$scheme:///${src.getCanonicalPath}/*/*", "text",
          StructType(Nil), Seq.empty, meta.getCanonicalPath, options)
        val _metadataLog = PrivateMethod[FileStreamSourceLog](Symbol("metadataLog"))
        val metadataLog = source invokePrivate _metadataLog()

        def verifyBatch(
            offset: FileStreamSourceOffset,
            expectedBatchId: Long,
            inputFiles: Seq[File],
            expectedListingCount: Int): Unit = {
          val batchId = offset.logOffset
          assert(batchId === expectedBatchId)

          val files = metadataLog.get(batchId).getOrElse(Array.empty[FileEntry])
          assert(files.forall(_.batchId == batchId))

          val actualInputFiles = files.map { p => new Path(p.path).toUri.getPath }
          val expectedInputFiles = inputFiles.slice(batchId.toInt * 10, batchId.toInt * 10 + 10)
            .map(_.getCanonicalPath)
          assert(actualInputFiles === expectedInputFiles)

          assert(expectedListingCount === CountListingLocalFileSystem.pathToNumListStatusCalled
            .get(src.getCanonicalPath).map(_.get()).getOrElse(0))
        }

        CountListingLocalFileSystem.resetCount()

        // provide 41 files in src, with sequential "last modified" to guarantee ordering
        val inputFiles = (0 to 40).map { idx =>
          val f = createFile(idx.toString, new File(src, idx.toString), tmp)
          f.setLastModified(idx * 10000)
          f
        }

        // 4 batches will be available for 40 input files
        (0 to 3).foreach { batchId =>
          val offsetBatch = source.latestOffset(FileStreamSourceOffset(-1L), ReadLimit.maxFiles(10))
            .asInstanceOf[FileStreamSourceOffset]
          verifyBatch(offsetBatch, expectedBatchId = batchId, inputFiles, expectedListingCount = 1)
        }

        // batch 5 will trigger list operation though the batch 4 should have 1 unseen file:
        // 1 is smaller than the threshold (refer FileStreamSource.DISCARD_UNSEEN_FILES_RATIO),
        // hence unseen files for batch 4 will be discarded.
        val offsetBatch = source.latestOffset(FileStreamSourceOffset(-1L), ReadLimit.maxFiles(10))
          .asInstanceOf[FileStreamSourceOffset]
        assert(4 === offsetBatch.logOffset)
        assert(2 === CountListingLocalFileSystem.pathToNumListStatusCalled
          .get(src.getCanonicalPath).map(_.get()).getOrElse(0))

        val offsetBatch2 = source.latestOffset(FileStreamSourceOffset(-1L), ReadLimit.maxFiles(10))
          .asInstanceOf[FileStreamSourceOffset]
        // latestOffset returns the offset for previous batch which means no new batch is presented
        assert(4 === offsetBatch2.logOffset)
        // listing should be performed after the list of unread files are exhausted
        assert(3 === CountListingLocalFileSystem.pathToNumListStatusCalled
          .get(src.getCanonicalPath).map(_.get()).getOrElse(0))
      }
    }
  }

  test("Don't cache unread files when latestFirst is true") {
    withCountListingLocalFileSystemAsLocalFileSystem {
      withThreeTempDirs { case (src, meta, tmp) =>
        val options = Map("latestFirst" -> "true", "maxFilesPerTrigger" -> "5")
        val scheme = CountListingLocalFileSystem.scheme
        val source = new FileStreamSource(spark, s"$scheme:///${src.getCanonicalPath}/*/*", "text",
          StructType(Nil), Seq.empty, meta.getCanonicalPath, options)

        CountListingLocalFileSystem.resetCount()

        // provide 20 files in src, with sequential "last modified" to guarantee ordering
        (0 to 19).map { idx =>
          val f = createFile(idx.toString, new File(src, idx.toString), tmp)
          f.setLastModified(idx * 10000)
          f
        }

        source.latestOffset(FileStreamSourceOffset(-1L), ReadLimit.maxFiles(5))
          .asInstanceOf[FileStreamSourceOffset]
        assert(1 === CountListingLocalFileSystem.pathToNumListStatusCalled
          .get(src.getCanonicalPath).map(_.get()).getOrElse(0))

        // Even though the first batch doesn't read all available files, since latestFirst is true,
        // file stream source will not leverage unread files - next batch will also trigger
        // listing files
        source.latestOffset(FileStreamSourceOffset(-1L), ReadLimit.maxFiles(5))
          .asInstanceOf[FileStreamSourceOffset]
        assert(2 === CountListingLocalFileSystem.pathToNumListStatusCalled
          .get(src.getCanonicalPath).map(_.get()).getOrElse(0))
      }
    }
  }

  test("SPARK-31962: file stream source shouldn't allow modifiedBefore/modifiedAfter") {
    def formatTime(time: LocalDateTime): String = {
      time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"))
    }

    def assertOptionIsNotSupported(options: Map[String, String], path: String): Unit = {
      val schema = StructType(Seq(StructField("a", StringType)))
      var dsReader = spark.readStream
        .format("csv")
        .option("timeZone", "UTC")
        .schema(schema)

      options.foreach { case (k, v) => dsReader = dsReader.option(k, v) }

      val df = dsReader.load(path)

      testStream(df)(
        ExpectFailure[IllegalArgumentException](
          t => assert(t.getMessage.contains("is not allowed in file stream source")),
          isFatalError = false)
      )
    }

    withTempDir { dir =>
      // "modifiedBefore"
      val futureTime = LocalDateTime.now(ZoneOffset.UTC).plusYears(1)
      val formattedFutureTime = formatTime(futureTime)
      assertOptionIsNotSupported(Map("modifiedBefore" -> formattedFutureTime), dir.getCanonicalPath)

      // "modifiedAfter"
      val prevTime = LocalDateTime.now(ZoneOffset.UTC).minusYears(1)
      val formattedPrevTime = formatTime(prevTime)
      assertOptionIsNotSupported(Map("modifiedAfter" -> formattedPrevTime), dir.getCanonicalPath)

      // both
      assertOptionIsNotSupported(
        Map("modifiedBefore" -> formattedFutureTime, "modifiedAfter" -> formattedPrevTime),
        dir.getCanonicalPath)
    }
  }

  private def createFile(content: String, src: File, tmp: File): File = {
    val tempFile = Utils.tempFileWith(new File(tmp, "text"))
    val finalFile = new File(src, tempFile.getName)
    require(!src.exists(), s"$src exists, dir: ${src.isDirectory}, file: ${src.isFile}")
    require(src.mkdirs(), s"Cannot create $src")
    require(src.isDirectory(), s"$src is not a directory")
    require(stringToFile(tempFile, content).renameTo(finalFile))
    finalFile
  }

  test("SPARK-35320: Reading JSON with key type different to String in a map should fail") {
    Seq(
      MapType(IntegerType, StringType),
      StructType(Seq(StructField("test", MapType(IntegerType, StringType)))),
      ArrayType(MapType(IntegerType, StringType)),
      MapType(StringType, MapType(IntegerType, StringType))
    ).foreach { schema =>
      withTempDir { dir =>
        val colName = "col"
        val msg = "can only contain StringType as a key type for a MapType"

        val thrown1 = intercept[AnalysisException](
          spark.readStream.schema(StructType(Seq(StructField(colName, schema))))
            .json(dir.getCanonicalPath).schema)
        assert(thrown1.getMessage.contains(msg))
      }
    }
  }
}

class FileStreamSourceStressTestSuite extends FileStreamSourceTest {

  import testImplicits._

  testQuietly("file source stress test") {
    val src = Utils.createTempDir(namePrefix = "streaming.src")
    val tmp = Utils.createTempDir(namePrefix = "streaming.tmp")

    val fileStream = createFileStream("text", src.getCanonicalPath)
    val ds = fileStream.as[String].map(_.toInt + 1)
    runStressTest(ds, data => {
      AddTextFileData(data.mkString("\n"), src, tmp)
    })

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }
}

/**
 * Fake FileSystem to test whether the method `fs.exists` is called during
 * `DataSource.resolveRelation`.
 */
class ExistsThrowsExceptionFileSystem extends RawLocalFileSystem {
  import ExistsThrowsExceptionFileSystem._

  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def exists(f: Path): Boolean = {
    throw new IllegalArgumentException("Exists shouldn't have been called!")
  }

  /** Simply return an empty file for now. */
  override def listStatus(file: Path): Array[FileStatus] = {
    val emptyFile = new FileStatus()
    emptyFile.setPath(file)
    Array(emptyFile)
  }
}

object ExistsThrowsExceptionFileSystem {
  val scheme = s"FileStreamSourceSuite${math.abs(Random.nextInt)}fs"
}

class CountListingLocalFileSystem extends RawLocalFileSystem {
  import CountListingLocalFileSystem._

  override def getUri: URI = {
    URI.create(s"$scheme:///")
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    val curVal = pathToNumListStatusCalled.getOrElseUpdate(f.toUri.getPath, new AtomicLong(0))
    curVal.incrementAndGet()
    super.listStatus(f)
  }
}

object CountListingLocalFileSystem {
  val scheme = s"CountListingLocalFileSystem${math.abs(Random.nextInt)}fs"
  val pathToNumListStatusCalled = new mutable.HashMap[String, AtomicLong]

  def resetCount(): Unit = pathToNumListStatusCalled.clear()
}
