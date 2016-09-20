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

import org.scalatest.concurrent.Eventually._
import org.scalatest.time.SpanSugar._

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class FileStreamSourceTest extends StreamTest with SharedSQLContext {

  import testImplicits._

  /**
   * A subclass [[AddData]] for adding data to files. This is meant to use the
   * [[FileStreamSource]] actually being used in the execution.
   */
  abstract class AddFileData extends AddData {
    override def addData(query: Option[StreamExecution]): (Source, Offset) = {
      require(
        query.nonEmpty,
        "Cannot add data when there is no query for finding the active file stream source")

      val sources = query.get.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[FileStreamSource] =>
          source.asInstanceOf[FileStreamSource]
      }
      if (sources.isEmpty) {
        throw new Exception(
          "Could not find file source in the StreamExecution logical plan to add data to")
      } else if (sources.size > 1) {
        throw new Exception(
          "Could not select the file source in the StreamExecution logical plan as there" +
            "are multiple file sources:\n\t" + sources.mkString("\n\t"))
      }
      val source = sources.head
      val newOffset = source.withBatchingLocked {
        addData(source)
        source.currentOffset + 1
      }
      logInfo(s"Added file to $source at offset $newOffset")
      (source, newOffset)
    }

    protected def addData(source: FileStreamSource): Unit
  }

  case class AddTextFileData(content: String, src: File, tmp: File)
    extends AddFileData {

    override def addData(source: FileStreamSource): Unit = {
      val tempFile = Utils.tempFileWith(new File(tmp, "text"))
      val finalFile = new File(src, tempFile.getName)
      src.mkdirs()
      require(stringToFile(tempFile, content).renameTo(finalFile))
      logInfo(s"Written text '$content' to file $finalFile")
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

  protected def withTempDirs(body: (File, File) => Unit) {
    val src = Utils.createTempDir(namePrefix = "streaming.src")
    val tmp = Utils.createTempDir(namePrefix = "streaming.tmp")
    try {
      body(src, tmp)
    } finally {
      Utils.deleteRecursively(src)
      Utils.deleteRecursively(tmp)
    }
  }

  val valueSchema = new StructType().add("value", StringType)
}

class FileStreamSourceSuite extends FileStreamSourceTest {

  import testImplicits._

  override val streamingTimeout = 20.seconds

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
        assert("Unable to infer schema. It must be specified manually.;" === e.getMessage)
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
          val source = streamExecution.logicalPlan.collect { case e: StreamingExecutionRelation =>
            e.source.asInstanceOf[FileStreamSource]
          }.head
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

        // Append to same partition=1 sub dir
        AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo")),

        // Create new partition sub dir and write to it
        AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar")),

        // Append to same partition=2 sub dir
        AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
        CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar"))
      )
    }
  }

  test("when schema inference is turned on, should read partition data") {
    def createFile(content: String, src: File, tmp: File): Unit = {
      val tempFile = Utils.tempFileWith(new File(tmp, "text"))
      val finalFile = new File(src, tempFile.getName)
      src.mkdirs()
      require(stringToFile(tempFile, content).renameTo(finalFile))
    }

    withSQLConf(SQLConf.STREAMING_SCHEMA_INFERENCE.key -> "true") {
      withTempDirs { case (dir, tmp) =>
        val partitionFooSubDir = new File(dir, "partition=foo")
        val partitionBarSubDir = new File(dir, "partition=bar")

        // Create files in partitions, so we can infer the schema.
        createFile("{'value': 'drop0'}", partitionFooSubDir, tmp)
        createFile("{'value': 'drop0'}", partitionBarSubDir, tmp)

        val fileStream = createFileStream("json", s"${dir.getCanonicalPath}")
        val filtered = fileStream.filter($"value" contains "keep")
        testStream(filtered)(
          // Create new partition=foo sub dir and write to it
          AddTextFileData("{'value': 'drop1'}\n{'value': 'keep2'}", partitionFooSubDir, tmp),
          CheckAnswer(("keep2", "foo")),

          // Append to same partition=1 sub dir
          AddTextFileData("{'value': 'keep3'}", partitionFooSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo")),

          // Create new partition sub dir and write to it
          AddTextFileData("{'value': 'keep4'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar")),

          // Append to same partition=2 sub dir
          AddTextFileData("{'value': 'keep5'}", partitionBarSubDir, tmp),
          CheckAnswer(("keep2", "foo"), ("keep3", "foo"), ("keep4", "bar"), ("keep5", "bar"))
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
        .asInstanceOf[StreamExecution]
      q.processAllAvailable()
      val memorySink = q.sink.asInstanceOf[MemorySink]
      val fileSource = q.logicalPlan.collect {
        case StreamingExecutionRelation(source, _) if source.isInstanceOf[FileStreamSource] =>
          source.asInstanceOf[FileStreamSource]
      }.head

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

  test("max files per trigger - incorrect values") {
    withTempDir { case src =>
      def testMaxFilePerTriggerValue(value: String): Unit = {
        val df = spark.readStream.option("maxFilesPerTrigger", value).text(src.getCanonicalPath)
        val e = intercept[IllegalArgumentException] {
          testStream(df)()
        }
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

  test("explain") {
    withTempDirs { case (src, tmp) =>
      src.mkdirs()

      val df = spark.readStream.format("text").load(src.getCanonicalPath).map(_ + "-x")
      // Test `explain` not throwing errors
      df.explain()

      val q = df.writeStream.queryName("file_explain").format("memory").start()
        .asInstanceOf[StreamExecution]
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
        assert("TextFileFormat".r.findAllMatchIn(explainWithoutExtended).size === 1)

        val explainWithExtended = q.explainInternal(true)
        // `extended = true` displays 3 logical plans (Parsed/Optimized/Optimized) and 1 physical
        // plan.
        assert("Relation.*text".r.findAllMatchIn(explainWithExtended).size === 3)
        assert("TextFileFormat".r.findAllMatchIn(explainWithExtended).size === 1)
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
    // by the ListingFileCatalog.
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

      testStream(df, InternalOutputModes.Complete)(
        AddTextFileData("0", src, tmp),
        CheckAnswer(0)
      )
    }
  }
}

class FileStreamSourceStressTestSuite extends FileStreamSourceTest {

  import testImplicits._

  test("file source stress test") {
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
