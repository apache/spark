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

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.{AnalysisException, DataFrame, StreamTest}
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
      logInfo(s"Added data to $source at offset $newOffset")
      (source, newOffset)
    }

    protected def addData(source: FileStreamSource): Unit
  }

  case class AddTextFileData(content: String, src: File, tmp: File)
    extends AddFileData {

    override def addData(source: FileStreamSource): Unit = {
      val file = Utils.tempFileWith(new File(tmp, "text"))
      stringToFile(file, content).renameTo(new File(src, file.getName))
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

    def writeToFile(df: DataFrame, src: File, tmp: File): Unit = {
      val file = Utils.tempFileWith(new File(tmp, "parquet"))
      df.write.parquet(file.getCanonicalPath)
      file.renameTo(new File(src, file.getName))
    }
  }

  def createFileStream(
      format: String,
      path: String,
      schema: Option[StructType] = None): DataFrame = {

    val reader =
      if (schema.isDefined) {
        sqlContext.read.format(format).schema(schema.get)
      } else {
        sqlContext.read.format(format)
      }
    reader.stream(path)
  }

  protected def getSourceFromFileStream(df: DataFrame): FileStreamSource = {
    val checkpointLocation = Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
    Utils.createTempDir(namePrefix = "streaming.metadata").getCanonicalPath
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

class FileStreamSourceSuite extends FileStreamSourceTest with SharedSQLContext {

  import testImplicits._

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
    val reader = sqlContext.read
    format.foreach(reader.format)
    schema.foreach(reader.schema)
    val df =
      if (path.isDefined) {
        reader.stream(path.get)
      } else {
        reader.stream()
      }
    df.queryExecution.analyzed
      .collect { case StreamingRelation(dataSource, _, _) =>
        dataSource.sourceSchema()
      }.head._2
  }

  test("FileStreamSource schema: no path") {
    val e = intercept[IllegalArgumentException] {
      createFileStreamSourceAndGetSchema(format = None, path = None, schema = None)
    }
    assert("'path' is not specified" === e.getMessage)
  }

  test("FileStreamSource schema: path doesn't exist") {
    intercept[AnalysisException] {
      createFileStreamSourceAndGetSchema(format = None, path = Some("/a/b/c"), schema = None)
    }
  }

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

  test("FileStreamSource schema: parquet, no existing files, no schema") {
    withTempDir { src =>
      val e = intercept[AnalysisException] {
        createFileStreamSourceAndGetSchema(
          format = Some("parquet"), path = Some(new File(src, "1").getCanonicalPath), schema = None)
      }
      assert("Unable to infer schema.  It must be specified manually.;" === e.getMessage)
    }
  }

  test("FileStreamSource schema: parquet, existing files, no schema") {
    withTempDir { src =>
      Seq("a", "b", "c").toDS().as("userColumn").toDF()
        .write.parquet(new File(src, "1").getCanonicalPath)
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("parquet"), path = Some(src.getCanonicalPath), schema = None)
      assert(schema === new StructType().add("value", StringType))
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

  test("FileStreamSource schema: json, no existing files, no schema") {
    withTempDir { src =>
      val e = intercept[AnalysisException] {
        createFileStreamSourceAndGetSchema(
          format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
      }
      assert("Unable to infer schema.  It must be specified manually.;" === e.getMessage)
    }
  }

  test("FileStreamSource schema: json, existing files, no schema") {
    withTempDir { src =>
      stringToFile(new File(src, "1"), "{'c': '1'}\n{'c': '2'}\n{'c': '3'}")
      val schema = createFileStreamSourceAndGetSchema(
        format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
      assert(schema === new StructType().add("c", StringType))
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

  test("read from text files") {
    withTempDirs { case (src, tmp) =>
      val textStream = createFileStream("text", src.getCanonicalPath)
      val filtered = textStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddTextFileData("drop1\nkeep2\nkeep3", src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddTextFileData("drop4\nkeep5\nkeep6", src, tmp),
        StartStream,
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

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
        StartStream,
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

      // Add a file so that we can infer its schema
      stringToFile(new File(src, "existing"), "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}")

      val fileStream = createFileStream("json", src.getCanonicalPath)
      require(fileStream.schema === StructType(Seq(StructField("c", StringType))))

      // FileStreamSource should infer the column "c"
      val filtered = fileStream.filter($"c" contains "keep")

      testStream(filtered)(
        AddTextFileData("{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6")
      )
    }
  }


  test("reading from json files inside partitioned directory") {
    withTempDirs { case (baseSrc, tmp) =>
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

  test("read from parquet files") {
    withTempDirs { case (src, tmp) =>
      val fileStream = createFileStream("parquet", src.getCanonicalPath, Some(valueSchema))
      val filtered = fileStream.filter($"value" contains "keep")

      testStream(filtered)(
        AddParquetFileData(Seq("drop1", "keep2", "keep3"), src, tmp),
        CheckAnswer("keep2", "keep3"),
        StopStream,
        AddParquetFileData(Seq("drop4", "keep5", "keep6"), src, tmp),
        StartStream,
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddParquetFileData(Seq("drop7", "keep8", "keep9"), src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }

  test("file stream source without schema") {
    withTempDir { src =>
      // Only "text" doesn't need a schema
      createFileStream("text", src.getCanonicalPath)

      // Both "json" and "parquet" require a schema if no existing file to infer
      intercept[AnalysisException] {
        createFileStream("json", src.getCanonicalPath)
      }
      intercept[AnalysisException] {
        createFileStream("parquet", src.getCanonicalPath)
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
        StartStream,
        CheckAnswer("keep2", "keep3", "keep5", "keep6"),
        AddTextFileData("drop7\nkeep8\nkeep9", src, tmp),
        CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
      )
    }
  }
}

class FileStreamSourceStressTestSuite extends FileStreamSourceTest with SharedSQLContext {

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
