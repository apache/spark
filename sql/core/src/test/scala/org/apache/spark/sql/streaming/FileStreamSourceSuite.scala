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

import java.io.{ByteArrayInputStream, File, FileNotFoundException, InputStream}

import com.google.common.base.Charsets.UTF_8

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.FileStreamSource._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.util.Utils

class FileStreamSourceTest extends StreamTest with SharedSQLContext {

  import testImplicits._

  case class AddTextFileData(source: FileStreamSource, content: String, src: File, tmp: File)
    extends AddData {

    override def addData(): Offset = {
      source.withBatchingLocked {
        val file = Utils.tempFileWith(new File(tmp, "text"))
        stringToFile(file, content).renameTo(new File(src, file.getName))
        source.currentOffset
      } + 1
    }
  }

  case class AddParquetFileData(
      source: FileStreamSource,
      content: Seq[String],
      src: File,
      tmp: File) extends AddData {

    override def addData(): Offset = {
      source.withBatchingLocked {
        val file = Utils.tempFileWith(new File(tmp, "parquet"))
        content.toDS().toDF().write.parquet(file.getCanonicalPath)
        file.renameTo(new File(src, file.getName))
        source.currentOffset
      } + 1
    }
  }

  /** Use `format` and `path` to create FileStreamSource via DataFrameReader */
  def createFileStreamSource(
      format: String,
      path: String,
      schema: Option[StructType] = None): FileStreamSource = {
    val reader =
      if (schema.isDefined) {
        sqlContext.read.format(format).schema(schema.get)
      } else {
        sqlContext.read.format(format)
      }
    reader.stream(path)
      .queryExecution.analyzed
      .collect { case StreamingRelation(s: FileStreamSource, _) => s }
      .head
  }

  val valueSchema = new StructType().add("value", StringType)
}

class FileStreamSourceSuite extends FileStreamSourceTest with SharedSQLContext {

  import testImplicits._

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
      .collect { case StreamingRelation(s: FileStreamSource, _) => s }
      .head
      .schema
  }

  test("FileStreamSource schema: no path") {
    val e = intercept[IllegalArgumentException] {
      createFileStreamSourceAndGetSchema(format = None, path = None, schema = None)
    }
    assert("'path' is not specified" === e.getMessage)
  }

  test("FileStreamSource schema: path doesn't exist") {
    intercept[FileNotFoundException] {
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
      val e = intercept[IllegalArgumentException] {
        createFileStreamSourceAndGetSchema(
          format = Some("parquet"), path = Some(new File(src, "1").getCanonicalPath), schema = None)
      }
      assert("No schema specified" === e.getMessage)
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
      val e = intercept[IllegalArgumentException] {
        createFileStreamSourceAndGetSchema(
          format = Some("json"), path = Some(src.getCanonicalPath), schema = None)
      }
      assert("No schema specified" === e.getMessage)
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
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    val filtered = textSource.toDF().filter($"value" contains "keep")

    testStream(filtered)(
      AddTextFileData(textSource, "drop1\nkeep2\nkeep3", src, tmp),
      CheckAnswer("keep2", "keep3"),
      StopStream,
      AddTextFileData(textSource, "drop4\nkeep5\nkeep6", src, tmp),
      StartStream,
      CheckAnswer("keep2", "keep3", "keep5", "keep6"),
      AddTextFileData(textSource, "drop7\nkeep8\nkeep9", src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
    )

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }

  test("read from json files") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val textSource = createFileStreamSource("json", src.getCanonicalPath, Some(valueSchema))
    val filtered = textSource.toDF().filter($"value" contains "keep")

    testStream(filtered)(
      AddTextFileData(
        textSource,
        "{'value': 'drop1'}\n{'value': 'keep2'}\n{'value': 'keep3'}",
        src,
        tmp),
      CheckAnswer("keep2", "keep3"),
      StopStream,
      AddTextFileData(
        textSource,
        "{'value': 'drop4'}\n{'value': 'keep5'}\n{'value': 'keep6'}",
        src,
        tmp),
      StartStream,
      CheckAnswer("keep2", "keep3", "keep5", "keep6"),
      AddTextFileData(
        textSource,
        "{'value': 'drop7'}\n{'value': 'keep8'}\n{'value': 'keep9'}",
        src,
        tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
    )

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }

  test("read from json files with inferring schema") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    // Add a file so that we can infer its schema
    stringToFile(new File(src, "existing"), "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}")

    val textSource = createFileStreamSource("json", src.getCanonicalPath)

    // FileStreamSource should infer the column "c"
    val filtered = textSource.toDF().filter($"c" contains "keep")

    testStream(filtered)(
      AddTextFileData(textSource, "{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6")
    )

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }

  test("read from parquet files") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val fileSource = createFileStreamSource("parquet", src.getCanonicalPath, Some(valueSchema))
    val filtered = fileSource.toDF().filter($"value" contains "keep")

    testStream(filtered)(
      AddParquetFileData(fileSource, Seq("drop1", "keep2", "keep3"), src, tmp),
      CheckAnswer("keep2", "keep3"),
      StopStream,
      AddParquetFileData(fileSource, Seq("drop4", "keep5", "keep6"), src, tmp),
      StartStream,
      CheckAnswer("keep2", "keep3", "keep5", "keep6"),
      AddParquetFileData(fileSource, Seq("drop7", "keep8", "keep9"), src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
    )

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }

  test("file stream source without schema") {
    val src = Utils.createTempDir("streaming.src")

    // Only "text" doesn't need a schema
    createFileStreamSource("text", src.getCanonicalPath)

    // Both "json" and "parquet" require a schema if no existing file to infer
    intercept[IllegalArgumentException] {
      createFileStreamSource("json", src.getCanonicalPath)
    }
    intercept[IllegalArgumentException] {
      createFileStreamSource("parquet", src.getCanonicalPath)
    }

    Utils.deleteRecursively(src)
  }

  test("fault tolerance") {
    def assertBatch(batch1: Option[Batch], batch2: Option[Batch]): Unit = {
      (batch1, batch2) match {
        case (Some(b1), Some(b2)) =>
          assert(b1.end === b2.end)
          assert(b1.data.as[String].collect() === b2.data.as[String].collect())
        case (None, None) =>
        case _ => fail(s"batch ($batch1) is not equal to batch ($batch2)")
      }
    }

    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    val filtered = textSource.toDF().filter($"value" contains "keep")

    testStream(filtered)(
      AddTextFileData(textSource, "drop1\nkeep2\nkeep3", src, tmp),
      CheckAnswer("keep2", "keep3"),
      StopStream,
      AddTextFileData(textSource, "drop4\nkeep5\nkeep6", src, tmp),
      StartStream,
      CheckAnswer("keep2", "keep3", "keep5", "keep6"),
      AddTextFileData(textSource, "drop7\nkeep8\nkeep9", src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
    )

    val textSource2 = createFileStreamSource("text", src.getCanonicalPath)
    assert(textSource2.currentOffset === textSource.currentOffset)
    assertBatch(textSource2.getNextBatch(None), textSource.getNextBatch(None))
    for (f <- 0L to textSource.currentOffset.offset) {
      val offset = LongOffset(f)
      assertBatch(textSource2.getNextBatch(Some(offset)), textSource.getNextBatch(Some(offset)))
    }

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }

  test("fault tolerance with corrupted metadata file") {
    val src = Utils.createTempDir("streaming.src")
    assert(new File(src, "_metadata").mkdirs())
    stringToFile(
      new File(src, "_metadata/0"),
      s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\n-/e/f/g\nEND\n")
    stringToFile(new File(src, "_metadata/1"), s"${FileStreamSource.VERSION}\nSTART\n-")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    // the metadata file of batch is corrupted, so currentOffset should be 0
    assert(textSource.currentOffset === LongOffset(0))

    Utils.deleteRecursively(src)
  }

  test("fault tolerance with normal metadata file") {
    val src = Utils.createTempDir("streaming.src")
    assert(new File(src, "_metadata").mkdirs())
    stringToFile(
      new File(src, "_metadata/0"),
      s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\n-/e/f/g\nEND\n")
    stringToFile(
      new File(src, "_metadata/1"),
      s"${FileStreamSource.VERSION}\nSTART\n-/x/y/z\nEND\n")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    assert(textSource.currentOffset === LongOffset(1))

    Utils.deleteRecursively(src)
  }

  test("readBatch") {
    def stringToStream(str: String): InputStream = new ByteArrayInputStream(str.getBytes(UTF_8))

    // Invalid metadata
    assert(readBatch(stringToStream("")) === Nil)
    assert(readBatch(stringToStream(FileStreamSource.VERSION)) === Nil)
    assert(readBatch(stringToStream(s"${FileStreamSource.VERSION}\n")) === Nil)
    assert(readBatch(stringToStream(s"${FileStreamSource.VERSION}\nSTART")) === Nil)
    assert(readBatch(stringToStream(s"${FileStreamSource.VERSION}\nSTART\n-")) === Nil)
    assert(readBatch(stringToStream(s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c")) === Nil)
    assert(readBatch(stringToStream(s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\n")) === Nil)
    assert(readBatch(stringToStream(s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\nEN")) === Nil)

    // Valid metadata
    assert(readBatch(stringToStream(
      s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\nEND")) === Seq("/a/b/c"))
    assert(readBatch(stringToStream(
      s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\nEND\n")) === Seq("/a/b/c"))
    assert(readBatch(stringToStream(
      s"${FileStreamSource.VERSION}\nSTART\n-/a/b/c\n-/e/f/g\nEND\n"))
      === Seq("/a/b/c", "/e/f/g"))
  }
}

class FileStreamSourceStressTestSuite extends FileStreamSourceTest with SharedSQLContext {

  import testImplicits._

  test("file source stress test") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    val ds = textSource.toDS[String]().map(_.toInt + 1)
    runStressTest(ds, data => {
      AddTextFileData(textSource, data.mkString("\n"), src, tmp)
    })

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }
}
