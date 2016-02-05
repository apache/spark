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

import java.io.{ByteArrayInputStream, File, InputStream}

import com.google.common.base.Charsets.UTF_8

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.FileStreamSource._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class FileStreamSourceTest extends StreamTest with SharedSQLContext {

  import testImplicits._

  case class AddTextFileData(source: FileStreamSource, content: String, src: File, tmp: File)
    extends AddData {

    override def addData(): Offset = {
      val file = Utils.tempFileWith(new File(tmp, "text"))
      stringToFile(file, content).renameTo(new File(src, file.getName))
      source.currentOffset + 1
    }
  }

  case class AddParquetFileData(
    source: FileStreamSource,
    content: Seq[String],
    src: File,
    tmp: File) extends AddData {

    override def addData(): Offset = {
      val file = Utils.tempFileWith(new File(tmp, "parquet"))
      content.toDS().toDF().write.parquet(file.getCanonicalPath)
      file.renameTo(new File(src, file.getName))
      source.currentOffset + 1
    }
  }

  /** Use `format` and `path` to create FileStreamSource via DataFrameReader */
  def createFileStreamSource(format: String, path: String): FileStreamSource = {
    sqlContext.read
      .format(format)
      .stream(path)
      .queryExecution.analyzed
      .collect { case StreamingRelation(s: FileStreamSource, _) => s }
      .head
  }

}

class FileStreamSourceSuite extends FileStreamSourceTest with SharedSQLContext {

  import testImplicits._

  test("read from text files") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    val df = textSource.toDF().filter($"value" contains "keep")
    val filtered = df

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

    val textSource = createFileStreamSource("json", src.getCanonicalPath)
    val df = textSource.toDF().filter($"value" contains "keep")
    val filtered = df

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
    val df = textSource.toDF().filter($"c" contains "keep")
    val filtered = df

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

    val fileSource = createFileStreamSource("parquet", src.getCanonicalPath)
    val df = fileSource.toDF().filter($"value" contains "keep")
    val filtered = df

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
    val df = textSource.toDF().filter($"value" contains "keep")
    val filtered = df

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
      s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\n-/e/f/g\nEND\n")
    stringToFile(
      new File(src, "_metadata/1"),
      s"${sqlContext.sparkContext.version}\nSTART\n-")

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
      s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\n-/e/f/g\nEND\n")
    stringToFile(
      new File(src, "_metadata/1"),
      s"${sqlContext.sparkContext.version}\nSTART\n-/x/y/z\nEND\n")

    val textSource = createFileStreamSource("text", src.getCanonicalPath)
    assert(textSource.currentOffset === LongOffset(1))

    Utils.deleteRecursively(src)
  }

  test("readBatch") {
    def stringToStream(str: String): InputStream = new ByteArrayInputStream(str.getBytes(UTF_8))

    // Invalid metadata
    assert(readBatch(stringToStream("")) === Nil)
    assert(readBatch(stringToStream(sqlContext.sparkContext.version)) === Nil)
    assert(readBatch(stringToStream(s"${sqlContext.sparkContext.version}\n")) === Nil)
    assert(readBatch(stringToStream(s"${sqlContext.sparkContext.version}\nSTART")) === Nil)
    assert(readBatch(stringToStream(s"${sqlContext.sparkContext.version}\nSTART\n-")) === Nil)
    assert(readBatch(stringToStream(s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c")) === Nil)
    assert(
      readBatch(stringToStream(s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\n")) === Nil)
    assert(
      readBatch(stringToStream(s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\nEN")) === Nil)

    // Valid metadata
    assert(readBatch(stringToStream(
      s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\nEND")) === Seq("/a/b/c"))
    assert(readBatch(stringToStream(
      s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\nEND\n")) === Seq("/a/b/c"))
    assert(readBatch(stringToStream(
      s"${sqlContext.sparkContext.version}\nSTART\n-/a/b/c\n-/e/f/g\nEND\n"))
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
