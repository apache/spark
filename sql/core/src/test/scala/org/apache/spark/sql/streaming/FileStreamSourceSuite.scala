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

import org.apache.spark.sql.StreamTest
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.datasources.ResolvedDataSource
import org.apache.spark.sql.execution.streaming.{FileStreamSource, Offset}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class FileStreamSourceSuite extends StreamTest with SharedSQLContext {

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

  test("read from text files") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val textSource = ResolvedDataSource.createSource(
      sqlContext,
      userSpecifiedSchema = None,
      providerName = "text",
      options = Map("path" -> src.getCanonicalPath)).asInstanceOf[FileStreamSource]
    val df = textSource.toDF().filter($"value" contains "keep")
    val filtered = df

    testStream(filtered)(
      AddTextFileData(textSource, "drop1\nkeep2\nkeep3", src, tmp),
      CheckAnswer("keep2", "keep3"),
      AddTextFileData(textSource, "drop4\nkeep5\nkeep6", src, tmp),
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

    val textSource = ResolvedDataSource.createSource(
      sqlContext,
      userSpecifiedSchema = None,
      providerName = "json",
      options = Map("path" -> src.getCanonicalPath)).asInstanceOf[FileStreamSource]
    val df = textSource.toDF().filter($"value" contains "keep")
    val filtered = df

    testStream(filtered)(
      AddTextFileData(textSource, "{'c': 'drop1'}\n{'c': 'keep2'}\n{'c': 'keep3'}", src, tmp),
      CheckAnswer("keep2", "keep3"),
      AddTextFileData(textSource, "{'c': 'drop4'}\n{'c': 'keep5'}\n{'c': 'keep6'}", src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6"),
      AddTextFileData(textSource, "{'c': 'drop7'}\n{'c': 'keep8'}\n{'c': 'keep9'}", src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
    )

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }

  test("read from parquet files") {
    val src = Utils.createTempDir("streaming.src")
    val tmp = Utils.createTempDir("streaming.tmp")

    val fileSource = ResolvedDataSource.createSource(
      sqlContext,
      userSpecifiedSchema = None,
      providerName = "parquet",
      options = Map("path" -> src.getCanonicalPath)).asInstanceOf[FileStreamSource]
    val df = fileSource.toDF().filter($"value" contains "keep")
    val filtered = df

    testStream(filtered)(
      AddParquetFileData(fileSource, Seq("drop1", "keep2", "keep3"), src, tmp),
      CheckAnswer("keep2", "keep3"),
      AddParquetFileData(fileSource, Seq("drop4", "keep5", "keep6"), src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6"),
      AddParquetFileData(fileSource, Seq("drop7", "keep8", "keep9"), src, tmp),
      CheckAnswer("keep2", "keep3", "keep5", "keep6", "keep8", "keep9")
    )

    Utils.deleteRecursively(src)
    Utils.deleteRecursively(tmp)
  }
}
