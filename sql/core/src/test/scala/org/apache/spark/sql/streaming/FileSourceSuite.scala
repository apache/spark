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

import java.io.{BufferedWriter, OutputStreamWriter, File}

import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.test.SharedSQLContext

class FileSourceSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  case class AddFileData(fileSource: FileStream, blocking: Boolean, data: String*) extends AddData {
    override def source: Source = fileSource

    override def addData(): Offset = {
      val startOffset = if (blocking) {
        fileSource.offset
      } else {
        LongOffset.empty
      }

      val path = new Path(fileSource.path + "/" + System.currentTimeMillis())
      val fs = FileSystem.get(sparkContext.hadoopConfiguration)
      val writer = new BufferedWriter(new OutputStreamWriter(fs.create(path, true)))
      data.foreach { line =>
        writer.write(line)
        writer.write("\n")
      }
      writer.close()

      if (blocking) {
        failAfter(streamingTimout) {
          while (fileSource.offset == startOffset) {
            Thread.sleep(10)
          }
        }
        fileSource.offset
      } else {
        startOffset
      }
    }
  }

  def newTempFile(): FileStream = {
    val metadata = File.createTempFile("streaming", "metadata")
    metadata.delete()
    metadata.mkdir()
    val data = File.createTempFile("streaming", "data")
    data.delete()
    data.mkdir()
    new FileStream(sqlContext, metadata.toURI.toString, data.toURI.toString)
  }

  test("stream from files") {
    val tempDir = newTempFile()
    val asStrings = tempDir.toDF().as[String]

    testStream(asStrings)(
      AddFileData(tempDir, true, "1", "2", "3"),
      CheckAnswer("1", "2", "3"),
      StopStream,
      AddFileData(tempDir, false, "4", "5"),
      AddFileData(tempDir, false, "6", "7"),
      AddFileData(tempDir, true, "8", "9"),
      StartStream,
      CheckAnswer("1", "2", "3", "4", "5", "6", "7", "8", "9"))
  }
}