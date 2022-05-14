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

package org.apache.spark.input

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.mockito.Mockito.{mock, when}

import org.apache.spark.{SparkFunSuite, SparkIOException}

class FixedLengthBinaryRecordReaderSuite extends SparkFunSuite {

  test("UNSUPPORTED_READ_COMPRESSED_FILE: " +
    "FixedLengthRecordReader does not support reading compressed files") {
    withTempDir { dir =>
      val fileName = "file1.gz"
      val inputSplit = generateFakeFileSplit(dir, fileName)

      val taskAttemptContext = mock(classOf[TaskAttemptContext])
      val configuration = new Configuration()
      configuration.set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec")
      when(taskAttemptContext.getConfiguration).thenReturn(configuration)

      val fixedLengthBinaryRecordReader = new FixedLengthBinaryRecordReader()
      val e = intercept[SparkIOException](
        fixedLengthBinaryRecordReader.initialize(inputSplit, taskAttemptContext)
      )
      assert(e.getErrorClass === "UNSUPPORTED_READ_COMPRESSED_FILE")
      assert(e.getMessage === "[UNSUPPORTED_READ_COMPRESSED_FILE] " +
        "FixedLengthRecordReader does not support reading compressed files.")
    }
  }

  private def generateFakeFileSplit(dir: File, fileName: String): FileSplit = {
    val path = s"${dir.getCanonicalPath}/$fileName"
    new FileSplit(new Path(path), 0, 1,
      Array[String]("loc1", "loc2", "loc3", "loc4", "loc5"))
  }
}
