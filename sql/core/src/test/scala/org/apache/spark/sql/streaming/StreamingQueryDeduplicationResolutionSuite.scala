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

import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream

class StreamingQueryDeduplicationResolutionSuite extends StreamTest {

  import testImplicits._

  test("metadata added downstream does not become a streaming deduplication key") {
    withTempDir { src =>
      stringToFile(new File(src, "first"), "same")
      stringToFile(new File(src, "second"), "same")
      val input = spark.readStream.format("text").load(src.getCanonicalPath)
      val metadataAfterDedup = input
        .dropDuplicates()
        .select($"value", $"_metadata.file_path".isNotNull.as("hasMetadata"))

      testStream(metadataAfterDedup)(
        StartStream(),
        ProcessAllAvailable(),
        CheckAnswer(("same", true)))
    }
  }

  test("metadata visible before streaming deduplication remains a key") {
    withTempDir { src =>
      stringToFile(new File(src, "first"), "same")
      stringToFile(new File(src, "second"), "same")
      val input = spark.readStream.format("text").load(src.getCanonicalPath)
      val metadataBeforeDedup = input
        .select($"value", $"_metadata")
        .dropDuplicates()
        .select($"value", $"_metadata.file_path".isNotNull.as("hasMetadata"))

      testStream(metadataBeforeDedup)(
        StartStream(),
        ProcessAllAvailable(),
        CheckAnswer(("same", true), ("same", true)))
    }
  }

  test("metadata added downstream does not become a batch deduplication key") {
    withTempDir { src =>
      stringToFile(new File(src, "first"), "same")
      stringToFile(new File(src, "second"), "same")
      val input = spark.read.format("text").load(src.getCanonicalPath)
      val batchMetadataAfterDedup = input
        .dropDuplicates()
        .select($"value".as("batchValue"), $"_metadata.file_path".isNotNull.as("hasMetadata"))
      val stream = MemoryStream[String]
      val streamingInput = stream.toDF().toDF("streamValue")
      val result = streamingInput.join(
        batchMetadataAfterDedup,
        $"streamValue" === $"batchValue")
        .select($"batchValue", $"hasMetadata")

      testStream(result)(
        AddData(stream, "same"),
        CheckAnswer(("same", true)))
    }
  }

  test("metadata visible before batch deduplication remains a key") {
    withTempDir { src =>
      stringToFile(new File(src, "first"), "same")
      stringToFile(new File(src, "second"), "same")
      val input = spark.read.format("text").load(src.getCanonicalPath)
      val batchMetadataBeforeDedup = input
        .select($"value", $"_metadata")
        .dropDuplicates()
        .select($"value".as("batchValue"), $"_metadata.file_path".isNotNull.as("hasMetadata"))
      val stream = MemoryStream[String]
      val streamingInput = stream.toDF().toDF("streamValue")
      val result = streamingInput.join(
        batchMetadataBeforeDedup,
        $"streamValue" === $"batchValue")
        .select($"batchValue", $"hasMetadata")

      testStream(result)(
        AddData(stream, "same"),
        CheckAnswer(("same", true), ("same", true)))
    }
  }
}
