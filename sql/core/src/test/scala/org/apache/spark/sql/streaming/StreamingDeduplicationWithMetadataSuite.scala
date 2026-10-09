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
import java.nio.charset.StandardCharsets.UTF_8

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.util.stringToFile
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.{concat, lit, timestamp_seconds}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

class StreamingDeduplicationWithMetadataSuite extends StreamTest {

  import testImplicits._

  private val confKey = SQLConf.DROP_DUPLICATES_DETERMINISTIC_KEY_ORDER.key

  private def testWithKeyOrders(name: String)(f: => Unit): Unit = {
    Seq(false, true).foreach { orderDeterministically =>
      test(s"$name (orderDeterministically = $orderDeterministically)") {
        withSQLConf(confKey -> orderDeterministically.toString)(f)
      }
    }
  }

  testWithKeyOrders("metadata added downstream does not become a streaming deduplication key") {
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

  testWithKeyOrders("metadata visible before streaming deduplication remains a key") {
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

  testWithKeyOrders(
      "metadata added downstream does not become a within-watermark deduplication key") {
    withTempDir { src =>
      stringToFile(new File(src, "first"), "same")
      stringToFile(new File(src, "second"), "same")
      val input = spark.readStream.format("text").load(src.getCanonicalPath)
        .withColumn("eventTime", timestamp_seconds(lit(1)))
        .withWatermark("eventTime", "10 seconds")
      val metadataAfterDedup = input
        .dropDuplicatesWithinWatermark()
        .select($"value", $"_metadata.file_path".isNotNull.as("hasMetadata"))

      testStream(metadataAfterDedup)(
        StartStream(),
        ProcessAllAvailable(),
        CheckAnswer(("same", true)))
    }
  }

  testWithKeyOrders("metadata visible before within-watermark deduplication remains a key") {
    withTempDir { src =>
      stringToFile(new File(src, "first"), "same")
      stringToFile(new File(src, "second"), "same")
      val input = spark.readStream.format("text").load(src.getCanonicalPath)
        .withColumn("eventTime", timestamp_seconds(lit(1)))
        .withWatermark("eventTime", "10 seconds")
      val metadataBeforeDedup = input
        .select($"value", $"eventTime", $"_metadata")
        .dropDuplicatesWithinWatermark()
        .select($"value", $"_metadata.file_path".isNotNull.as("hasMetadata"))

      testStream(metadataBeforeDedup)(
        StartStream(),
        ProcessAllAvailable(),
        CheckAnswer(("same", true), ("same", true)))
    }
  }

  test("metadata propagation preserves key order from a Spark 4.2 checkpoint") {
    withTempDir { src =>
      withTempDir { checkpoint =>
        val resource = this.getClass.getResource(
          "/structured-streaming/" +
            "checkpoint-version-4.2.0-deduplication-metadata-boundary/").toURI
        Utils.copyDirectory(new File(resource), checkpoint)
        val firstFile = stringToFile(new File(src, "first"), "same")
        stringToFile(new File(src, "second"), "same")

        // File source logs contain absolute paths. Point the copied batch-0 entry at the equivalent
        // file in this test's temporary source directory while leaving the 4.2 state untouched.
        val sourceLog = new Path(checkpoint.getCanonicalPath + "/sources/0/0")
        val fileSystem = sourceLog.getFileSystem(spark.sessionState.newHadoopConf())
        val output = fileSystem.create(sourceLog, true)
        try {
          output.write(
            s"""v1
               |{"path":"${firstFile.toURI}","timestamp":0,"batchId":0}""".stripMargin
              .getBytes(UTF_8))
        } finally {
          output.close()
        }

        // Spark 4.2 wrote the checkpoint after processing another file containing "same" with
        // four ordinary columns as keys. Its offset log has no deterministic-key-order setting,
        // so restart must use the legacy key order even though the session default is true.
        val result = spark.readStream.format("text").load(src.getCanonicalPath)
          .select(
            concat($"value", lit("_1")).as("column_1"),
            concat($"value", lit("_2")).as("column_2"),
            concat($"value", lit("_3")).as("column_3"),
            concat($"value", lit("_4")).as("column_4"))
          .dropDuplicates()
          .select($"*", $"_metadata.file_path".as("filePath"))

        testStream(result)(
          StartStream(checkpointLocation = checkpoint.getCanonicalPath),
          ProcessAllAvailable(),
          CheckLastBatch(),
          StopStream)
      }
    }
  }

  testWithKeyOrders("metadata added downstream does not become a batch deduplication key") {
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

  testWithKeyOrders("metadata visible before batch deduplication remains a key") {
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
