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
package org.apache.spark.sql.sqs

import java.util.Locale

import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryException, StreamTest}
import org.apache.spark.sql.types.StructType

class SqsSourceOptionsSuite extends StreamTest {

  test("bad source options") {
    def testBadOptions(option: (String, String))(expectedMsg: String): Unit = {

      var query : StreamingQuery = null

      try {
        val errorMessage = intercept[StreamingQueryException] {
          val dummySchema = new StructType
          val reader = spark
            .readStream
            .format("s3-sqs")
            .option("fileFormat", "json")
            .schema(dummySchema)
            .option("sqsUrl", "https://DUMMY_URL")
            .option("region", "us-east-1")
            .option(option._1, option._2)
            .load()

          query = reader.writeStream
            .format("memory")
            .queryName("badOptionsTest")
            .start()

          query.processAllAvailable()
        }.getMessage
        assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
      } finally {
        if (query != null) {
          // terminating streaming query if necessary
          query.stop()
        }

      }
    }

    testBadOptions("sqsFetchIntervalSeconds" -> "-2")("Invalid value '-2' " +
      "for option 'sqsFetchIntervalSeconds', must be a positive integer")
    testBadOptions("sqsLongPollingWaitTimeSeconds" -> "-5")("Invalid value '-5' " +
      "for option 'sqsLongPollingWaitTimeSeconds',must be an integer between 0 and 20")
    testBadOptions("sqsMaxConnections" -> "-2")("Invalid value '-2' " +
      "for option 'sqsMaxConnections', must be a positive integer")
    testBadOptions("maxFilesPerTrigger" -> "-50")("Invalid value '-50' " +
      "for option 'maxFilesPerTrigger', must be a positive integer")
    testBadOptions("ignoreFileDeletion" -> "x")("Invalid value 'x' " +
      "for option 'ignoreFileDeletion', must be true or false")
    testBadOptions("fileNameOnly" -> "x")("Invalid value 'x' " +
      "for option 'fileNameOnly', must be true or false")
    testBadOptions("shouldSortFiles" -> "x")("Invalid value 'x' " +
      "for option 'shouldSortFiles', must be true or false")
    testBadOptions("useInstanceProfileCredentials" -> "x")("Invalid value 'x' " +
      "for option 'useInstanceProfileCredentials', must be true or false")

  }

  test("missing mandatory options") {

    def testMissingMandatoryOptions(options: List[(String, String)])(expectedMsg: String): Unit = {

      var query: StreamingQuery = null

      try {
        val errorMessage = intercept[StreamingQueryException] {
          val dummySchema = new StructType
          val reader = spark
            .readStream
            .format("s3-sqs")
            .schema(dummySchema)

          val readerWithOptions = options.map { option =>
           reader.option(option._1, option._2)
          }.last.load()

          query = readerWithOptions.writeStream
            .format("memory")
            .queryName("missingMandatoryOptions")
            .start()

          query.processAllAvailable()
        }.getMessage
        assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
      } finally {
        if (query != null) {
          // terminating streaming query if necessary
          query.stop()
        }
      }
    }

    // No fileFormat specified
    testMissingMandatoryOptions(List("sqsUrl" -> "https://DUMMY_URL", "region"->"us-east-1"))(
      "Specifying file format is mandatory with sqs source")

    // Sqs URL not specified
    testMissingMandatoryOptions(List("fileFormat" -> "json", "region"->"us-east-1"))(
      "SQS Url is not specified")
  }

  test("schema not specified") {

    var query: StreamingQuery = null

    val expectedMsg = "Sqs source doesn't support empty schema"

    try {
      val errorMessage = intercept[IllegalArgumentException] {
        val reader = spark
          .readStream
          .format("s3-sqs")
          .option("sqsUrl", "https://DUMMY_URL")
          .option("fileFormat", "json")
          .option("region", "us-east-1")
          .load()

        query = reader.writeStream
          .format("memory")
          .queryName("missingSchema")
          .start()

        query.processAllAvailable()
      }.getMessage
      assert(errorMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
    } finally {
      if (query != null) {
        // terminating streaming query if necessary
        query.stop()
      }
    }

  }

}

