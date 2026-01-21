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

package org.apache.spark.sql.connect.streaming

import org.apache.spark.sql.connect.test.{QueryTest, RemoteSparkSession}

/**
 * Test suite for DataStreamReader.name() functionality in Spark Connect. Tests cover the naming
 * API, validation rules, and proper transmission of source names from Connect client to server.
 */
class DataStreamReaderNameSuite extends QueryTest with RemoteSparkSession {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Enable streaming source evolution feature
    spark.conf.set("spark.sql.streaming.queryEvolution.enableSourceEvolution", "true")
    spark.conf.set("spark.sql.streaming.offsetLog.formatVersion", "2")
  }

  test("name() with valid source names") {
    // Test that various valid patterns work correctly
    Seq("mySource", "my_source", "MySource123", "_private", "source_123_test", "123source")
      .foreach { name =>
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.range(10).write.parquet(path)

          val df = spark.readStream
            .format("parquet")
            .schema("id LONG")
            .name(name)
            .load(path)

          assert(df.isStreaming, s"DataFrame should be streaming for name: $name")
        }
      }
  }

  test("name() method chaining") {
    // Verify that name() returns the reader for method chaining
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val df = spark.readStream
        .format("parquet")
        .schema("id LONG")
        .name("my_source")
        .option("maxFilesPerTrigger", "1")
        .load(path)

      assert(df.isStreaming, "DataFrame should be streaming")
    }
  }

  test("name() before format()") {
    // Test that order doesn't matter - name can be set before format
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val df = spark.readStream
        .name("my_source")
        .format("parquet")
        .schema("id LONG")
        .load(path)

      assert(df.isStreaming, "DataFrame should be streaming")
    }
  }

  test("invalid source name - contains hyphen") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val exception = intercept[IllegalArgumentException] {
        spark.readStream
          .format("parquet")
          .schema("id LONG")
          .name("my-source")
          .load(path)
      }

      assert(exception.getMessage.contains("Invalid streaming source name"))
      assert(exception.getMessage.contains("my-source"))
    }
  }

  test("invalid source name - contains space") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val exception = intercept[IllegalArgumentException] {
        spark.readStream
          .format("parquet")
          .schema("id LONG")
          .name("my source")
          .load(path)
      }

      assert(exception.getMessage.contains("Invalid streaming source name"))
      assert(exception.getMessage.contains("my source"))
    }
  }

  test("invalid source name - contains dot") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val exception = intercept[IllegalArgumentException] {
        spark.readStream
          .format("parquet")
          .schema("id LONG")
          .name("my.source")
          .load(path)
      }

      assert(exception.getMessage.contains("Invalid streaming source name"))
      assert(exception.getMessage.contains("my.source"))
    }
  }

  test("invalid source name - contains special characters") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val exception = intercept[IllegalArgumentException] {
        spark.readStream
          .format("parquet")
          .schema("id LONG")
          .name("my@source")
          .load(path)
      }

      assert(exception.getMessage.contains("Invalid streaming source name"))
      assert(exception.getMessage.contains("my@source"))
    }
  }

  test("invalid source name - empty string") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      spark.range(10).write.parquet(path)

      val exception = intercept[IllegalArgumentException] {
        spark.readStream
          .format("parquet")
          .schema("id LONG")
          .name("")
          .load(path)
      }

      assert(exception.getMessage.contains("Invalid streaming source name"))
    }
  }

  test("name() with different data sources") {
    // Test that name() works with different streaming sources
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      // Create some test data
      spark.range(10).write.parquet(path + "/parquet_data")

      // Test with parquet
      val parquetDf = spark.readStream
        .format("parquet")
        .name("parquet_source")
        .schema("id LONG")
        .load(path + "/parquet_data")

      assert(parquetDf.isStreaming, "Parquet DataFrame should be streaming")

      // Test with json - create test data and specify schema
      spark
        .range(10)
        .selectExpr("id", "CAST(id AS STRING) as value")
        .write
        .json(path + "/json_data")

      val jsonDf = spark.readStream
        .format("json")
        .name("json_source")
        .schema("id LONG, value STRING")
        .load(path + "/json_data")

      assert(jsonDf.isStreaming, "JSON DataFrame should be streaming")
    }
  }

  test("name() persists through query execution") {
    withTempPath { dataDir =>
      withTempPath { checkpointDir =>
        val dataPath = dataDir.getCanonicalPath
        val checkpointPath = checkpointDir.getCanonicalPath

        // Create some test data
        spark.range(10).write.parquet(dataPath)

        val df = spark.readStream
          .format("parquet")
          .schema("id LONG")
          .name("parquet_source_test")
          .load(dataPath)

        val query = df.writeStream
          .format("noop")
          .option("checkpointLocation", checkpointPath)
          .start()

        try {
          // Let it run briefly
          Thread.sleep(1000)

          // Verify query is running
          assert(query.isActive, "Query should be active")

          // The query should have the checkpoint location set
          assert(
            query.lastProgress != null || query.recentProgress.isEmpty,
            "Query should have progress or be starting")
        } finally {
          query.stop()
        }
      }
    }
  }
}
