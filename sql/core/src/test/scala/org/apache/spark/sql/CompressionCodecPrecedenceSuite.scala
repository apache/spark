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

package org.apache.spark.sql

import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.execution.datasources.parquet.ParquetOptions
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SQLTestUtils, SharedSQLContext}

class CompressionCodecSuite extends SQLTestUtils with SharedSQLContext {
  test("Test `spark.sql.parquet.compression.codec` config") {
    Seq("NONE", "UNCOMPRESSED", "SNAPPY", "GZIP", "LZO").foreach { c =>
      withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> c) {
        val expected = if (c == "NONE") "UNCOMPRESSED" else c
        val option = new ParquetOptions(Map.empty[String, String], spark.sessionState.conf)
        assert(option.compressionCodecClassName == expected)
      }
    }
  }

  test("[SPARK-21786] Test Acquiring 'compressionCodecClassName' for parquet in right order.") {
    // When "compression" is configured, it should be the first choice.
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      val props = Map("compression" -> "uncompressed", ParquetOutputFormat.COMPRESSION -> "gzip")
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == "UNCOMPRESSED")
    }

    // When "compression" is not configured, "parquet.compression" should be the preferred choice.
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      val props = Map(ParquetOutputFormat.COMPRESSION -> "gzip")
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == "GZIP")
    }

    // When both "compression" and "parquet.compression" are not configured,
    // spark.sql.parquet.compression.codec should be the right choice.
    withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> "snappy") {
      val props = Map.empty[String, String]
      val option = new ParquetOptions(props, spark.sessionState.conf)
      assert(option.compressionCodecClassName == "SNAPPY")
    }
  }
}
