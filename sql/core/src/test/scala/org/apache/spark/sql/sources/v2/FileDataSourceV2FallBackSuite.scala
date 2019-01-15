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
package org.apache.spark.sql.sources.v2

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetTest}
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DummyReadOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallBackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: DataSourceOptions): Table = {
    throw new AnalysisException("Dummy file reader")
  }
}

class FileDataSourceV2FallBackSuite extends QueryTest with ParquetTest with SharedSQLContext {

  private val dummyParquetReaderV2 = classOf[DummyReadOnlyFileDataSourceV2].getName

  test("Fall back to v1 when writing to file with read only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      // Writing file should fall back to v1 and succeed.
      df.write.format(dummyParquetReaderV2).save(path)

      // Validate write result with [[ParquetFileFormat]].
      checkAnswer(spark.read.parquet(path), df)

      // Dummy File reader should fail as expected.
      val exception = intercept[AnalysisException] {
        spark.read.format(dummyParquetReaderV2).load(path)
      }
      assert(exception.message.equals("Dummy file reader"))
    }
  }

  test("Fall back read path to v1 with configuration DISABLED_V2_FILE_DATA_SOURCE_READERS") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      df.write.parquet(path)
      Seq(
        "foo,parquet,bar",
        "ParQuet,bar,foo",
        s"foobar,$dummyParquetReaderV2"
      ).foreach { fallbackReaders =>
        withSQLConf(SQLConf.USE_V1_SOURCE_READER_LIST.key -> fallbackReaders) {
          // Reading file should fall back to v1 and succeed.
          checkAnswer(spark.read.format(dummyParquetReaderV2).load(path), df)
          checkAnswer(sql(s"SELECT * FROM parquet.`$path`"), df)
        }
      }

      withSQLConf(SQLConf.USE_V1_SOURCE_READER_LIST.key -> "foo,bar") {
        // Dummy File reader should fail as DISABLED_V2_FILE_DATA_SOURCE_READERS doesn't include it.
        val exception = intercept[AnalysisException] {
          spark.read.format(dummyParquetReaderV2).load(path)
        }
        assert(exception.message.equals("Dummy file reader"))
      }
    }
  }
}
