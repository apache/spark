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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.v2.reader.ScanBuilder
import org.apache.spark.sql.sources.v2.writer.WriteBuilder
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DummyReadOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyReadOnlyFileTable
  }
}

class DummyReadOnlyFileTable extends Table with SupportsRead {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    throw new AnalysisException("Dummy file reader")
  }

  override def capabilities(): java.util.Set[TableCapability] =
    Set(TableCapability.BATCH_READ).asJava
}

class DummyWriteOnlyFileDataSourceV2 extends FileDataSourceV2 {

  override def fallbackFileFormat: Class[_ <: FileFormat] = classOf[ParquetFileFormat]

  override def shortName(): String = "parquet"

  override def getTable(options: CaseInsensitiveStringMap): Table = {
    new DummyWriteOnlyFileTable
  }
}

class DummyWriteOnlyFileTable extends Table with SupportsWrite {
  override def name(): String = "dummy"

  override def schema(): StructType = StructType(Nil)

  override def newWriteBuilder(options: CaseInsensitiveStringMap): WriteBuilder =
    throw new AnalysisException("Dummy file writer")

  override def capabilities(): java.util.Set[TableCapability] =
    Set(TableCapability.BATCH_WRITE).asJava
}

class FileDataSourceV2FallBackSuite extends QueryTest with SharedSQLContext {

  private val dummyParquetReaderV2 = classOf[DummyReadOnlyFileDataSourceV2].getName
  private val dummyParquetWriterV2 = classOf[DummyWriteOnlyFileDataSourceV2].getName

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
        spark.read.format(dummyParquetReaderV2).load(path).collect()
      }
      assert(exception.message.equals("Dummy file reader"))
    }
  }

  test("Fall back read path to v1 with configuration USE_V1_SOURCE_READER_LIST") {
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
          spark.read.format(dummyParquetReaderV2).load(path).collect()
        }
        assert(exception.message.equals("Dummy file reader"))
      }
    }
  }

  test("Fall back to v1 when reading file with write only FileDataSourceV2") {
    val df = spark.range(10).toDF()
    withTempPath { file =>
      val path = file.getCanonicalPath
      // Dummy File writer should fail as expected.
      val exception = intercept[AnalysisException] {
        df.write.format(dummyParquetWriterV2).save(path)
      }
      assert(exception.message.equals("Dummy file writer"))
      df.write.parquet(path)
      // Fallback reads to V1
      checkAnswer(spark.read.format(dummyParquetWriterV2).load(path), df)
    }
  }

  test("Fall back write path to v1 with configuration USE_V1_SOURCE_WRITER_LIST") {
    val df = spark.range(10).toDF()
    Seq(
      "foo,parquet,bar",
      "ParQuet,bar,foo",
      s"foobar,$dummyParquetWriterV2"
    ).foreach { fallbackWriters =>
      withSQLConf(SQLConf.USE_V1_SOURCE_WRITER_LIST.key -> fallbackWriters) {
        withTempPath { file =>
          val path = file.getCanonicalPath
          // Writes should fall back to v1 and succeed.
          df.write.format(dummyParquetWriterV2).save(path)
          checkAnswer(spark.read.parquet(path), df)
        }
      }
    }
    withSQLConf(SQLConf.USE_V1_SOURCE_WRITER_LIST.key -> "foo,bar") {
      withTempPath { file =>
        val path = file.getCanonicalPath
        // Dummy File reader should fail as USE_V1_SOURCE_READER_LIST doesn't include it.
        val exception = intercept[AnalysisException] {
          df.write.format(dummyParquetWriterV2).save(path)
        }
        assert(exception.message.equals("Dummy file writer"))
      }
    }
  }
}
