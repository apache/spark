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
package org.apache.spark.sql.execution.datasources.parquet

import java.math.BigDecimal
import java.sql.{Date, Timestamp}
import java.time.{Duration, Period}

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.parquet.column.{Encoding, ParquetProperties}
import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.TestUtils
import org.apache.spark.memory.MemoryMode
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

// TODO: this needs a lot more testing but it's currently not easy to test with the parquet
// writer abstractions. Revisit.
class ParquetEncodingSuite extends ParquetCompatibilityTest with SharedSparkSession {
  import testImplicits._

  val ROW = ((1).toByte, 2, 3L, "abc", Period.of(1, 1, 0), Duration.ofMillis(100), true)
  val NULL_ROW = (
    null.asInstanceOf[java.lang.Byte],
    null.asInstanceOf[Integer],
    null.asInstanceOf[java.lang.Long],
    null.asInstanceOf[String],
    null.asInstanceOf[Period],
    null.asInstanceOf[Duration],
    null.asInstanceOf[java.lang.Boolean])

  private def withMemoryModes(f: String => Unit): Unit = {
    Seq(MemoryMode.OFF_HEAP, MemoryMode.ON_HEAP).foreach(mode => {
      val offHeap = if (mode == MemoryMode.OFF_HEAP) "true" else "false"
      f(offHeap)
    })
  }

  test("All Types Dictionary") {
    (1 :: 1000 :: Nil).foreach { n => {
      withTempPath { dir =>
        List.fill(n)(ROW).toDF.repartition(1).write.parquet(dir.getCanonicalPath)
        val file = TestUtils.listDirectory(dir).head

        val conf = sqlContext.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        reader.initialize(file, null)
        val batch = reader.resultBatch()
        assert(reader.nextBatch())
        assert(batch.numRows() == n)
        var i = 0
        while (i < n) {
          assert(batch.column(0).getByte(i) == 1)
          assert(batch.column(1).getInt(i) == 2)
          assert(batch.column(2).getLong(i) == 3)
          assert(batch.column(3).getUTF8String(i).toString == "abc")
          assert(batch.column(4).getInt(i) == 13)
          assert(batch.column(5).getLong(i) == 100000)
          assert(batch.column(6).getBoolean(i) == true)
          i += 1
        }
        reader.close()
      }
    }}
  }

  test("All Types Null") {
    (1 :: 100 :: Nil).foreach { n => {
      withTempPath { dir =>
        val data = List.fill(n)(NULL_ROW).toDF
        data.repartition(1).write.parquet(dir.getCanonicalPath)
        val file = TestUtils.listDirectory(dir).head

        val conf = sqlContext.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        reader.initialize(file, null)
        val batch = reader.resultBatch()
        assert(reader.nextBatch())
        assert(batch.numRows() == n)
        var i = 0
        while (i < n) {
          assert(batch.column(0).isNullAt(i))
          assert(batch.column(1).isNullAt(i))
          assert(batch.column(2).isNullAt(i))
          assert(batch.column(3).isNullAt(i))
          assert(batch.column(4).isNullAt(i))
          assert(batch.column(5).isNullAt(i))
          assert(batch.column(6).isNullAt(i))
          i += 1
        }
        reader.close()
      }}
    }
  }

  test("Read row group containing both dictionary and plain encoded pages") {
    withSQLConf(ParquetOutputFormat.DICTIONARY_PAGE_SIZE -> "2048",
      ParquetOutputFormat.PAGE_SIZE -> "4096") {
      withTempPath { dir =>
        // In order to explicitly test for SPARK-14217, we set the parquet dictionary and page size
        // such that the following data spans across 3 pages (within a single row group) where the
        // first page is dictionary encoded and the remaining two are plain encoded.
        val data = (0 until 512).flatMap(i => Seq.fill(3)(i.toString))
        data.toDF("f").coalesce(1).write.parquet(dir.getCanonicalPath)
        val file = TestUtils.listDirectory(dir).head

        val conf = sqlContext.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        reader.initialize(file, null /* set columns to null to project all columns */)
        val column = reader.resultBatch().column(0)
        assert(reader.nextBatch())

        (0 until 512).foreach { i =>
          assert(column.getUTF8String(3 * i).toString == i.toString)
          assert(column.getUTF8String(3 * i + 1).toString == i.toString)
          assert(column.getUTF8String(3 * i + 2).toString == i.toString)
        }
        reader.close()
      }
    }
  }

  test("parquet v2 pages - delta encoding") {
    val extraOptions = Map[String, String](
      ParquetOutputFormat.WRITER_VERSION -> ParquetProperties.WriterVersion.PARQUET_2_0.toString,
      ParquetOutputFormat.ENABLE_DICTIONARY -> "false"
    )

    val hadoopConf = spark.sessionState.newHadoopConfWithOptions(extraOptions)
    withMemoryModes { offHeapMode =>
      withSQLConf(
        SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offHeapMode,
        SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
        ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL") {
        withTempPath { dir =>
          val path = s"${dir.getCanonicalPath}/test.parquet"
          // Have more than 2 * 4096 records (so we have multiple tasks and each task
          // reads at least twice from the reader). This will catch any issues with state
          // maintained by the reader(s)
          // Add at least one string with a null
          val data = (1 to 8193).map { i =>
            (i,
              i.toLong, i.toShort, Array[Byte](i.toByte),
              if (i % 2 == 1) s"test_$i" else null,
              DateTimeUtils.fromJavaDate(Date.valueOf(s"2021-11-0" + ((i % 9) + 1))),
              DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf(s"2020-11-01 12:00:0" + (i % 10))),
              Period.of(1, (i % 11) + 1, 0),
              Duration.ofMillis(((i % 9) + 1) * 100),
              new BigDecimal(java.lang.Long.toUnsignedString(i * 100000))
            )
          }

          spark.createDataFrame(data)
            .write.options(extraOptions).mode("overwrite").parquet(path)

          val blockMetadata = readFooter(new Path(path), hadoopConf).getBlocks.asScala.head
          val columnChunkMetadataList = blockMetadata.getColumns.asScala

          // Verify that indeed delta encoding is used for each column
          assert(columnChunkMetadataList.length === 10)
          assert(columnChunkMetadataList(0).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          assert(columnChunkMetadataList(1).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          assert(columnChunkMetadataList(2).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          // Both fixed-length byte array and variable-length byte array (also called BINARY)
          // are use DELTA_BYTE_ARRAY for encoding
          assert(columnChunkMetadataList(3).getEncodings.contains(Encoding.DELTA_BYTE_ARRAY))
          assert(columnChunkMetadataList(4).getEncodings.contains(Encoding.DELTA_BYTE_ARRAY))

          assert(columnChunkMetadataList(5).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          assert(columnChunkMetadataList(6).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          assert(columnChunkMetadataList(7).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          assert(columnChunkMetadataList(8).getEncodings.contains(Encoding.DELTA_BINARY_PACKED))
          assert(columnChunkMetadataList(9).getEncodings.contains(Encoding.DELTA_BYTE_ARRAY))

          val actual = spark.read.parquet(path).collect()
          assert(actual.sortBy(_.getInt(0)) === data.map(Row.fromTuple));
        }
      }
    }
  }

  test("parquet v2 pages - rle encoding for boolean value columns") {
    val extraOptions = Map[String, String](
      ParquetOutputFormat.WRITER_VERSION -> ParquetProperties.WriterVersion.PARQUET_2_0.toString
    )

    val hadoopConf = spark.sessionState.newHadoopConfWithOptions(extraOptions)
    withSQLConf(
      SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> "true",
      ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL") {
      withTempPath { dir =>
        val path = s"${dir.getCanonicalPath}/test.parquet"
        val size = 10000
        val data = (1 to size).map { i => (true, false, i % 2 == 1) }

        spark.createDataFrame(data)
          .write.options(extraOptions).mode("overwrite").parquet(path)

        val blockMetadata = readFooter(new Path(path), hadoopConf).getBlocks.asScala.head
        val columnChunkMetadataList = blockMetadata.getColumns.asScala

        // Verify that indeed rle encoding is used for each column
        assert(columnChunkMetadataList.length === 3)
        assert(columnChunkMetadataList.head.getEncodings.contains(Encoding.RLE))
        assert(columnChunkMetadataList(1).getEncodings.contains(Encoding.RLE))
        assert(columnChunkMetadataList(2).getEncodings.contains(Encoding.RLE))

        val actual = spark.read.parquet(path).collect()
        assert(actual.length == size)
        assert(actual.map(_.getBoolean(0)).forall(_ == true))
        assert(actual.map(_.getBoolean(1)).forall(_ == false))
        val excepted = (1 to size).map { i => i % 2 == 1 }
        assert(actual.map(_.getBoolean(2)).sameElements(excepted))
      }
    }
  }
}
