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

import scala.collection.JavaConverters._

import org.apache.parquet.hadoop.ParquetOutputFormat

import org.apache.spark.sql.test.SharedSQLContext

// TODO: this needs a lot more testing but it's currently not easy to test with the parquet
// writer abstractions. Revisit.
class ParquetEncodingSuite extends ParquetCompatibilityTest with SharedSQLContext {
  import testImplicits._

  val ROW = ((1).toByte, 2, 3L, "abc")
  val NULL_ROW = (
    null.asInstanceOf[java.lang.Byte],
    null.asInstanceOf[Integer],
    null.asInstanceOf[java.lang.Long],
    null.asInstanceOf[String])

  test("All Types Dictionary") {
    (1 :: 1000 :: Nil).foreach { n => {
      withTempPath { dir =>
        List.fill(n)(ROW).toDF.repartition(1).write.parquet(dir.getCanonicalPath)
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head

        val conf = sqlContext.conf
        val reader = new VectorizedParquetRecordReader(
          null, conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        reader.initialize(file.asInstanceOf[String], null)
        val batch = reader.resultBatch()
        assert(reader.nextBatch())
        assert(batch.numRows() == n)
        var i = 0
        while (i < n) {
          assert(batch.column(0).getByte(i) == 1)
          assert(batch.column(1).getInt(i) == 2)
          assert(batch.column(2).getLong(i) == 3)
          assert(batch.column(3).getUTF8String(i).toString == "abc")
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
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head

        val conf = sqlContext.conf
        val reader = new VectorizedParquetRecordReader(
          null, conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        reader.initialize(file.asInstanceOf[String], null)
        val batch = reader.resultBatch()
        assert(reader.nextBatch())
        assert(batch.numRows() == n)
        var i = 0
        while (i < n) {
          assert(batch.column(0).isNullAt(i))
          assert(batch.column(1).isNullAt(i))
          assert(batch.column(2).isNullAt(i))
          assert(batch.column(3).isNullAt(i))
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
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).asScala.head

        val conf = sqlContext.conf
        val reader = new VectorizedParquetRecordReader(
          null, conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
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
}
