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

import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
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
      println("================", n)
      withTempPath { dir =>
        List.fill(n)(ROW).toDF.repartition(1).write.parquet(dir.getCanonicalPath)
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head

        val reader = new VectorizedParquetRecordReader
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

        val reader = new VectorizedParquetRecordReader
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

  test("Hybrid") {
    (128 * 1024 :: Nil).foreach { n =>

      withTempPath { dir =>
        val l = (0 until n).map(i => (i, i))
          .flatMap(i => List(i._1.toString))
        ( l ++ List.fill(10 * 1024 * 1024)(1.toString))
          .toDF.repartition(1).write.parquet(dir.getCanonicalPath)
        val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head

        val reader = new VectorizedParquetRecordReader
        reader.initialize(file.asInstanceOf[String], null)
        val batch = reader.resultBatch()
        assert(reader.nextBatch())
        //assert(batch.numRows() == 2 * n)
        //assert(batch.column(0).asInstanceOf[OnHeapColumnVector].intData.toSeq == l ++ l)

        var i = 0
        while (i < n) {
          //assert(batch.column(0).getUTF8String(2 * i).toString == i.toString)
          //assert(batch.column(0).getUTF8String(2 * i + 1).toString == i.toString)
          //assert(batch.column(0).getUTF8String(3 * i + 2).toString == i.toString)
          //assert(batch.column(0).getInt(4 * i + 3) == i)
          i += 1
        }

        reader.close()
      }
    }
  }

  test("experiments") {
    spark.conf.set("parquet.dictionary.page.size", "2048")
    spark.conf.set("parquet.page.size", "4096")
    // spark.conf.set("parquet.block.size", "81920")

    withTempPath { dir =>
      val data = (10 until 99).map(_.toString * 512) ++ (0 until 99).map(_ => "ab" * 512)
      println(data.length)
      data.toDF("f").coalesce(1).write.mode("overwrite").parquet(dir.getCanonicalPath)
      val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head

      val reader = new VectorizedParquetRecordReader
      reader.initialize(file.asInstanceOf[String], null)
      val batch = reader.resultBatch()
      assert(reader.nextBatch())
    }
  }

  test("experiments 2") {
    spark.conf.set("parquet.dictionary.page.size", "2048")
    spark.conf.set("parquet.page.size", "4096")

    withTempPath { dir =>
      val data = (0 until 512).flatMap(i => List(i.toString, i.toString, i.toString))
      data.toDF("f").coalesce(1).write.mode("overwrite").parquet(dir.getCanonicalPath)
      val file = SpecificParquetRecordReaderBase.listDirectory(dir).toArray.head

      val reader = new VectorizedParquetRecordReader
      reader.initialize(file.asInstanceOf[String], null)
      val batch = reader.resultBatch()
      assert(reader.nextBatch())

      var i = 0
      while (i < 512) {
        assert(batch.column(0).getUTF8String(3 * i).toString == i.toString)
        assert(batch.column(0).getUTF8String(3 * i + 1).toString == i.toString)
        assert(batch.column(0).getUTF8String(3 * i + 2).toString == i.toString)
        i += 1
      }

    }
  }
}
