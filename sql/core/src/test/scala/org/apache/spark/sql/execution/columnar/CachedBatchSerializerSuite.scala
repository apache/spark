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

package org.apache.spark.sql.execution.columnar

import scala.collection.JavaConverters._

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer}
import org.apache.spark.sql.execution.columnar.InMemoryRelation.clearSerializer
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel

case class SingleIntCachedBatch(data: Array[Int]) extends CachedBatch {
  override def numRows: Int = data.length
  override def sizeInBytes: Long = 4 * data.length
}

/**
 * Very simple serializer that only supports a single int column, but does support columnar.
 */
class TestSingleIntColumnarCachedBatchSerializer extends CachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = true

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    throw new IllegalStateException("This does not work. This is only for testing")
  }

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    if (schema.length != 1 || schema.head.dataType != IntegerType) {
      throw new IllegalArgumentException("Only a single column of non-nullable ints works. " +
          s"This is for testing $schema")
    }
    input.map { cb =>
      val column = cb.column(0)
      val data = column.getInts(0, cb.numRows())
      SingleIntCachedBatch(data)
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = true
  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Some(attributes.map(_ => classOf[OnHeapColumnVector].getName))

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    if (selectedAttributes.isEmpty) {
      input.map { cached =>
        val single = cached.asInstanceOf[SingleIntCachedBatch]
        new ColumnarBatch(new Array[ColumnVector](0), single.numRows)
      }
    } else {
      if (selectedAttributes.length > 1 ||
          selectedAttributes.head.dataType != IntegerType) {
        throw new IllegalArgumentException("Only a single column of non-nullable ints works. " +
            s"This is for testing")
      }
      input.map { cached =>
        val single = cached.asInstanceOf[SingleIntCachedBatch]
        val cv = OnHeapColumnVector.allocateColumns(single.numRows, selectedAttributes.toStructType)
        val data = single.data
        cv(0).putInts(0, data.length, data, 0)
        new ColumnarBatch(cv.toArray, single.numRows)
      }
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    convertCachedBatchToColumnarBatch(input, cacheAttributes, selectedAttributes, conf)
      .mapPartitionsInternal { batches =>
        val toUnsafe = UnsafeProjection.create(selectedAttributes, selectedAttributes)
        batches.flatMap { batch =>
          batch.rowIterator().asScala.map(toUnsafe)
        }
      }
  }

  override def buildFilter(
      predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    def ret(index: Int, cb: Iterator[CachedBatch]): Iterator[CachedBatch] = cb
    ret
  }
}

class CachedBatchSerializerSuite  extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(
      StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
      classOf[TestSingleIntColumnarCachedBatchSerializer].getName)
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    clearSerializer()
  }

  protected override def afterAll(): Unit = {
    clearSerializer()
    super.afterAll()
  }

  test("Columnar Cache Plugin") {
    withTempPath { workDir =>
      val workDirPath = workDir.getAbsolutePath
      val input = Seq(100, 200, 300).toDF("count")
      input.write.parquet(workDirPath)
      val data = spark.read.parquet(workDirPath)
      data.cache()
      val df = data.union(data)
      assert(df.count() == 6)
      checkAnswer(df, Row(100) :: Row(200) :: Row(300) :: Row(100) :: Row(200) :: Row(300) :: Nil)
    }
  }
}
