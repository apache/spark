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

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.columnar.{CachedBatch, SimpleMetricsCachedBatch}
import org.apache.spark.sql.execution.columnar.InMemoryRelation.clearSerializer
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

object DummyAllocator {
  private var allocated: Long = 0
  def alloc(size: Long): Unit = synchronized {
    allocated += size
  }
  def release(size: Long): Unit = synchronized {
    allocated -= size
  }
  def getAllocatedMemory: Long = synchronized {
    allocated
  }
}

case class RefCountedCachedBatch(
    numRows: Int,
    stats: InternalRow,
    size: Long,
    cachedBatch: CachedBatch) extends SimpleMetricsCachedBatch with AutoCloseable {
  DummyAllocator.alloc(size)
  var allocated_size: Long = size
  override def close(): Unit = synchronized {
    DummyAllocator.release(allocated_size)
    allocated_size = 0
  }
  override def sizeInBytes: Long = allocated_size
}

class RefCountedTestCachedBatchSerializer extends DefaultCachedBatchSerializer {

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    val cachedBatchRdd = convertForCacheInternal(input, schema, batchSize, useCompression)
    cachedBatchRdd.mapPartitionsInternal { cachedBatchIter =>
      cachedBatchIter.map(cachedBatch => {
        val actualCachedBatch = cachedBatch.asInstanceOf[DefaultCachedBatch]
        new RefCountedCachedBatch(
          actualCachedBatch.numRows,
          actualCachedBatch.stats,
          100,
          cachedBatch)
      })
    }
  }

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    val actualCachedBatchIter = input.mapPartitionsInternal { cachedBatchIter =>
      cachedBatchIter.map(_.asInstanceOf[RefCountedCachedBatch].cachedBatch)
    }
    super.convertCachedBatchToInternalRow(
      actualCachedBatchIter,
      cacheAttributes,
      selectedAttributes,
      conf)
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = false
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false
}

class RefCountedTestCachedBatchSerializerSuite extends QueryTest with SharedSparkSession {
  import testImplicits._

  override protected def sparkConf: SparkConf = {
    super.sparkConf.set(
      StaticSQLConf.SPARK_CACHE_SERIALIZER.key,
      classOf[RefCountedTestCachedBatchSerializer].getName)
  }

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    clearSerializer()
  }

  protected override def afterAll(): Unit = {
    clearSerializer()
    super.afterAll()
  }

  test("SPARK-35396: Release objects stored in InMemoryRelation when clearCache called") {
    val df = spark.range(1, 100).selectExpr("id % 10 as id")
      .rdd.map(id => Tuple1(s"str_$id")).toDF("i")
    val cached = df.cache()
    // count triggers the caching action. It should not throw.
    cached.count()

    // Make sure, the DataFrame is indeed cached.
    assert(spark.sharedState.cacheManager.lookupCachedData(cached).nonEmpty)
    assert(DummyAllocator.getAllocatedMemory > 0)

    // Drop the cache.
    cached.unpersist(blocking = true)

    // Check if refCnt is cleaned.
    assert(DummyAllocator.getAllocatedMemory == 0)
  }
}
