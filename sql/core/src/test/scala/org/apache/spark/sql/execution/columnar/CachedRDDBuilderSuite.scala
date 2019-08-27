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

import org.scalatest.Matchers._
import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, SparkFunSuite, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel

class CachedRDDBuilderSuite extends SparkFunSuite with SharedSparkSession {

  test("CachedRDDBuilder correctly generates and increments ids") {
    val cacheBuilder1 = createCachedRDDBuilder()
    val cacheBuilder2 = createCachedRDDBuilder()
    val cacheBuilder3 = createCachedRDDBuilder()

    cacheBuilder1.id should not be cacheBuilder2.id
    cacheBuilder2.id should not be cacheBuilder3.id
    cacheBuilder3.id should not be cacheBuilder1.id
  }

  test("CachedRDDBuilder correctly generates new id when copied") {
    val cacheBuilder1 = createCachedRDDBuilder()
    val cacheBuilder2 = cacheBuilder1.copy()

    cacheBuilder1.id should not be cacheBuilder2.id
  }

  test("CachedRDDBuilder correctly stores id in metadata") {
    val cacheBuilder = createCachedRDDBuilder()

    cacheBuilder.metadata("cacheBuilderId") shouldBe cacheBuilder.id.toString
  }

  test("CachedRDDBuilder does not update numComputedPartitions metric with cached plan with" +
    " no partitions") {
    val sourceDataFrame = sqlContext.sparkSession.emptyDataFrame
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan)

    cacheBuilder.metrics("numComputedPartitions").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 0
  }

  test("CachedRDDBuilder correctly updates numComputedPartitions metric with single empty" +
    " partition") {
    val sourceRDD = new emptyPartitionsRDD[Row](1, sparkContext)
    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(sourceRDD, StructType(Nil))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan)

    cacheBuilder.metrics("numComputedPartitions").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 1
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 1
  }

  test("CachedRDDBuilder correctly updates numComputedPartitions metric with multiple empty" +
    " partitions") {
    val sourceRDD = new emptyPartitionsRDD[Row](3, sparkContext)
    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(sourceRDD, StructType(Nil))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan)

    cacheBuilder.metrics("numComputedPartitions").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 3
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 3
  }

  test("CachedRDDBuilder correctly updates numComputedPartitions metric") {
    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(Seq(("1", "2")))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan)

    cacheBuilder.metrics("numComputedPartitions").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 1
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 1
  }

  test("CachedRDDBuilder correctly updates numComputedPartitions metric with multiple" +
    " partitions") {
    // Sets the number of partitions that will be computed to 3
    sqlContext.sparkContext.conf.set("spark.default.parallelism", "3")

    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(Seq(
      ("1", "2"),
      ("3", "4"),
      ("5", "6"),
      ("7", "8"),
      ("9", "0")))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan)

    cacheBuilder.metrics("numComputedPartitions").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 3
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedPartitions").value shouldBe 3
  }

  test("CachedRDDBuilder correctly updates numComputedRows metric") {
    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(Seq(("1", "2")))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan)

    cacheBuilder.metrics("numComputedRows").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedRows").value shouldBe 1
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedRows").value shouldBe 1
  }

  test("CachedRDDBuilder correctly updates numComputedRows metric with batchSize less than" +
    " rows computed") {
    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(Seq(
      ("1", "2"),
      ("3", "4"),
      ("5", "6")))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder = createCachedRDDBuilder(cachedPlan, 1)

    cacheBuilder.metrics("numComputedRows").value shouldBe 0
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedRows").value shouldBe 3
    cacheBuilder.cachedColumnBuffers.collect()
    cacheBuilder.metrics("numComputedRows").value shouldBe 3
  }

  test("Copied CachedRDDBuilder nulls out buffers") {
    val sourceDataFrame = sqlContext.sparkSession.createDataFrame(Seq(("1", "2")))
    val cachedPlan = sourceDataFrame.queryExecution.executedPlan
    val cacheBuilder1 = createCachedRDDBuilder(cachedPlan)
    val output1 = cacheBuilder1.cachedColumnBuffers
    val cacheBuilder2 = cacheBuilder1.copy()

    cacheBuilder1.isCachedColumnBuffersLoaded shouldBe true
    cacheBuilder2.isCachedColumnBuffersLoaded shouldBe false

    val output2 = cacheBuilder2.cachedColumnBuffers

    output1 should not be output2
  }

  private def createCachedRDDBuilder(cachedPlan: SparkPlan = NamedDummyLeafNode(),
                                     batchSize: Int = 10): CachedRDDBuilder = {
    CachedRDDBuilder(useCompression = false, batchSize, StorageLevel.MEMORY_ONLY, cachedPlan, None)
  }
}

private case class NamedDummyLeafNode(override val nodeName: String = "") extends LeafExecNode {
  override protected def doExecute(): RDD[InternalRow] = throw new UnsupportedOperationException
  override def output: Seq[Attribute] = Seq.empty
}

private class emptyPartitionsRDD[T: ClassTag](numPartitions: Int, sc: SparkContext)
  extends RDD[T](sc, Seq.empty) {

  override def getPartitions: Array[Partition] = {
    (0 until numPartitions).map(index => new emptyPartition(index)).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    Seq().iterator
  }
}

private class emptyPartition(val index: Int) extends Partition
