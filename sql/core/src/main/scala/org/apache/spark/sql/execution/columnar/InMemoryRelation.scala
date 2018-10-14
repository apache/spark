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

import org.apache.commons.lang3.StringUtils

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{HintInfo, LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{LongAccumulator, Utils}


/**
 * CachedBatch is a cached batch of rows.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
private[columnar]
case class CachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)

case class CachedRDDBuilder(
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    @transient cachedPlan: SparkPlan,
    tableName: Option[String])(
    @transient private var _cachedColumnBuffers: RDD[CachedBatch] = null) {

  val sizeInBytesStats: LongAccumulator = cachedPlan.sqlContext.sparkContext.longAccumulator

  def cachedColumnBuffers: RDD[CachedBatch] = {
    if (_cachedColumnBuffers == null) {
      synchronized {
        if (_cachedColumnBuffers == null) {
          _cachedColumnBuffers = buildBuffers()
        }
      }
    }
    _cachedColumnBuffers
  }

  def clearCache(blocking: Boolean = true): Unit = {
    if (_cachedColumnBuffers != null) {
      synchronized {
        if (_cachedColumnBuffers != null) {
          _cachedColumnBuffers.unpersist(blocking)
          _cachedColumnBuffers = null
        }
      }
    }
  }

  def withCachedPlan(cachedPlan: SparkPlan): CachedRDDBuilder = {
    new CachedRDDBuilder(
      useCompression,
      batchSize,
      storageLevel,
      cachedPlan = cachedPlan,
      tableName
    )(_cachedColumnBuffers)
  }

  private def buildBuffers(): RDD[CachedBatch] = {
    val output = cachedPlan.output
    val cached = cachedPlan.execute().mapPartitionsInternal { rowIterator =>
      new Iterator[CachedBatch] {
        def next(): CachedBatch = {
          val columnBuilders = output.map { attribute =>
            ColumnBuilder(attribute.dataType, batchSize, attribute.name, useCompression)
          }.toArray

          var rowCount = 0
          var totalSize = 0L
          while (rowIterator.hasNext && rowCount < batchSize
            && totalSize < ColumnBuilder.MAX_BATCH_SIZE_IN_BYTE) {
            val row = rowIterator.next()

            // Added for SPARK-6082. This assertion can be useful for scenarios when something
            // like Hive TRANSFORM is used. The external data generation script used in TRANSFORM
            // may result malformed rows, causing ArrayIndexOutOfBoundsException, which is somewhat
            // hard to decipher.
            assert(
              row.numFields == columnBuilders.length,
              s"Row column number mismatch, expected ${output.size} columns, " +
                s"but got ${row.numFields}." +
                s"\nRow content: $row")

            var i = 0
            totalSize = 0
            while (i < row.numFields) {
              columnBuilders(i).appendFrom(row, i)
              totalSize += columnBuilders(i).columnStats.sizeInBytes
              i += 1
            }
            rowCount += 1
          }

          sizeInBytesStats.add(totalSize)

          val stats = InternalRow.fromSeq(
            columnBuilders.flatMap(_.columnStats.collectedStatistics))
          CachedBatch(rowCount, columnBuilders.map { builder =>
            JavaUtils.bufferToArray(builder.build())
          }, stats)
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }.persist(storageLevel)

    cached.setName(
      tableName.map(n => s"In-memory table $n")
        .getOrElse(StringUtils.abbreviate(cachedPlan.toString, 1024)))
    cached
  }
}

object InMemoryRelation {

  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String],
      logicalPlan: LogicalPlan): InMemoryRelation = {
    val cacheBuilder = CachedRDDBuilder(useCompression, batchSize, storageLevel, child, tableName)()
    new InMemoryRelation(child.output, cacheBuilder)(
      statsOfPlanToCache = logicalPlan.stats, outputOrdering = logicalPlan.outputOrdering)
  }

  def apply(cacheBuilder: CachedRDDBuilder, logicalPlan: LogicalPlan): InMemoryRelation = {
    new InMemoryRelation(cacheBuilder.cachedPlan.output, cacheBuilder)(
      statsOfPlanToCache = logicalPlan.stats, outputOrdering = logicalPlan.outputOrdering)
  }
}

case class InMemoryRelation(
    output: Seq[Attribute],
    @transient cacheBuilder: CachedRDDBuilder)(
    statsOfPlanToCache: Statistics,
    override val outputOrdering: Seq[SortOrder])
  extends logical.LeafNode with MultiInstanceRelation {

  override protected def innerChildren: Seq[SparkPlan] = Seq(cachedPlan)

  override def doCanonicalize(): logical.LogicalPlan =
    copy(output = output.map(QueryPlan.normalizeExprId(_, cachedPlan.output)),
      cacheBuilder)(
      statsOfPlanToCache,
      outputOrdering)

  override def producedAttributes: AttributeSet = outputSet

  @transient val partitionStatistics = new PartitionStatistics(output)

  def cachedPlan: SparkPlan = cacheBuilder.cachedPlan

  override def computeStats(): Statistics = {
    if (cacheBuilder.sizeInBytesStats.value == 0L) {
      // Underlying columnar RDD hasn't been materialized, use the stats from the plan to cache.
      // Note that we should drop the hint info here. We may cache a plan whose root node is a hint
      // node. When we lookup the cache with a semantically same plan without hint info, the plan
      // returned by cache lookup should not have hint info. If we lookup the cache with a
      // semantically same plan with a different hint info, `CacheManager.useCachedData` will take
      // care of it and retain the hint info in the lookup input plan.
      statsOfPlanToCache.copy(hints = HintInfo())
    } else {
      Statistics(sizeInBytes = cacheBuilder.sizeInBytesStats.value.longValue)
    }
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(newOutput, cacheBuilder)(statsOfPlanToCache, outputOrdering)
  }

  override def newInstance(): this.type = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      cacheBuilder)(
        statsOfPlanToCache,
        outputOrdering).asInstanceOf[this.type]
  }

  override protected def otherCopyArgs: Seq[AnyRef] = Seq(statsOfPlanToCache, outputOrdering)

  override def simpleString: String =
    s"InMemoryRelation [${Utils.truncatedString(output, ", ")}], ${cacheBuilder.storageLevel}"
}
