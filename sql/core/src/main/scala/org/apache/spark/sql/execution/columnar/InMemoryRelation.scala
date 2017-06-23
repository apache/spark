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
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.ColumnarBatch
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.LongAccumulator


object InMemoryRelation {
  def apply(
      useCompression: Boolean,
      batchSize: Int,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String]): InMemoryRelation =
    new InMemoryRelation(child.output, useCompression, batchSize, storageLevel, child, tableName)()
}


/**
 * An abstract representation of a cached batch of rows.
 */
private[columnar] trait CachedBatch {
  val stats: InternalRow
  def getNumRows(): Int
}


/**
 * A cached batch of rows stored as a list of byte arrays, one for each column.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
private[columnar] case class CachedBatchBytes(
    numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
  extends CachedBatch {
  def getNumRows(): Int = numRows
}


/**
 * A cached batch of rows stored as a [[ColumnarBatch]].
 */
private[columnar] case class CachedColumnarBatch(columnarBatch: ColumnarBatch, stats: InternalRow)
  extends CachedBatch {
  def getNumRows(): Int = columnarBatch.numRows()
}


case class InMemoryRelation(
    output: Seq[Attribute],
    useCompression: Boolean,
    batchSize: Int,
    storageLevel: StorageLevel,
    @transient child: SparkPlan,
    tableName: Option[String])(
    @transient var _cachedColumnBuffers: RDD[CachedBatch] = null,
    val batchStats: LongAccumulator = child.sqlContext.sparkContext.longAccumulator)
  extends logical.LeafNode with MultiInstanceRelation {

  private[columnar] val useColumnarBatches: Boolean = {
    // In the initial implementation, for ease of review
    // support only integer and double and # of fields is less than wholeStageMaxNumFields
    val schema = StructType.fromAttributes(child.output)
    schema.fields.find(f => f.dataType match {
      case IntegerType => false
      case DoubleType => false
      case _ => true
    }).isEmpty
    child.sqlContext.conf.getConf(SQLConf.CACHE_CODEGEN)
  }

  override def innerChildren: Seq[SparkPlan] = Seq(child)

  override def producedAttributes: AttributeSet = outputSet

  @transient val partitionStatistics = new PartitionStatistics(output)

  override def computeStats(conf: SQLConf): Statistics = {
    if (batchStats.value == 0L) {
      // Underlying columnar RDD hasn't been materialized, no useful statistics information
      // available, return the default statistics.
      Statistics(sizeInBytes = child.sqlContext.conf.defaultSizeInBytes)
    } else {
      Statistics(sizeInBytes = batchStats.value.longValue)
    }
  }

  /**
   * Batch the input rows into [[CachedBatch]]es.
   */
  private def buildColumnBuffers: RDD[CachedBatch] = {
    val buffers =
      if (useColumnarBatches) {
        buildColumnarBatches()
      } else {
        buildColumnBytes()
      }
    buffers.setName(
      tableName.map { n => s"In-memory table $n" }
        .getOrElse(StringUtils.abbreviate(child.toString, 1024)))
    buffers.asInstanceOf[RDD[CachedBatch]]
  }

  /**
   * Batch the input rows into [[CachedBatchBytes]] built using [[ColumnBuilder]]s.
   *
   * This handles complex types and compression, but is more expensive than
   * [[buildColumnarBatches]], which generates code to build the buffers.
   */
  private def buildColumnBytes(): RDD[CachedBatchBytes] = {
    val output = child.output
    child.execute().mapPartitionsInternal { rowIterator =>
      new Iterator[CachedBatchBytes] {
        def next(): CachedBatchBytes = {
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

          batchStats.add(totalSize)

          val stats = InternalRow.fromSeq(
            columnBuilders.flatMap(_.columnStats.collectedStatistics))
          CachedBatchBytes(rowCount, columnBuilders.map { builder =>
            JavaUtils.bufferToArray(builder.build())
          }, stats)
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }.persist(storageLevel)
  }

  /**
   * Batch the input rows using [[ColumnarBatch]]es.
   *
   * Compared with [[buildColumnBytes]], this provides a faster implementation of memory
   * scan because both the read path and the write path are generated.
   * However, this does not compress data for now
   */
  private def buildColumnarBatches(): RDD[CachedColumnarBatch] = {
    val schema = StructType.fromAttributes(child.output)
    val newStorageLevel = GenerateColumnarBatch.compressStorageLevel(storageLevel, useCompression)
    child.execute().mapPartitionsInternal { rows =>
      new GenerateColumnarBatch(schema, batchSize, newStorageLevel).generate(rows).map {
        cachedColumnarBatch => {
          var i = 0
          var totalSize = 0L
          while (i < cachedColumnarBatch.columnarBatch.numCols()) {
            totalSize += cachedColumnarBatch.stats.getLong(4 + i * 5)
            i += 1
          }
          batchStats.add(totalSize)
          cachedColumnarBatch
        }
      }
    }.persist(storageLevel)
  }

  // If the cached column buffers were not passed in, we calculate them in the constructor.
  // As in Spark, the actual work of caching is lazy.
  if (_cachedColumnBuffers == null) {
    _cachedColumnBuffers = buildColumnBuffers
  }

  def recache(): Unit = {
    if (_cachedColumnBuffers != null) {
      _cachedColumnBuffers.unpersist()
      _cachedColumnBuffers = null
    }
    _cachedColumnBuffers = buildColumnBuffers
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    InMemoryRelation(
      newOutput, useCompression, batchSize, storageLevel, child, tableName)(
        _cachedColumnBuffers, batchStats)
  }

  override def newInstance(): this.type = {
    new InMemoryRelation(
      output.map(_.newInstance()),
      useCompression,
      batchSize,
      storageLevel,
      child,
      tableName)(
        _cachedColumnBuffers,
        batchStats).asInstanceOf[this.type]
  }

  /**
   * Return lazily cached batches of rows in the original plan.
   */
  def cachedColumnBuffers: RDD[CachedBatch] = {
    if (_cachedColumnBuffers == null) {
      _cachedColumnBuffers = buildColumnBuffers
    }
    _cachedColumnBuffers
  }

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(_cachedColumnBuffers, batchStats)
}
