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

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types.{AtomicType, BinaryType, StructType, UserDefinedType}
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.{LongAccumulator, Utils}

/**
 * Basic interface that all cached batches of data must support. This is primarily to allow
 * for metrics to be handled outside of the encoding and decoding steps in a standard way.
 */
trait CachedBatch {
  def numRows: Int
  def sizeInBytes: Long
}

/**
 * Provides APIs for compressing, filtering, and decompressing SQL data that will be
 * persisted/cached.
 */
trait CachedBatchSerializer extends Serializable {
  /**
   * Run the given plan and convert its output to a implementation of [[CachedBatch]].
   * @param cachedPlan the plan to run.
   * @return the RDD containing the batches of data to cache.
   */
  def convertForCache(cachedPlan: SparkPlan): RDD[CachedBatch]

  /**
   * Builds a function that can be used to filter which batches are loaded.
   * In most cases extending [[SimpleMetricsCachedBatchSerializer]] will provide what
   * you need with the added expense of calculating the min and max value for some
   * data columns, depending on their data type. Note that this is intended to skip batches
   * that are not needed, and the actual filtering of individual rows is handled later.
   * @param predicates the set of expressions to use for filtering.
   * @param cachedAttributes the schema/attributes of the data that is cached. This can be helpful
   *                         if you don't store it with the data.
   * @return a function that takes the partition id and the iterator of batches in the partition.
   *         It returns an iterator of batches that should be loaded.
   */
  def buildFilter(predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]): (Int, Iterator[CachedBatch]) => Iterator[CachedBatch]

  /**
   * Decompress the cached data into a ColumnarBatch. This currently is only used for basic types
   * BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType
   * That may change in the future.
   * @param input the cached batches that should be decompressed.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data, and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return the batches in the ColumnarBatch format.
   */
  def decompressColumnar(input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch]

  /**
   * Decompress the cached into back into [[InternalRow]]. If you want this to be performant code
   * generation is advised.
   * @param input the cached batches that should be decompressed.
   * @param cacheAttributes the attributes of the data in the batch.
   * @param selectedAttributes the field that should be loaded from the data, and the order they
   *                           should appear in the output batch.
   * @param conf the configuration for the job.
   * @return the rows that were stored in the cached batches.
   */
  def decompressToRows(input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow]
}

/**
 * A [[CachedBatch]] that stored some simple metrics that can be used for filtering of batches with
 * the [[SimpleMetricsCachedBatchSerializer]].
 */
trait SimpleMetricsCachedBatch extends CachedBatch {
  /**
   * Holds the same as ColumnStats.
   * upperBound (optional), lowerBound (Optional), nullCount: Int, rowCount: Int, sizeInBytes: Long
   */
  val stats: InternalRow
  override def sizeInBytes: Long = stats.getLong(4)
}

// Currently, only use statistics from atomic types except binary type only.
private object ExtractableLiteral {
  def unapply(expr: Expression): Option[Literal] = expr match {
    case lit: Literal => lit.dataType match {
      case BinaryType => None
      case _: AtomicType => Some(lit)
      case _ => None
    }
    case _ => None
  }
}

/**
 * Provides basic filtering for [[CachedBatchSerializer]] implementations.
 */
trait SimpleMetricsCachedBatchSerializer extends CachedBatchSerializer with Logging {
  override def buildFilter(predicates: Seq[Expression],
      cachedAttributes: Seq[Attribute]):
  (Int, Iterator[CachedBatch]) => Iterator[CachedBatch] = {
    val stats = new PartitionStatistics(cachedAttributes)
    val statsSchema = stats.schema

    def statsFor(a: Attribute): ColumnStatisticsSchema = {
      stats.forAttribute(a)
    }

    // Returned filter predicate should return false iff it is impossible for the input expression
    // to evaluate to `true` based on statistics collected about this partition batch.
    @transient lazy val buildFilter: PartialFunction[Expression, Expression] = {
      case And(lhs: Expression, rhs: Expression)
        if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
        (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

      case Or(lhs: Expression, rhs: Expression)
        if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
        buildFilter(lhs) || buildFilter(rhs)

      case EqualTo(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualTo(ExtractableLiteral(l), a: AttributeReference) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

      case EqualNullSafe(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
      case EqualNullSafe(ExtractableLiteral(l), a: AttributeReference) =>
        statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

      case LessThan(a: AttributeReference, ExtractableLiteral(l)) => statsFor(a).lowerBound < l
      case LessThan(ExtractableLiteral(l), a: AttributeReference) => l < statsFor(a).upperBound

      case LessThanOrEqual(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound <= l
      case LessThanOrEqual(ExtractableLiteral(l), a: AttributeReference) =>
        l <= statsFor(a).upperBound

      case GreaterThan(a: AttributeReference, ExtractableLiteral(l)) => l < statsFor(a).upperBound
      case GreaterThan(ExtractableLiteral(l), a: AttributeReference) => statsFor(a).lowerBound < l

      case GreaterThanOrEqual(a: AttributeReference, ExtractableLiteral(l)) =>
        l <= statsFor(a).upperBound
      case GreaterThanOrEqual(ExtractableLiteral(l), a: AttributeReference) =>
        statsFor(a).lowerBound <= l

      case IsNull(a: Attribute) => statsFor(a).nullCount > 0
      case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0

      case In(a: AttributeReference, list: Seq[Expression])
        if list.forall(ExtractableLiteral.unapply(_).isDefined) && list.nonEmpty =>
        list.map(l => statsFor(a).lowerBound <= l.asInstanceOf[Literal] &&
            l.asInstanceOf[Literal] <= statsFor(a).upperBound).reduce(_ || _)
      // This is an example to explain how it works, imagine that the id column stored as follows:
      // __________________________________________
      // | Partition ID | lowerBound | upperBound |
      // |--------------|------------|------------|
      // |      p1      |    '1'     |    '9'     |
      // |      p2      |    '10'    |    '19'    |
      // |      p3      |    '20'    |    '29'    |
      // |      p4      |    '30'    |    '39'    |
      // |      p5      |    '40'    |    '49'    |
      // |______________|____________|____________|
      //
      // A filter: df.filter($"id".startsWith("2")).
      // In this case it substr lowerBound and upperBound:
      // ________________________________________________________________________________________
      // | Partition ID | lowerBound.substr(0, Length("2")) | upperBound.substr(0, Length("2")) |
      // |--------------|-----------------------------------|-----------------------------------|
      // |      p1      |    '1'                            |    '9'                            |
      // |      p2      |    '1'                            |    '1'                            |
      // |      p3      |    '2'                            |    '2'                            |
      // |      p4      |    '3'                            |    '3'                            |
      // |      p5      |    '4'                            |    '4'                            |
      // |______________|___________________________________|___________________________________|
      //
      // We can see that we only need to read p1 and p3.
      case StartsWith(a: AttributeReference, ExtractableLiteral(l)) =>
        statsFor(a).lowerBound.substr(0, Length(l)) <= l &&
            l <= statsFor(a).upperBound.substr(0, Length(l))
    }

    // When we bind the filters we need to do it against the stats schema
    val partitionFilters: Seq[Expression] = {
      predicates.flatMap { p =>
        val filter = buildFilter.lift(p)
        val boundFilter =
          filter.map(
            BindReferences.bindReference(
              _,
              statsSchema,
              allowFailures = true))

        boundFilter.foreach(_ =>
          filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

        // If the filter can't be resolved then we are missing required statistics.
        boundFilter.filter(_.resolved)
      }
    }

    def ret(index: Int, cachedBatchIterator: Iterator[CachedBatch]): Iterator[CachedBatch] = {
      val partitionFilter = Predicate.create(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        cachedAttributes)

      partitionFilter.initialize(index)
      val schemaIndex = cachedAttributes.zipWithIndex

      cachedBatchIterator.filter { cb =>
        val cachedBatch = cb.asInstanceOf[SimpleMetricsCachedBatch]
        if (!partitionFilter.eval(cachedBatch.stats)) {
          logDebug {
            val statsString = schemaIndex.map { case (a, i) =>
              val value = cachedBatch.stats.get(i, a.dataType)
              s"${a.name}: $value"
            }.mkString(", ")
            s"Skipping partition based on stats $statsString"
          }
          false
        } else {
          true
        }
      }
    }

    ret
  }
}

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
case class DefaultCachedBatch(numRows: Int, buffers: Array[Array[Byte]], stats: InternalRow)
    extends SimpleMetricsCachedBatch

/**
 * The default implementation of CachedBatchSerializer.
 */
class DefaultCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {
  override def convertForCache(cachedPlan: SparkPlan): RDD[CachedBatch] = {
    val batchSize = cachedPlan.conf.columnBatchSize
    val useCompression = cachedPlan.conf.useCompression
    convertForCacheInternal(cachedPlan, batchSize, useCompression)
  }

  def convertForCacheInternal(cachedPlan: SparkPlan,
      batchSize: Int,
      useCompression: Boolean): RDD[CachedBatch] = {
    val output = cachedPlan.output
    cachedPlan.execute().mapPartitionsInternal { rowIterator =>
      new Iterator[DefaultCachedBatch] {
        def next(): DefaultCachedBatch = {
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

          val stats = InternalRow.fromSeq(
            columnBuilders.flatMap(_.columnStats.collectedStatistics))
          DefaultCachedBatch(rowCount, columnBuilders.map { builder =>
            JavaUtils.bufferToArray(builder.build())
          }, stats)
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }
  }

  override def decompressColumnar(input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {

    val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled
    val outputSchema = StructType.fromAttributes(selectedAttributes)
    val columnIndices =
      selectedAttributes.map(a => cacheAttributes.map(o => o.exprId).indexOf(a.exprId)).toArray

    def createAndDecompressColumn(cb: CachedBatch): ColumnarBatch = {
      val cachedColumnarBatch = cb.asInstanceOf[DefaultCachedBatch]
      val rowCount = cachedColumnarBatch.numRows
      val taskContext = Option(TaskContext.get())
      val columnVectors = if (!offHeapColumnVectorEnabled || taskContext.isEmpty) {
        OnHeapColumnVector.allocateColumns(rowCount, outputSchema)
      } else {
        OffHeapColumnVector.allocateColumns(rowCount, outputSchema)
      }
      val columnarBatch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])
      columnarBatch.setNumRows(rowCount)

      for (i <- selectedAttributes.indices) {
        ColumnAccessor.decompress(
          cachedColumnarBatch.buffers(columnIndices(i)),
          columnarBatch.column(i).asInstanceOf[WritableColumnVector],
          outputSchema.fields(i).dataType, rowCount)
      }
      taskContext.foreach(_.addTaskCompletionListener[Unit](_ => columnarBatch.close()))
      columnarBatch
    }

    input.map(createAndDecompressColumn)
  }

  override def decompressToRows(input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[InternalRow] = {
    // Find the ordinals and data types of the requested columns.
    val (requestedColumnIndices, requestedColumnDataTypes) =
      selectedAttributes.map { a =>
        cacheAttributes.map(_.exprId).indexOf(a.exprId) -> a.dataType
      }.unzip

    val columnTypes = requestedColumnDataTypes.map {
      case udt: UserDefinedType[_] => udt.sqlType
      case other => other
    }.toArray

    input.mapPartitionsInternal { cachedBatchIterator =>
      val columnarIterator = GenerateColumnAccessor.generate(columnTypes)
      columnarIterator.initialize(cachedBatchIterator.asInstanceOf[Iterator[DefaultCachedBatch]],
        columnTypes,
        requestedColumnIndices.toArray)
      columnarIterator
    }
  }
}

case class CachedRDDBuilder(
    serializer: CachedBatchSerializer,
    storageLevel: StorageLevel,
    @transient cachedPlan: SparkPlan,
    tableName: Option[String]) {

  @transient @volatile private var _cachedColumnBuffers: RDD[CachedBatch] = null

  val sizeInBytesStats: LongAccumulator = cachedPlan.sqlContext.sparkContext.longAccumulator
  val rowCountStats: LongAccumulator = cachedPlan.sqlContext.sparkContext.longAccumulator

  val cachedName = tableName.map(n => s"In-memory table $n")
    .getOrElse(StringUtils.abbreviate(cachedPlan.toString, 1024))

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

  def clearCache(blocking: Boolean = false): Unit = {
    if (_cachedColumnBuffers != null) {
      synchronized {
        if (_cachedColumnBuffers != null) {
          _cachedColumnBuffers.unpersist(blocking)
          _cachedColumnBuffers = null
        }
      }
    }
  }

  def isCachedColumnBuffersLoaded: Boolean = {
    _cachedColumnBuffers != null
  }

  private def buildBuffers(): RDD[CachedBatch] = {
    val cached = serializer.convertForCache(cachedPlan)
        .map { batch =>
          sizeInBytesStats.add(batch.sizeInBytes)
          rowCountStats.add(batch.numRows)
          batch
        }.persist(storageLevel)
    cached.setName(cachedName)
    cached
  }
}

object InMemoryRelation {

  private[this] var ser: Option[CachedBatchSerializer] = None
  private[this] def getSerializer(sqlConf: SQLConf): CachedBatchSerializer = {
    if (ser.isEmpty) {
      synchronized {
        if (ser.isEmpty) {
          val serName = sqlConf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
          val serClass = Utils.classForName(serName)
          val instance = serClass.getConstructor().newInstance().asInstanceOf[CachedBatchSerializer]
          ser = Some(instance)
        }
      }
    }
    ser.get
  }

  def apply(
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String],
      optimizedPlan: LogicalPlan): InMemoryRelation = {
    val cacheBuilder = CachedRDDBuilder(getSerializer(child.conf), storageLevel, child, tableName)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  /**
   * This API is intended only to be used for testing.
   */
  def apply(
      serializer: CachedBatchSerializer,
      storageLevel: StorageLevel,
      child: SparkPlan,
      tableName: Option[String],
      optimizedPlan: LogicalPlan): InMemoryRelation = {
    val cacheBuilder = CachedRDDBuilder(serializer, storageLevel, child, tableName)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(cacheBuilder: CachedRDDBuilder, optimizedPlan: LogicalPlan): InMemoryRelation = {
    val relation = new InMemoryRelation(
      cacheBuilder.cachedPlan.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(
      output: Seq[Attribute],
      cacheBuilder: CachedRDDBuilder,
      outputOrdering: Seq[SortOrder],
      statsOfPlanToCache: Statistics): InMemoryRelation = {
    val relation = InMemoryRelation(output, cacheBuilder, outputOrdering)
    relation.statsOfPlanToCache = statsOfPlanToCache
    relation
  }
}

case class InMemoryRelation(
    output: Seq[Attribute],
    @transient cacheBuilder: CachedRDDBuilder,
    override val outputOrdering: Seq[SortOrder])
  extends logical.LeafNode with MultiInstanceRelation {

  @volatile var statsOfPlanToCache: Statistics = null

  override def innerChildren: Seq[SparkPlan] = Seq(cachedPlan)

  override def doCanonicalize(): logical.LogicalPlan =
    copy(output = output.map(QueryPlan.normalizeExpressions(_, cachedPlan.output)),
      cacheBuilder,
      outputOrdering)

  @transient val partitionStatistics = new PartitionStatistics(output)

  def cachedPlan: SparkPlan = cacheBuilder.cachedPlan

  private[sql] def updateStats(
      rowCount: Long,
      newColStats: Map[Attribute, ColumnStat]): Unit = this.synchronized {
    val newStats = statsOfPlanToCache.copy(
      rowCount = Some(rowCount),
      attributeStats = AttributeMap((statsOfPlanToCache.attributeStats ++ newColStats).toSeq)
    )
    statsOfPlanToCache = newStats
  }

  override def computeStats(): Statistics = {
    if (!cacheBuilder.isCachedColumnBuffersLoaded) {
      // Underlying columnar RDD hasn't been materialized, use the stats from the plan to cache.
      statsOfPlanToCache
    } else {
      statsOfPlanToCache.copy(
        sizeInBytes = cacheBuilder.sizeInBytesStats.value.longValue,
        rowCount = Some(cacheBuilder.rowCountStats.value.longValue)
      )
    }
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation =
    InMemoryRelation(newOutput, cacheBuilder, outputOrdering, statsOfPlanToCache)

  override def newInstance(): this.type = {
    InMemoryRelation(
      output.map(_.newInstance()),
      cacheBuilder,
      outputOrdering,
      statsOfPlanToCache).asInstanceOf[this.type]
  }

  // override `clone` since the default implementation won't carry over mutable states.
  override def clone(): LogicalPlan = {
    val cloned = this.copy()
    cloned.statsOfPlanToCache = this.statsOfPlanToCache
    cloned
  }

  override def simpleString(maxFields: Int): String =
    s"InMemoryRelation [${truncatedString(output, ", ", maxFields)}], ${cacheBuilder.storageLevel}"
}
