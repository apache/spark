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

import com.esotericsoftware.kryo.{DefaultSerializer, Kryo, Serializer => KryoSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}

import org.apache.spark.{SparkException, TaskContext}
import org.apache.spark.network.util.JavaUtils
import org.apache.spark.rdd.{DeterministicLevel, RDD}
import org.apache.spark.sql.catalyst.{FileSourceOptions, InternalRow}
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{logical, QueryPlan}
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.trees.TreePattern.CURRENT_LIKE
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.catalyst.util.{truncatedString, CaseInsensitiveMap}
import org.apache.spark.sql.columnar.{CachedBatch, CachedBatchSerializer, SimpleMetricsCachedBatch, SimpleMetricsCachedBatchSerializer}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.datasources.{FileFormat, FileScanRDD, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.binaryfile.BinaryFileFormat
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.text.TextFileFormat
import org.apache.spark.sql.execution.vectorized.{OffHeapColumnVector, OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.PartitionKeyedAccumulator
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

/**
 * The default implementation of CachedBatch.
 *
 * @param numRows The total number of rows in this batch
 * @param buffers The buffers for serialized columns
 * @param stats The stat of columns
 */
@DefaultSerializer(classOf[DefaultCachedBatchKryoSerializer])
case class DefaultCachedBatch(
     numRows: Int,
     buffers: Array[Array[Byte]],
     stats: InternalRow)
  extends SimpleMetricsCachedBatch

class DefaultCachedBatchKryoSerializer extends KryoSerializer[DefaultCachedBatch] {
  override def write(kryo: Kryo, output: KryoOutput, batch: DefaultCachedBatch): Unit = {
    output.writeInt(batch.numRows)
    SparkException.require(batch.buffers != null, "INVALID_KRYO_SERIALIZER_NO_DATA",
      Map("obj" -> "DefaultCachedBatch.buffers",
        "serdeOp" -> "serialize",
        "serdeClass" -> this.getClass.getName))
    output.writeInt(batch.buffers.length + 1) // +1 to distinguish Kryo.NULL
    for (i <- batch.buffers.indices) {
      val buffer = batch.buffers(i)
        SparkException.require(buffer != null, "INVALID_KRYO_SERIALIZER_NO_DATA",
          Map("obj" -> s"DefaultCachedBatch.buffers($i)",
            "serdeOp" -> "serialize",
            "serdeClass" -> this.getClass.getName))
      output.writeInt(buffer.length + 1)  // +1 to distinguish Kryo.NULL
      output.writeBytes(buffer)
    }
    kryo.writeClassAndObject(output, batch.stats)
  }

  override def read(
      kryo: Kryo, input: KryoInput, cls: Class[DefaultCachedBatch]): DefaultCachedBatch = {
    val numRows = input.readInt()
    val length = input.readInt()
    SparkException.require(length != Kryo.NULL, "INVALID_KRYO_SERIALIZER_NO_DATA",
      Map("obj" -> "DefaultCachedBatch.buffers",
        "serdeOp" -> "deserialize",
        "serdeClass" -> this.getClass.getName))
    val buffers = 0.until(length - 1).map { i => // -1 to restore
      val subLength = input.readInt()
      SparkException.require(subLength != Kryo.NULL, "INVALID_KRYO_SERIALIZER_NO_DATA",
          Map("obj" -> s"DefaultCachedBatch.buffers($i)",
          "serdeOp" -> "deserialize",
          "serdeClass" -> this.getClass.getName))
      val innerArray = new Array[Byte](subLength - 1) // -1 to restore
      input.readBytes(innerArray)
      innerArray
    }.toArray
    val stats = kryo.readClassAndObject(input).asInstanceOf[InternalRow]
    DefaultCachedBatch(numRows, buffers, stats)
  }
}

/**
 * The default implementation of CachedBatchSerializer.
 */
class DefaultCachedBatchSerializer extends SimpleMetricsCachedBatchSerializer {
  override def supportsColumnarInput(schema: Seq[Attribute]): Boolean = false

  override def convertColumnarBatchToCachedBatch(
      input: RDD[ColumnarBatch],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] =
    throw SparkException.internalError("Columnar input is not supported")

  override def convertInternalRowToCachedBatch(
      input: RDD[InternalRow],
      schema: Seq[Attribute],
      storageLevel: StorageLevel,
      conf: SQLConf): RDD[CachedBatch] = {
    val batchSize = conf.columnBatchSize
    val useCompression = conf.useCompression
    convertForCacheInternal(input, schema, batchSize, useCompression)
  }

  def convertForCacheInternal(
      input: RDD[InternalRow],
      output: Seq[Attribute],
      batchSize: Int,
      useCompression: Boolean): RDD[CachedBatch] = {
    input.mapPartitionsInternal { rowIterator =>
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

          val stats = new GenericInternalRow(
            columnBuilders.flatMap(_.columnStats.collectedStatistics))
          DefaultCachedBatch(rowCount, columnBuilders.map { builder =>
            JavaUtils.bufferToArray(builder.build())
          }, stats)
        }

        def hasNext: Boolean = rowIterator.hasNext
      }
    }
  }

  override def supportsColumnarOutput(schema: StructType): Boolean = schema.fields.forall(f =>
    f.dataType match {
      // More types can be supported, but this is to match the original implementation that
      // only supported primitive types "for ease of review"
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType => true
      case _ => false
    })

  override def vectorTypes(attributes: Seq[Attribute], conf: SQLConf): Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(
      if (!conf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))

  override def convertCachedBatchToColumnarBatch(
      input: RDD[CachedBatch],
      cacheAttributes: Seq[Attribute],
      selectedAttributes: Seq[Attribute],
      conf: SQLConf): RDD[ColumnarBatch] = {
    val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled
    val outputSchema = DataTypeUtils.fromAttributes(selectedAttributes)
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

  override def convertCachedBatchToInternalRow(
      input: RDD[CachedBatch],
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
      val columnarIterator = GenerateColumnAccessor.generate(columnTypes.toImmutableArraySeq)
      columnarIterator.initialize(cachedBatchIterator.asInstanceOf[Iterator[DefaultCachedBatch]],
        columnTypes,
        requestedColumnIndices.toArray)
      columnarIterator
    }
  }
}

private[sql]
case class CachedRDDBuilder(
    serializer: CachedBatchSerializer,
    storageLevel: StorageLevel,
    @transient cachedPlan: SparkPlan,
    tableName: Option[String],
    @transient logicalPlan: LogicalPlan,
    isCachedLogicalPlanRepeatable: Boolean = false,
    hasSelectivePredicate: Boolean = false,
    fileSourceOptions: Seq[Map[String, String]] = Seq.empty) {

  @transient @volatile private var _cachedColumnBuffers: RDD[CachedBatch] = null
  @volatile private var isCachedRDDRepeatable = false
  private var hasStrictFileSourceReads = true
  private var _materializedStats: Option[(Long, Long)] = None

  // The cache's materialization bookkeeping: a partition-keyed accumulator storing
  // (rowCount, sizeInBytes) per partition. AQE creates a separate cache scan stage per reference to
  // the same cache and each submits its own build job, so the same partition can be computed by
  // several concurrent jobs (and speculative tasks); Spark has no global cross-executor "compute
  // this partition once" barrier (only a per-executor write lock). Keying by partition id
  // (last-write-wins) means those duplicate completions cannot mark the cache loaded before every
  // partition has been computed -- which otherwise let AQE read rowCount 0 on a non-empty cache and
  // propagate an empty relation, silently dropping rows -- and also yields exact, de-duplicated row
  // count / size.
  private def newPartitionStats(): PartitionKeyedAccumulator[(Long, Long)] = {
    val acc = new PartitionKeyedAccumulator[(Long, Long)]
    cachedPlan.session.sparkContext.register(acc)
    acc
  }

  // Old tasks can still finish after unpersist; isolating each buffer generation prevents their
  // late updates from making a rebuilt cache appear complete.
  private var partitionStats = newPartitionStats()

  val cachedName = tableName.map(n => s"In-memory table $n")
    .getOrElse(Utils.abbreviate(cachedPlan.toString, 1024))

  val supportsColumnarInput: Boolean = {
    cachedPlan.supportsColumnar &&
      serializer.supportsColumnarInput(cachedPlan.output)
  }

  def cachedColumnBuffers: RDD[CachedBatch] = synchronized {
    if (_cachedColumnBuffers == null) {
      _cachedColumnBuffers = buildBuffers()
    }
    _cachedColumnBuffers
  }

  def clearCache(blocking: Boolean = false): Unit = synchronized {
    if (_cachedColumnBuffers != null) {
      _cachedColumnBuffers.unpersist(blocking)
      _cachedColumnBuffers = null
      _materializedStats = None
      partitionStats = newPartitionStats()
    }
    isCachedRDDRepeatable = false
  }

  def isCachedColumnBuffersLoaded: Boolean = loadedMaterializedStats.isDefined

  private[sql] def isCachedPlanRepeatable: Boolean =
    isCachedLogicalPlanRepeatable && isCachedRDDRepeatable

  /** Reads completeness and exact statistics from one cache generation atomically. */
  private[sql] def loadedMaterializedStats: Option[(Long, Long)] = synchronized {
    if (_cachedColumnBuffers == null) {
      None
    } else {
      _materializedStats.orElse {
        partitionStats.foldValuesIfComplete(
          _cachedColumnBuffers.partitions.length,
          (0L, 0L)) {
          case ((rows, bytes), (partitionRows, partitionBytes)) =>
            (rows + partitionRows, bytes + partitionBytes)
        }.map { stats =>
          _materializedStats = Some(stats)
          stats
        }
      }
    }
  }

  private[sql] def repeatableMaterializedStats: Option[(Long, Long)] = synchronized {
    if (isCachedPlanRepeatable) loadedMaterializedStats else None
  }

  // Reported row count / size for the cache's statistics: exact and de-duplicated, folded over the
  // distinct materialized partitions. Synchronized so a fold never races a concurrent `clearCache`
  // reset.
  private[sql] def materializedRowCount: Long = synchronized {
    partitionStats.foldValues(0L)((sum, v) => sum + v._1)
  }

  private[sql] def materializedSizeInBytes: Long = synchronized {
    partitionStats.foldValues(0L)((sum, v) => sum + v._2)
  }

  // The id of the accumulator backing this cache's materialization bookkeeping. Exposed only so
  // `CachedTableSuite`'s accumulator-cleanup test can verify it is cleared after uncache + GC.
  private[sql] def materializationAccumulatorId: Long = synchronized {
    partitionStats.id
  }

  private def buildBuffers(): RDD[CachedBatch] = {
    def buildInputRDD[T](input: => RDD[T]): RDD[T] = {
      if (fileSourceOptions.isEmpty) {
        input
      } else {
        // File scans initialize their input RDD lazily. Check both configuration domains while
        // constructing that RDD so a temporary best-effort setting cannot be mistaken for a
        // repeatable strict read.
        val materializationConf = SQLConf.get.clone()
        val cachedPlanConf = cachedPlan.conf.clone()

        def hasStrictReads(conf: SQLConf): Boolean = SQLConf.withExistingConf(conf) {
          fileSourceOptions.forall { options =>
            val effectiveOptions = new FileSourceOptions(options)
            !effectiveOptions.ignoreMissingFiles && !effectiveOptions.ignoreCorruptFiles
          }
        }

        val (inputRDD, strictPhysicalReads) = SQLConf.withExistingConf(materializationConf) {
          val result = input
          val fileScans = cachedPlan.collect { case scan: FileSourceScanExec => scan }
          val scansAreStrict = fileScans.size == fileSourceOptions.size && fileScans.forall {
            scan => scan.inputRDD match {
              case fileRDD: FileScanRDD => fileRDD.hasStrictFileReads
              case _ => false
            }
          }
          (result, scansAreStrict)
        }
        hasStrictFileSourceReads = hasStrictFileSourceReads &&
          hasStrictReads(materializationConf) && hasStrictReads(cachedPlanConf) &&
          strictPhysicalReads
        inputRDD
      }
    }

    val cb = try {
      if (supportsColumnarInput) {
        serializer.convertColumnarBatchToCachedBatch(
          buildInputRDD(cachedPlan.executeColumnar()),
          cachedPlan.output,
          storageLevel,
          cachedPlan.conf)
      } else {
        serializer.convertInternalRowToCachedBatch(
          buildInputRDD(cachedPlan.execute()),
          cachedPlan.output,
          storageLevel,
          cachedPlan.conf)
      }
    } catch {
      case e: Throwable if cachedPlan.isInstanceOf[AdaptiveSparkPlanExec] =>
        // SPARK-49982: during RDD execution, AQE will execute all stages except ResultStage. If any
        // failure happen, the failure will be cached and the next SQL cache caller will hit the
        // negative cache. Therefore we need to recache the plan.
        val session = cachedPlan.session
        session.sharedState.cacheManager.recacheByPlan(session, logicalPlan)
        throw e
    }
    // Records one successful partition materialization: this partition's (rows, bytes) keyed by its
    // id. Bound to a local so the task closure below captures only the accumulator, not the
    // enclosing CachedRDDBuilder (whose cachedPlan is not serializable).
    val accumulator = partitionStats
    val cached = cb.mapPartitionsWithIndexInternal { (partitionId, it) =>
      val taskContext = TaskContext.get()
      // This task computes exactly one partition. Tally its totals so the completion listener
      // records them once, keyed by partition id (covering empty-output partitions, which produce
      // no batches).
      var localRows = 0L
      var localBytes = 0L
      var fullyConsumed = false
      taskContext.addTaskCompletionListener[Unit] { context =>
        if (fullyConsumed && !context.isFailed() && !context.isInterrupted()) {
          accumulator.add((partitionId, (localRows, localBytes)))
        }
      }
      new Iterator[CachedBatch] {
        override def hasNext: Boolean = {
          val more = it.hasNext
          if (!more) fullyConsumed = true
          more
        }
        override def next(): CachedBatch = {
          val batch = it.next()
          localBytes += batch.sizeInBytes
          localRows += batch.numRows
          batch
        }
      }
    }.persist(storageLevel)
    cached.setName(cachedName)
    isCachedRDDRepeatable = hasStrictFileSourceReads &&
      cached.outputDeterministicLevel != DeterministicLevel.INDETERMINATE &&
      InMemoryRelation.hasRepeatablePhysicalPlan(cachedPlan)
    cached
  }
}

object InMemoryRelation extends PredicateHelper {

  private val trustedFileFormatClasses: Set[Class[_ <: FileFormat]] = Set(
    classOf[BinaryFileFormat],
    classOf[CSVFileFormat],
    classOf[JsonFileFormat],
    classOf[OrcFileFormat],
    classOf[ParquetFileFormat],
    classOf[TextFileFormat])

  private val trustedExternalFileFormatNames = Set(
    "org.apache.spark.sql.avro.AvroFileFormat",
    "org.apache.spark.sql.hive.orc.OrcFileFormat")

  private def hasSafeExpressions(plan: QueryPlan[_]): Boolean = {
    plan.expressions.forall { expression =>
      !expression.exists {
        case _: AesEncrypt | _: NonSQLExpression | _: UserDefinedExpression => true
        case value => !value.deterministic || value.containsPattern(CURRENT_LIKE) ||
          !value.getClass.getName.startsWith("org.apache.spark.sql.catalyst.expressions.")
      }
    }
  }

  private def hasRepeatableLogicalPlan(analyzedPlan: LogicalPlan, plan: LogicalPlan): Boolean = {
    // Runtime-replaceable expressions such as AES encryption can become deterministic-looking
    // StaticInvoke nodes during optimization despite using a fresh random initialization vector.
    // Inspect the original analyzed expressions before trusting the optimized execution shape.
    analyzedPlan.deterministic && analyzedPlan.collectWithSubqueries {
      case node if !hasSafeExpressions(node) => true
    }.isEmpty && plan.deterministic && plan.collectWithSubqueries {
      case node if !hasSafeExpressions(node) => true
      case _: logical.Project | _: logical.Filter | _: logical.SubqueryAlias |
           _: logical.Range | _: logical.LocalRelation => false
      case relation: LogicalRelation => relation.relation match {
        case fileRelation: HadoopFsRelation =>
          val fileFormatClass = fileRelation.fileFormat.getClass
          !(trustedFileFormatClasses.contains(fileFormatClass) ||
            trustedExternalFileFormatNames.contains(fileFormatClass.getName))
        case _ => true
      }
      case _ => true
    }.forall(!_)
  }

  private[columnar] def hasRepeatablePhysicalPlan(plan: SparkPlan): Boolean = {
    !plan.exists { node =>
      val supported = node match {
        case _: ColumnarToRowExec | _: FileSourceScanExec | _: FilterExec |
             _: InputAdapter | _: LocalTableScanExec | _: ProjectExec |
             _: RangeExec | _: WholeStageCodegenExec => true
        case _ => false
      }
      !supported || node.subqueries.nonEmpty || !hasSafeExpressions(node)
    }
  }

  private def collectFileSourceOptions(plan: LogicalPlan): Seq[Map[String, String]] = {
    val relevantOptions = Seq(
      FileSourceOptions.IGNORE_MISSING_FILES,
      FileSourceOptions.IGNORE_CORRUPT_FILES)
    plan.collectWithSubqueries {
      case relation: LogicalRelation if relation.relation.isInstanceOf[HadoopFsRelation] =>
        val options = CaseInsensitiveMap(relation.relation.asInstanceOf[HadoopFsRelation].options)
        relevantOptions.flatMap { key => options.get(key).map(key -> _) }.toMap
    }
  }

  private def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.collectWithSubqueries {
      case logical.Filter(condition, _) if condition.deterministic &&
          isLikelySelective(condition) => true
    }.nonEmpty
  }

  private def newCacheBuilder(
      serializer: CachedBatchSerializer,
      storageLevel: StorageLevel,
      cachedPlan: SparkPlan,
      tableName: Option[String],
      logicalPlan: LogicalPlan,
      analyzedPlan: LogicalPlan,
      optimizedPlan: LogicalPlan): CachedRDDBuilder = {
    CachedRDDBuilder(
      serializer,
      storageLevel,
      cachedPlan,
      tableName,
      logicalPlan,
      isCachedLogicalPlanRepeatable = hasRepeatableLogicalPlan(analyzedPlan, optimizedPlan),
      hasSelectivePredicate = hasSelectivePredicate(optimizedPlan),
      fileSourceOptions = collectFileSourceOptions(optimizedPlan))
  }

  private[this] var ser: Option[CachedBatchSerializer] = None
  private[this] def getSerializer(sqlConf: SQLConf): CachedBatchSerializer = synchronized {
    if (ser.isEmpty) {
      val serName = sqlConf.getConf(StaticSQLConf.SPARK_CACHE_SERIALIZER)
      val serClass = Utils.classForName(serName)
      val instance = serClass.getConstructor().newInstance().asInstanceOf[CachedBatchSerializer]
      ser = Some(instance)
    }
    ser.get
  }

  /* Visible for testing */
  private[sql] def clearSerializer(): Unit = synchronized { ser = None }

  def apply(
      storageLevel: StorageLevel,
      qe: QueryExecution,
      tableName: Option[String]): InMemoryRelation = {
    val optimizedPlan = qe.optimizedPlan
    val serializer = getSerializer(optimizedPlan.conf)
    val child = if (serializer.supportsColumnarInput(optimizedPlan.output)) {
      serializer.convertToColumnarPlanIfPossible(qe.executedPlan)
    } else {
      qe.executedPlan
    }
    val cacheBuilder = newCacheBuilder(
      serializer, storageLevel, child, tableName, qe.logical, qe.analyzed, optimizedPlan)
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
    val cacheBuilder = newCacheBuilder(
      serializer, storageLevel, child, tableName, optimizedPlan, optimizedPlan, optimizedPlan)
    val relation = new InMemoryRelation(child.output, cacheBuilder, optimizedPlan.outputOrdering)
    relation.statsOfPlanToCache = optimizedPlan.stats
    relation
  }

  def apply(cacheBuilder: CachedRDDBuilder, qe: QueryExecution): InMemoryRelation = {
    val optimizedPlan = qe.optimizedPlan
    val serializer = cacheBuilder.serializer
    val newCachedPlan = if (serializer.supportsColumnarInput(optimizedPlan.output)) {
      serializer.convertToColumnarPlanIfPossible(qe.executedPlan)
    } else {
      qe.executedPlan
    }
    val newBuilder = newCacheBuilder(
      serializer,
      cacheBuilder.storageLevel,
      newCachedPlan,
      cacheBuilder.tableName,
      qe.logical,
      qe.analyzed,
      optimizedPlan)
    val relation = new InMemoryRelation(
      newBuilder.cachedPlan.output, newBuilder, optimizedPlan.outputOrdering)
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
  extends logical.LeafNodeWithAccurateStats with MultiInstanceRelation {

  @volatile var statsOfPlanToCache: Statistics = null

  override def innerChildren: Seq[SparkPlan] = Seq(cachedPlan)

  override def doCanonicalize(): logical.LogicalPlan =
    withOutput(output.map(QueryPlan.normalizeExpressions(_, output)))

  @transient val partitionStatistics = new PartitionStatistics(output)

  def cachedPlan: SparkPlan = cacheBuilder.cachedPlan

  override def statsAvailable: Boolean =
    cacheBuilder.storageLevel.useDisk && cacheBuilder.repeatableMaterializedStats.isDefined

  override def isOutputRepeatable: Boolean = cacheBuilder.repeatableMaterializedStats.isDefined

  override def hasSelectivePredicate: Boolean = cacheBuilder.hasSelectivePredicate

  private[sql] def updateStats(
      rowCount: Long,
      newColStats: Map[Attribute, ColumnStat]): Unit = this.synchronized {
    val newStats = statsOfPlanToCache.copy(
      rowCount = Some(rowCount),
      attributeStats = AttributeMap(statsOfPlanToCache.attributeStats ++ newColStats)
    )
    statsOfPlanToCache = newStats
  }

  override def computeStats(): Statistics = {
    cacheBuilder.loadedMaterializedStats.map { case (rowCount, sizeInBytes) =>
      statsOfPlanToCache.copy(sizeInBytes = sizeInBytes, rowCount = Some(rowCount))
    }.getOrElse {
      // Underlying columnar RDD hasn't been materialized, use the stats from the plan to cache.
      statsOfPlanToCache
    }
  }

  def withOutput(newOutput: Seq[Attribute]): InMemoryRelation = {
    val map = AttributeMap(output.zip(newOutput))
    val newOutputOrdering = outputOrdering
      .map(_.transform { case a: Attribute => map(a) })
      .asInstanceOf[Seq[SortOrder]]
    InMemoryRelation(newOutput, cacheBuilder, newOutputOrdering, statsOfPlanToCache)
  }

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

  override def makeCopy(newArgs: Array[AnyRef]): LogicalPlan = {
    val copied = super.makeCopy(newArgs).asInstanceOf[InMemoryRelation]
    copied.statsOfPlanToCache = this.statsOfPlanToCache
    copied
  }

  override def simpleString(maxFields: Int): String =
    s"InMemoryRelation [${truncatedString(output, ", ", maxFields)}], ${cacheBuilder.storageLevel}"

  override def stringArgs: Iterator[Any] =
    Iterator(output, cacheBuilder.storageLevel, outputOrdering)
}
