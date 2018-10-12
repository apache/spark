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

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, SparkPlan, WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnarBatch, ColumnVector}


case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafExecNode with ColumnarBatchScan {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  override def doCanonicalize(): SparkPlan =
    copy(attributes = attributes.map(QueryPlan.normalizeExprId(_, relation.output)),
      predicates = predicates.map(QueryPlan.normalizeExprId(_, relation.output)),
      relation = relation.canonicalized.asInstanceOf[InMemoryRelation])

  override def vectorTypes: Option[Seq[String]] =
    Option(Seq.fill(attributes.length)(
      if (!conf.offHeapColumnVectorEnabled) {
        classOf[OnHeapColumnVector].getName
      } else {
        classOf[OffHeapColumnVector].getName
      }
    ))

  /**
   * If true, get data from ColumnVector in ColumnarBatch, which are generally faster.
   * If false, get data from UnsafeRow build from CachedBatch
   */
  override val supportsBatch: Boolean = {
    // In the initial implementation, for ease of review
    // support only primitive data types and # of fields is less than wholeStageMaxNumFields
    conf.cacheVectorizedReaderEnabled && relation.schema.fields.forall(f => f.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType => true
      case _ => false
    }) && !WholeStageCodegenExec.isTooManyFields(conf, relation.schema)
  }

  // TODO: revisit this. Shall we always turn off whole stage codegen if the output data are rows?
  override def supportCodegen: Boolean = supportsBatch

  override protected def needsUnsafeRowConversion: Boolean = false

  private val columnIndices =
    attributes.map(a => relation.output.map(o => o.exprId).indexOf(a.exprId)).toArray

  private val relationSchema = relation.schema.toArray

  private lazy val columnarBatchSchema = new StructType(columnIndices.map(i => relationSchema(i)))

  private def createAndDecompressColumn(
      cachedColumnarBatch: CachedBatch,
      offHeapColumnVectorEnabled: Boolean): ColumnarBatch = {
    val rowCount = cachedColumnarBatch.numRows
    val taskContext = Option(TaskContext.get())
    val columnVectors = if (!offHeapColumnVectorEnabled || taskContext.isEmpty) {
      OnHeapColumnVector.allocateColumns(rowCount, columnarBatchSchema)
    } else {
      OffHeapColumnVector.allocateColumns(rowCount, columnarBatchSchema)
    }
    val columnarBatch = new ColumnarBatch(columnVectors.asInstanceOf[Array[ColumnVector]])
    columnarBatch.setNumRows(rowCount)

    for (i <- attributes.indices) {
      ColumnAccessor.decompress(
        cachedColumnarBatch.buffers(columnIndices(i)),
        columnarBatch.column(i).asInstanceOf[WritableColumnVector],
        columnarBatchSchema.fields(i).dataType, rowCount)
    }
    taskContext.foreach(_.addTaskCompletionListener[Unit](_ => columnarBatch.close()))
    columnarBatch
  }

  private lazy val inputRDD: RDD[InternalRow] = {
    val buffers = filteredCachedBatches()
    val offHeapColumnVectorEnabled = conf.offHeapColumnVectorEnabled
    if (supportsBatch) {
      // HACK ALERT: This is actually an RDD[ColumnarBatch].
      // We're taking advantage of Scala's type erasure here to pass these batches along.
      buffers
        .map(createAndDecompressColumn(_, offHeapColumnVectorEnabled))
        .asInstanceOf[RDD[InternalRow]]
    } else {
      val numOutputRows = longMetric("numOutputRows")

      if (enableAccumulatorsForTest) {
        readPartitions.setValue(0)
        readBatches.setValue(0)
      }

      // Using these variables here to avoid serialization of entire objects (if referenced
      // directly) within the map Partitions closure.
      val relOutput: AttributeSeq = relation.output

      filteredCachedBatches().mapPartitionsInternal { cachedBatchIterator =>
        // Find the ordinals and data types of the requested columns.
        val (requestedColumnIndices, requestedColumnDataTypes) =
          attributes.map { a =>
            relOutput.indexOf(a.exprId) -> a.dataType
          }.unzip

        // update SQL metrics
        val withMetrics = cachedBatchIterator.map { batch =>
          if (enableAccumulatorsForTest) {
            readBatches.add(1)
          }
          numOutputRows += batch.numRows
          batch
        }

        val columnTypes = requestedColumnDataTypes.map {
          case udt: UserDefinedType[_] => udt.sqlType
          case other => other
        }.toArray
        val columnarIterator = GenerateColumnAccessor.generate(columnTypes)
        columnarIterator.initialize(withMetrics, columnTypes, requestedColumnIndices.toArray)
        if (enableAccumulatorsForTest && columnarIterator.hasNext) {
          readPartitions.add(1)
        }
        columnarIterator
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = Seq(inputRDD)

  override def output: Seq[Attribute] = attributes

  private def updateAttribute(expr: Expression): Expression = {
    // attributes can be pruned so using relation's output.
    // E.g., relation.output is [id, item] but this scan's output can be [item] only.
    val attrMap = AttributeMap(relation.cachedPlan.output.zip(relation.output))
    expr.transform {
      case attr: Attribute => attrMap.getOrElse(attr, attr)
    }
  }

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputPartitioning: Partitioning = {
    relation.cachedPlan.outputPartitioning match {
      case e: Expression => updateAttribute(e).asInstanceOf[Partitioning]
      case other => other
    }
  }

  // The cached version does not change the outputOrdering of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputOrdering: Seq[SortOrder] =
    relation.cachedPlan.outputOrdering.map(updateAttribute(_).asInstanceOf[SortOrder])

  // Keeps relation's partition statistics because we don't serialize relation.
  private val stats = relation.partitionStatistics
  private def statsFor(a: Attribute) = stats.forAttribute(a)

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

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
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
  }

  lazy val partitionFilters: Seq[Expression] = {
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            stats.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  lazy val enableAccumulatorsForTest: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  // Accumulators used for testing purposes
  lazy val readPartitions = sparkContext.longAccumulator
  lazy val readBatches = sparkContext.longAccumulator

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  private def filteredCachedBatches(): RDD[CachedBatch] = {
    // Using these variables here to avoid serialization of entire objects (if referenced directly)
    // within the map Partitions closure.
    val schema = stats.schema
    val schemaIndex = schema.zipWithIndex
    val buffers = relation.cacheBuilder.cachedColumnBuffers

    buffers.mapPartitionsWithIndexInternal { (index, cachedBatchIterator) =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        schema)
      partitionFilter.initialize(index)

      // Do partition batch pruning if enabled
      if (inMemoryPartitionPruningEnabled) {
        cachedBatchIterator.filter { cachedBatch =>
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
      } else {
        cachedBatchIterator
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    if (supportsBatch) {
      WholeStageCodegenExec(this)(codegenStageId = 0).execute()
    } else {
      inputRDD
    }
  }
}
