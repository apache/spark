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

import org.apache.spark.{InterruptibleIterator, Partition, SparkEnv}
import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{Predicate => GenPredicate, _}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.{ColumnarBatchScan, LeafExecNode, WholeStageCodegenExec}
import org.apache.spark.sql.execution.vectorized._
import org.apache.spark.sql.types._


case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafExecNode with ColumnarBatchScan {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

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
   * If false, get data from UnsafeRow build from ColumnVector
   */
  override val supportCodegen: Boolean = {
    // In the initial implementation, for ease of review
    // support only primitive data types and # of fields is less than wholeStageMaxNumFields
    relation.schema.fields.forall(f => f.dataType match {
      case BooleanType | ByteType | ShortType | IntegerType | LongType |
           FloatType | DoubleType => true
      case _ => false
    }) && !WholeStageCodegenExec.isTooManyFields(conf, relation.schema)
  }

  private val columnIndices =
    attributes.map(a => relation.output.map(o => o.exprId).indexOf(a.exprId)).toArray

  private val relationSchema = relation.schema.toArray

  private lazy val columnarBatchSchema = new StructType(columnIndices.map(i => relationSchema(i)))

  private def createAndDecompressColumn(cachedColumnarBatch: CachedBatch): ColumnarBatch = {
    val rowCount = cachedColumnarBatch.numRows
    val taskContext = Option(TaskContext.get())
    val columnVectors = if (!conf.offHeapColumnVectorEnabled || taskContext.isEmpty) {
      OnHeapColumnVector.allocateColumns(rowCount, columnarBatchSchema)
    } else {
      OffHeapColumnVector.allocateColumns(rowCount, columnarBatchSchema)
    }
    val columnarBatch = new ColumnarBatch(
      columnarBatchSchema, columnVectors.asInstanceOf[Array[ColumnVector]], rowCount)
    columnarBatch.setNumRows(rowCount)

    for (i <- 0 until attributes.length) {
      ColumnAccessor.decompress(
        cachedColumnarBatch.buffers(columnIndices(i)),
        columnarBatch.column(i).asInstanceOf[WritableColumnVector],
        columnarBatchSchema.fields(i).dataType, rowCount)
    }
    taskContext.foreach(_.addTaskCompletionListener(_ => columnarBatch.close()))
    columnarBatch
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    assert(supportCodegen)
    val buffers = filteredCachedBatches()
    // HACK ALERT: This is actually an RDD[ColumnarBatch].
    // We're taking advantage of Scala's type erasure here to pass these batches along.
    Seq(buffers.map(createAndDecompressColumn(_)).asInstanceOf[RDD[InternalRow]])
  }

  override def output: Seq[Attribute] = attributes

  private def updateAttribute(expr: Expression): Expression = {
    // attributes can be pruned so using relation's output.
    // E.g., relation.output is [id, item] but this scan's output can be [item] only.
    val attrMap = AttributeMap(relation.child.output.zip(relation.output))
    expr.transform {
      case attr: Attribute => attrMap.getOrElse(attr, attr)
    }
  }

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputPartitioning: Partitioning = {
    relation.child.outputPartitioning match {
      case h: HashPartitioning => updateAttribute(h).asInstanceOf[HashPartitioning]
      case _ => relation.child.outputPartitioning
    }
  }

  // The cached version does not change the outputOrdering of the original SparkPlan.
  // But the cached version could alias output, so we need to replace output.
  override def outputOrdering: Seq[SortOrder] =
    relation.child.outputOrdering.map(updateAttribute(_).asInstanceOf[SortOrder])

  private def statsFor(a: Attribute) = relation.partitionStatistics.forAttribute(a)

  // Returned filter predicate should return false iff it is impossible for the input expression
  // to evaluate to `true' based on statistics collected about this partition batch.
  @transient val buildFilter: PartialFunction[Expression, Expression] = {
    case And(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) || buildFilter.isDefinedAt(rhs) =>
      (buildFilter.lift(lhs) ++ buildFilter.lift(rhs)).reduce(_ && _)

    case Or(lhs: Expression, rhs: Expression)
      if buildFilter.isDefinedAt(lhs) && buildFilter.isDefinedAt(rhs) =>
      buildFilter(lhs) || buildFilter(rhs)

    case EqualTo(a: AttributeReference, l: Literal) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualTo(l: Literal, a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case EqualNullSafe(a: AttributeReference, l: Literal) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound
    case EqualNullSafe(l: Literal, a: AttributeReference) =>
      statsFor(a).lowerBound <= l && l <= statsFor(a).upperBound

    case LessThan(a: AttributeReference, l: Literal) => statsFor(a).lowerBound < l
    case LessThan(l: Literal, a: AttributeReference) => l < statsFor(a).upperBound

    case LessThanOrEqual(a: AttributeReference, l: Literal) => statsFor(a).lowerBound <= l
    case LessThanOrEqual(l: Literal, a: AttributeReference) => l <= statsFor(a).upperBound

    case GreaterThan(a: AttributeReference, l: Literal) => l < statsFor(a).upperBound
    case GreaterThan(l: Literal, a: AttributeReference) => statsFor(a).lowerBound < l

    case GreaterThanOrEqual(a: AttributeReference, l: Literal) => l <= statsFor(a).upperBound
    case GreaterThanOrEqual(l: Literal, a: AttributeReference) => statsFor(a).lowerBound <= l

    case IsNull(a: Attribute) => statsFor(a).nullCount > 0
    case IsNotNull(a: Attribute) => statsFor(a).count - statsFor(a).nullCount > 0

    case In(a: AttributeReference, list: Seq[Expression])
      if list.forall(_.isInstanceOf[Literal]) && list.nonEmpty =>
      list.map(l => statsFor(a).lowerBound <= l.asInstanceOf[Literal] &&
        l.asInstanceOf[Literal] <= statsFor(a).upperBound).reduce(_ || _)
  }

  val partitionFilters: Seq[Expression] = {
    predicates.flatMap { p =>
      val filter = buildFilter.lift(p)
      val boundFilter =
        filter.map(
          BindReferences.bindReference(
            _,
            relation.partitionStatistics.schema,
            allowFailures = true))

      boundFilter.foreach(_ =>
        filter.foreach(f => logInfo(s"Predicate $p generates partition filter: $f")))

      // If the filter can't be resolved then we are missing required statistics.
      boundFilter.filter(_.resolved)
    }
  }

  lazy val enableAccumulators: Boolean =
    sqlContext.getConf("spark.sql.inMemoryTableScanStatistics.enable", "false").toBoolean

  // Accumulators used for testing purposes
  lazy val readPartitions = sparkContext.longAccumulator
  lazy val readBatches = sparkContext.longAccumulator

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  private def doFilterCachedBatches(
      cachedBatchIterator: Iterator[CachedBatch],
      partitionStatsSchema: Seq[AttributeReference],
      partitionFilter: GenPredicate): Iterator[CachedBatch] = {
    val schemaIndex = partitionStatsSchema.zipWithIndex
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
  }

  private def buildFilteredRDDPartitions(metadataPartitionIds: Seq[Int]): Seq[Partition] = {
    metadataPartitionIds.zipWithIndex.map {
      case (parentPartitionIdx, newPartitionIdx) =>
        new FilteredCachedColumnarPartition(newPartitionIdx, parentPartitionIdx)
    }
  }

  private def filteredCachedBatches(): RDD[CachedBatch] = {
    // Using these variables here to avoid serialization of entire objects (if referenced directly)
    // within the map Partitions closure.
    val schema = relation.partitionStatistics.schema
    val buffers = relation.cachedColumnBuffers

    val metadataOfValidPartitions = CachedColumnarRDD.fetchMetadataForRDD(buffers.id).map {
      metadata =>
        metadata.zipWithIndex.
          filter { case (m, partitionIndex) =>
            val partitionFilter = newPredicate(
              partitionFilters.reduceOption(And).getOrElse(Literal(true)),
              schema)
            partitionFilter.initialize(partitionIndex)
            partitionFilter.eval(m)
          }.map(_._2)
    }.getOrElse(buffers.partitions.indices)

    new FilteredCachedColumnarRDD(buffers.sparkContext, buffers.asInstanceOf[CachedColumnarRDD],
      buildFilteredRDDPartitions(metadataOfValidPartitions))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    if (enableAccumulators) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    // Using these variables here to avoid serialization of entire objects (if referenced directly)
    // within the map Partitions closure.
    val relOutput: AttributeSeq = relation.output

    filteredCachedBatches().mapPartitionsInternal { cachedBatchIterator =>
      // Find the ordinals and data types of the requested columns.
      val (requestedColumnIndices, requestedColumnDataTypes) =
        attributes.map { a =>
          relOutput.indexOf(a.exprId) -> a.dataType
        }.unzip

      // update SQL metrics
      val withMetrics = cachedBatchIterator.map { batch =>
        if (enableAccumulators) {
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
      if (enableAccumulators && columnarIterator.hasNext) {
        readPartitions.add(1)
      }
      columnarIterator
    }
  }
}
