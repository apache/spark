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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{CodegenSupport, LeafExecNode, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.execution.vectorized.ColumnVector
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._


case class InMemoryTableScanExec(
    attributes: Seq[Attribute],
    predicates: Seq[Expression],
    @transient relation: InMemoryRelation)
  extends LeafExecNode {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(relation) ++ super.innerChildren

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = attributes

  // The cached version does not change the outputPartitioning of the original SparkPlan.
  override def outputPartitioning: Partitioning = relation.child.outputPartitioning

  // The cached version does not change the outputOrdering of the original SparkPlan.
  override def outputOrdering: Seq[SortOrder] = relation.child.outputOrdering

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

    case In(a: AttributeReference, list: Seq[Expression]) if list.forall(_.isInstanceOf[Literal]) =>
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

  def incrementReadPartitionAccumulator(): Unit = {
    readPartitions.add(1)
  }

  private val inMemoryPartitionPruningEnabled = sqlContext.conf.inMemoryPartitionPruning

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    if (enableAccumulators) {
      readPartitions.setValue(0)
      readBatches.setValue(0)
    }

    // Using these variables here to avoid serialization of entire objects (if referenced directly)
    // within the map Partitions closure.
    val schema = relation.partitionStatistics.schema
    val schemaIndex = schema.zipWithIndex
    val relOutput: AttributeSeq = relation.output
    val buffers = relation.cachedColumnBuffers

    buffers.mapPartitionsInternal { cachedBatchIterator =>
      val partitionFilter = newPredicate(
        partitionFilters.reduceOption(And).getOrElse(Literal(true)),
        schema)

      // Find the ordinals and data types of the requested columns.
      val (requestedColumnIndices, requestedColumnDataTypes) =
        attributes.map { a =>
          relOutput.indexOf(a.exprId) -> a.dataType
        }.unzip

      // Do partition batch pruning if enabled
      val cachedBatchesToScan =
        if (inMemoryPartitionPruningEnabled) {
          cachedBatchIterator.filter { cachedBatch =>
            if (!partitionFilter(cachedBatch.stats)) {
              def statsString: String = schemaIndex.map {
                case (a, i) =>
                  val value = cachedBatch.stats.get(i, a.dataType)
                  s"${a.name}: $value"
              }.mkString(", ")
              logInfo(s"Skipping partition based on stats $statsString")
              false
            } else {
              true
            }
          }
        } else {
          cachedBatchIterator
        }

      // update SQL metrics
      val withMetrics = cachedBatchesToScan.map { batch =>
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
      columnarIterator.initialize(withMetrics, columnTypes, requestedColumnIndices.toArray,
        if (!enableAccumulators) null else this)
      if (enableAccumulators && !columnarIterator.isSupportColumnarCodeGen &&
        columnarIterator.hasNext) {
        incrementReadPartitionAccumulator
      }
      columnarIterator
    }
  }
}

private[sql] object InMemoryTableScanExec {
  private val columnarItrName = "columnar_itr"
  private val columnarBatchIdxName = "columnar_batchIdx"

  def enableColumnCodeGen(
     sqlContext: SQLContext, ctx: CodegenContext, child: SparkPlan): Boolean = {
    ctx.enableColumnCodeGen &&
      sqlContext.getConf(SQLConf.COLUMN_VECTOR_CODEGEN.key).toBoolean &&
      child.find(c => c.isInstanceOf[InMemoryTableScanExec]).isDefined &&
      child.find(c => c.isInstanceOf[CodegenSupport] &&
        c.asInstanceOf[CodegenSupport].useUnsafeRow).isEmpty
  }

  def produceColumnLoop(
      ctx: CodegenContext, codegen: CodegenSupport, output: Seq[Attribute]): String = {
    val idx = columnarBatchIdxName
    val numRows = "columnar_numRows"
    ctx.addMutableState("int", idx, s"$idx = 0;")
    ctx.addMutableState("int", numRows, s"$numRows = 0;")
    val rowidx = ctx.freshName("rowIdx")

    val colVars = output.indices.map(i => ctx.freshName("col" + i))
    val columnAssigns = colVars.zipWithIndex.map { case (name, i) =>
      ctx.addMutableState("org.apache.spark.sql.execution.vectorized.ColumnVector",
        name, s"$name = null;", s"$name = null;")
      s"$name = ${columnarItrName}.getColumn($i);"
    }
    val columns = (output zip colVars).map { case (attr, colVar) =>
      new ColumnVectorReference(colVar, rowidx, attr.dataType, attr.nullable).genCode(ctx) }

    s"""
       | while (true) {
       |   if ($idx == 0) {
       |     $numRows = ${columnarItrName}.initForColumnar();
       |     if ($numRows < 0) {
       |       cleanup();
       |       break;
       |     }
       |     ${columnAssigns.mkString("", "\n", "")}
       |   }
       |
       |   while ($idx < $numRows) {
       |     int $rowidx = $idx++;
       |     ${codegen.consume(ctx, columns, null).trim}
       |     if (shouldStop()) return;
       |   }
       |   $idx = 0;
       | }
      """.stripMargin
  }

  def produceProcessNext(
      ctx: CodegenContext, codegen: CodegenSupport, child: SparkPlan, codeRow: String): String = {
    ctx.isRow = false
    val codeCol = child.asInstanceOf[CodegenSupport].produce(ctx, codegen)
    val columnarItrClz = "org.apache.spark.sql.execution.columnar.ColumnarIterator"
    val colItr = columnarItrName
    ctx.addMutableState(s"$columnarItrClz", colItr, s"$colItr = null;", s"$colItr = null;")

    s"""
      private void processBatch() throws java.io.IOException {
        ${codeCol.trim}
      }

      private void processRow() throws java.io.IOException {
        ${codeRow.trim}
      }

      private void cleanup() {
        ${ctx.cleanupMutableStates()}
      }

      protected void processNext() throws java.io.IOException {
        if ((${columnarBatchIdxName} != 0) ||
            (${ctx.iteratorInput} instanceof $columnarItrClz &&
             ($colItr = ($columnarItrClz)${ctx.iteratorInput}).isSupportColumnarCodeGen())) {
          processBatch();
        } else {
          processRow();
        }
      }
     """.trim
  }
}
