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

package org.apache.spark.sql.execution.python

import java.io.File

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._

import org.apache.spark.{JobArtifactSet, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * Execution logic for the post-shuffle FINAL stage of an incremental Python aggregation (see
 * [[org.apache.spark.sql.catalyst.expressions.PythonAggregate]]). It groups the (shuffled) child
 * rows by the grouping expressions via a local sort + [[GroupedIterator]], sends each group's
 * intermediate-buffer columns to the Python worker as Arrow record batches, and joins the single
 * result row the worker returns per group back with the grouping key.
 *
 * The map-side PARTIAL stage does not extend this base: it hash-combines many groups per batch
 * inside the worker and needs no sort (see [[PythonIncrementalAggregatePartialExec]]). This base is
 * kept as an abstract class so the per-stage inputs, eval type, output attributes and final
 * projection stay explicit hooks.
 */
abstract class PythonIncrementalAggregateExecBase extends UnaryExecNode with PythonSQLMetrics {

  def groupingExpressions: Seq[NamedExpression]
  def aggExpressions: Seq[AggregateExpression]

  protected val udfExpressions: Seq[PythonAggregate] =
    aggExpressions.map(_.aggregateFunction.asInstanceOf[PythonAggregate])

  /** The Python eval type for this stage. */
  protected def evalType: Int

  /** Per-UDF input expressions to project out of the child and send to the Python worker. */
  protected def udfInputs: Seq[Seq[Expression]]

  /** Attributes of the row the Python worker returns per group (right side of the join). */
  protected def pythonOutputAttributes: Seq[Attribute]

  /** Expressions producing this operator's output from (groupingKey ++ pythonOutput). */
  protected def outputExpressions: Seq[NamedExpression]

  /** The grouping attributes as seen in the child's output. */
  protected def groupingAttributes: Seq[Attribute] = groupingExpressions.map(_.toAttribute)

  /**
   * Whether to still invoke Python on an empty partition. Only the FINAL stage of a *global*
   * (no grouping) aggregation sets this: it must emit the identity row `finish(zero)` for empty
   * input, matching SQL aggregate semantics. Everywhere else an empty partition yields no rows.
   */
  protected def emitOnEmptyPartition: Boolean = false

  override def output: Seq[Attribute] = outputExpressions.map(_.toAttribute)

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingExpressions.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)

    val pyFuncs = udfExpressions.map { u =>
      (ChainedPythonFunctions(Seq(u.func)), u.resultId.id)
    }

    // Filter child output attributes down to only those that are UDF inputs, and eliminate
    // duplicates, mirroring ArrowAggregatePythonExec.
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    val argMetas = PythonIncrementalAggregateExec.buildArgMetas(udfInputs, allInputs, dataTypes)

    val aggInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    }.toArray)

    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
    val sessionUUID = Option(session).collect {
      case s if s.sessionState.conf.pythonWorkerLoggingEnabled => s.sessionUUID
    }

    val groupingExprs = groupingExpressions
    val childOutput = child.output
    val joinedAttributes = groupingAttributes ++ pythonOutputAttributes
    val resultExprs = outputExpressions
    val localEvalType = evalType

    val emitIdentityOnEmpty = emitOnEmptyPartition
    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty && !emitIdentityOnEmpty) iter else {
      val prunedProj = UnsafeProjection.create(allInputs.toSeq, childOutput)

      val groupedItr = if (groupingExprs.isEmpty) {
        Iterator((new UnsafeRow(), iter))
      } else {
        GroupedIterator(iter, groupingExprs, childOutput)
      }

      // For a global aggregation with empty input, feed one all-null buffer row so the Python
      // worker still emits `finish(zero)`. An empty group cannot be sent through
      // GroupedPythonArrowInput (it asserts a non-empty batch per group), and the worker treats a
      // null partial buffer as contributing nothing to `merge`.
      lazy val nullInputRow: UnsafeRow =
        UnsafeProjection.create(aggInputSchema.map(_.dataType).toArray)
          .apply(new GenericInternalRow(aggInputSchema.length)).copy()
      val grouped = groupedItr.map { case (key, rows) =>
        val projected = rows.map(prunedProj)
        val toSend = if (emitIdentityOnEmpty && groupingExprs.isEmpty && !projected.hasNext) {
          Iterator(nullInputRow)
        } else {
          projected
        }
        (key, toSend)
      }

      val context = TaskContext.get()

      // In pipelined mode the queue's add() runs in the writer thread and remove() in the task
      // thread; use lock-free mode to skip per-row synchronization (as ArrowAggregatePythonExec).
      val pipelined = SparkEnv.get.conf.get(PYTHON_UDF_PIPELINED_EXECUTION)
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), groupingExprs.length, lockFree = pipelined)
      context.addTaskCompletionListener[Unit] { _ => queue.close() }

      val projectedRowIter = grouped.map { case (groupingKey, rows) =>
        queue.add(groupingKey.asInstanceOf[UnsafeRow])
        rows
      }

      val runner = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs,
        localEvalType,
        argMetas,
        aggInputSchema,
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        sessionUUID) with GroupedPythonArrowInput

      val columnarBatchIter = runner.compute(projectedRowIter, context.partitionId(), context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(resultExprs, joinedAttributes)

      columnarBatchIter.map(_.rowIterator.next()).map { pythonOutputRow =>
        val leftRow = queue.remove()
        resultProj(joined(leftRow, pythonOutputRow))
      }
    }}
  }
}

/**
 * Map-side PARTIAL stage: hash-combines the input rows of each group into a per-group intermediate
 * buffer via the aggregator's `reduce`, inside the Python worker. Unlike the FINAL stage it needs
 * neither a clustered distribution nor an ordering: the whole point of the map-side combine is to
 * avoid a full pre-shuffle sort. It streams ordinary (multi-group) Arrow batches to the worker,
 * which maintains one running buffer per distinct grouping key and emits, at end of partition, one
 * row per key -- the grouping key columns followed by one intermediate-buffer struct column per
 * aggregator.
 *
 * Because keys may be split across partitions (no shuffle here) and the worker's map-side grouping
 * is only a combine optimization, correctness does not depend on it being exhaustive:
 * [[PythonIncrementalAggregateFinalExec]] re-groups the emitted partial buffers authoritatively
 * (by JVM `UnsafeRow` key, after the shuffle) and merges any that share a key.
 */
case class PythonIncrementalAggregatePartialExec(
    groupingExpressions: Seq[NamedExpression],
    aggExpressions: Seq[AggregateExpression],
    bufferAttributes: Seq[Attribute],
    child: SparkPlan) extends UnaryExecNode with PythonSQLMetrics {

  private val udfExpressions: Seq[PythonAggregate] =
    aggExpressions.map(_.aggregateFunction.asInstanceOf[PythonAggregate])

  private def groupingAttributes: Seq[Attribute] = groupingExpressions.map(_.toAttribute)

  override def output: Seq[Attribute] = groupingAttributes ++ bufferAttributes

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = Seq(UnspecifiedDistribution)

  // No ordering: the worker hash-combines rather than relying on grouped input.
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(Nil)

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val largeVarTypes = conf.arrowUseLargeVarTypes
    val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)

    val pyFuncs = udfExpressions.map { u =>
      (ChainedPythonFunctions(Seq(u.func)), u.resultId.id)
    }

    // The columns sent to Python are the grouping keys first (so the worker can hash-group by them
    // and echo them back with each partial buffer), followed by the deduplicated aggregator input
    // columns. A UDF input that coincides with a grouping key simply reuses that leading column.
    val allInputs = new ArrayBuffer[Expression]
    val dataTypes = new ArrayBuffer[DataType]
    groupingExpressions.foreach { g =>
      allInputs += g
      dataTypes += g.dataType
    }
    val numGroupingKeys = groupingExpressions.length

    val argMetas = PythonIncrementalAggregateExec.buildArgMetas(
      udfExpressions.map(_.children), allInputs, dataTypes)

    val aggInputSchema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
      StructField(s"_$i", dt)
    }.toArray)
    // The leading `numGroupingKeys` columns are the grouping keys; hand their schema to the worker.
    val groupingKeySchemaJson = StructType(aggInputSchema.fields.take(numGroupingKeys)).json

    val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)
    val sessionUUID = Option(session).collect {
      case s if s.sessionState.conf.pythonWorkerLoggingEnabled => s.sessionUUID
    }

    val childOutput = child.output
    val outputAttrs = output

    inputRDD.mapPartitionsInternal { iter => if (iter.isEmpty) Iterator.empty else {
      val prunedProj = UnsafeProjection.create(allInputs.toSeq, childOutput)
      val projectedRowIter = iter.map(prunedProj)

      val context = TaskContext.get()

      val runner = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs,
        PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_PARTIAL_UDF,
        argMetas,
        aggInputSchema,
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        sessionUUID) with BatchedPythonArrowInput {
        // Tell the worker how many leading columns are grouping keys, so it can hash-group by them
        // and re-emit them alongside each partial buffer.
        override protected def evalConf: Map[String, String] =
          super.evalConf + ("grouping_key_schema" -> groupingKeySchemaJson)
      }

      val columnarBatchIter = runner.compute(
        Iterator(projectedRowIter), context.partitionId(), context)

      // Each batch the worker returns holds (grouping key columns ++ one buffer struct column per
      // aggregator), i.e. this operator's output columns; copy each row out as an UnsafeRow.
      val resultProj = UnsafeProjection.create(outputAttrs, outputAttrs)
      columnarBatchIter.flatMap(_.rowIterator().asScala).map(resultProj)
    }}
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

/**
 * Post-shuffle FINAL stage: clusters the partial buffers by the grouping key, merges the buffers
 * of each group via the aggregator's `merge`, and produces the output via `finish`. Its input is
 * the [[PythonIncrementalAggregatePartialExec]] output (grouping key columns followed by the
 * intermediate-buffer columns); it sends the buffer columns to Python and outputs
 * `resultExpressions`.
 */
case class PythonIncrementalAggregateFinalExec(
    groupingExpressions: Seq[NamedExpression],
    aggExpressions: Seq[AggregateExpression],
    bufferAttributes: Seq[Attribute],
    resultExpressions: Seq[NamedExpression],
    child: SparkPlan) extends PythonIncrementalAggregateExecBase {

  override protected def evalType: Int =
    PythonEvalType.SQL_GROUPED_AGG_ARROW_INCREMENTAL_FINAL_UDF

  // Each aggregator reads its own intermediate-buffer column from the (shuffled) child.
  override protected def udfInputs: Seq[Seq[Expression]] = bufferAttributes.map(Seq(_))

  override protected def pythonOutputAttributes: Seq[Attribute] =
    aggExpressions.map(_.resultAttribute)

  override protected def outputExpressions: Seq[NamedExpression] = resultExpressions

  // A global (no-grouping) aggregation must return the identity row even for empty input. This
  // stage runs on a single partition (AllTuples), so exactly one identity row is produced.
  override protected def emitOnEmptyPartition: Boolean = groupingExpressions.isEmpty

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingExpressions.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingExpressions) :: Nil
    }
  }

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

object PythonIncrementalAggregateExec {

  /**
   * Deduplicates the per-aggregator input expressions into a shared column list (matched by
   * [[Expression.semanticEquals]]), returning one [[ArgumentMetadata]] array per aggregator that
   * points into that list. `allInputs`/`dataTypes` may be pre-seeded -- the PARTIAL stage prepends
   * its grouping-key columns so a UDF input equal to a grouping key reuses that leading column --
   * and any newly seen columns are appended to them in place. Shared by both stages (and mirrors
   * the same dedup in [[ArrowAggregatePythonExec]]).
   */
  private[python] def buildArgMetas(
      udfInputs: Seq[Seq[Expression]],
      allInputs: ArrayBuffer[Expression],
      dataTypes: ArrayBuffer[DataType]): Array[Array[ArgumentMetadata]] = {
    udfInputs.map { input =>
      input.map { e =>
        val (key, value) = e match {
          case NamedArgumentExpression(key, value) => (Some(key), value)
          case _ => (None, e)
        }
        if (allInputs.exists(_.semanticEquals(value))) {
          ArgumentMetadata(allInputs.indexWhere(_.semanticEquals(value)), key)
        } else {
          allInputs += value
          dataTypes += value.dataType
          ArgumentMetadata(allInputs.length - 1, key)
        }
      }.toArray
    }.toArray
  }

  /**
   * Builds the two-stage physical plan (PARTIAL -> [Exchange, inserted by EnsureRequirements] ->
   * FINAL) for a logical aggregation whose aggregate functions are all [[PythonAggregate]].
   */
  def plan(
      groupingExpressions: Seq[NamedExpression],
      aggExpressions: Seq[AggregateExpression],
      resultExpressions: Seq[NamedExpression],
      child: SparkPlan): SparkPlan = {
    // One intermediate-buffer attribute per aggregator, threaded from the PARTIAL output into the
    // FINAL inputs (matched by expression id).
    val bufferAttributes = aggExpressions.map { ae =>
      val agg = ae.aggregateFunction.asInstanceOf[PythonAggregate]
      AttributeReference(s"buf_${agg.resultId.id}", agg.bufferSchema, nullable = true)()
    }
    val partial = PythonIncrementalAggregatePartialExec(
      groupingExpressions, aggExpressions, bufferAttributes, child)
    // After the PARTIAL stage the grouping expressions are materialized as plain attributes.
    val groupingAttributes = groupingExpressions.map(_.toAttribute)
    PythonIncrementalAggregateFinalExec(
      groupingAttributes, aggExpressions, bufferAttributes, resultExpressions, partial)
  }
}
