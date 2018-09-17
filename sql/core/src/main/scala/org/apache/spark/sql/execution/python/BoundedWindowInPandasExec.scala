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

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

case class BoundedWindowInPandasExec(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan
) extends WindowExecBase(windowExpression, partitionSpec, orderSpec, child) {

  override def output: Seq[Attribute] =
    child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MB?
      logWarning("No Partition Defined for Window operation! Moving all data to a single "
        + "partition, this can cause serious performance degradation.")
      AllTuples :: Nil
    } else {
      ClusteredDistribution(partitionSpec) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(partitionSpec.map(SortOrder(_, Ascending)) ++ orderSpec)

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  private def collectFunctions(udf: PythonUDF): (ChainedPythonFunctions, Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonUDF) =>
        val (chained, children) = collectFunctions(u)
        (ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(_.find(_.isInstanceOf[PythonUDF]).isEmpty))
        (ChainedPythonFunctions(Seq(udf.func)), udf.children)
    }
  }


  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    def beginOffsetIndex(frameIndex: Int) = frameIndex * 2
    def endOffsetIndex(frameIndex: Int) = frameIndex * 2 + 1

    // Unwrap the expressions and factories from the map.
    val expressionsWithFrameIndex =
      windowFrameExpressionFactoryPairs.map(_._1).zipWithIndex.flatMap {
        case (buffer, frameIndex) => buffer.map( expr => (expr, frameIndex))
      }

    val expressions = expressionsWithFrameIndex.map(_._1)
    val expressionIndexToFrameIndex =
      expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap

    val factories = windowFrameExpressionFactoryPairs.map(_._2).toArray

    val numFrames = factories.length

    val inMemoryThreshold = sqlContext.conf.windowExecBufferInMemoryThreshold
    val spillThreshold = sqlContext.conf.windowExecBufferSpillThreshold

    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

    // Extract window expressions and window functions
    val windowExpressions = expressions.flatMap(_.collect { case e: WindowExpression => e })
    val udfExpressions = windowExpressions.map(_.windowFunction.asInstanceOf[PythonUDF])

    // We shouldn't be chaining anything here.
    // All chained python functions should only contain one function.
    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip
    require(pyFuncs.length == expressions.length)

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs.
    val dataInputs = new ArrayBuffer[Expression]
    val dataInputTypes = new ArrayBuffer[DataType]
    val argOffsets = inputs.map { input =>
      input.map { e =>
        if (dataInputs.exists(_.semanticEquals(e))) {
          dataInputs.indexWhere(_.semanticEquals(e))
        } else {
          dataInputs += e
          dataInputTypes += e.dataType
          dataInputs.length - 1
        }
      }.toArray
    }.toArray

    // Add window indices to allInputs, dataTypes and argOffsets
    // We also deduplicate window indices so window frames that are identical can
    // share the same window index columns
    val indiceInputs = factories.indices.flatMap {frameIndex =>
      Seq(
        BoundReference(beginOffsetIndex(frameIndex), IntegerType, nullable = false),
        BoundReference(endOffsetIndex(frameIndex), IntegerType, nullable = false)
      )
    }

    pyFuncs.indices.foreach { exprIndex =>
      val frameIndex = expressionIndexToFrameIndex(exprIndex)
      argOffsets(exprIndex) =
        Array(beginOffsetIndex(frameIndex), endOffsetIndex(frameIndex)) ++
          argOffsets(exprIndex).map(_ + indiceInputs.length)
    }

    val allInputs = indiceInputs ++ dataInputs
    val allInputTypes = allInputs.map(_.dataType)

    // Schema of input rows to the python runner
    val windowInputSchema = StructType(
      allInputTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }
    )
    // Start processing.
    child.execute().mapPartitions { iter =>
      val context = TaskContext.get()

      // Get all relevant projections.
      val resultProj = createResultProjection(expressions)
      val pythonInputProj = UnsafeProjection.create(
        allInputs,
        indiceInputs.map(ref => AttributeReference("i", ref.dataType)()) ++ child.output
      )
      val grouping = UnsafeProjection.create(partitionSpec, child.output)

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), child.output.length)
      context.addTaskCompletionListener[Unit] { _ =>
        queue.close()
      }

      val stream = iter.map { row =>
        queue.add(row.asInstanceOf[UnsafeRow])
        row
      }

      val pythonInput = new Iterator[Iterator[UnsafeRow]] {

        // Manage the stream and the grouping.
        var nextRow: UnsafeRow = null
        var nextGroup: UnsafeRow = null
        var nextRowAvailable: Boolean = false
        private[this] def fetchNextRow() {
          nextRowAvailable = stream.hasNext
          if (nextRowAvailable) {
            nextRow = stream.next().asInstanceOf[UnsafeRow]
            nextGroup = grouping(nextRow)
          } else {
            nextRow = null
            nextGroup = null
          }
        }
        fetchNextRow()

        // Manage the current partition.
        val buffer: ExternalAppendOnlyUnsafeRowArray =
          new ExternalAppendOnlyUnsafeRowArray(inMemoryThreshold, spillThreshold)
        var bufferIterator: Iterator[UnsafeRow] = _

        val indexRow = new SpecificInternalRow(Array.fill(numFrames * 2)(IntegerType))

        val frames = factories.map(_(indexRow))

        private[this] def fetchNextPartition() {
          // Collect all the rows in the current partition.
          // Before we start to fetch new input rows, make a copy of nextGroup.
          val currentGroup = nextGroup.copy()

          // clear last partition
          buffer.clear()

          while (nextRowAvailable && nextGroup == currentGroup) {
            buffer.add(nextRow)
            fetchNextRow()
          }

          // Setup the frames.
          var i = 0
          while (i < numFrames) {
            frames(i).prepare(buffer)
            i += 1
          }

          // Setup iteration
          rowIndex = 0
          bufferIterator = buffer.generateIterator()
        }

        // Iteration
        var rowIndex = 0

        override final def hasNext: Boolean =
          (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable

        override final def next(): Iterator[UnsafeRow] = {
          // Load the next partition if we need to.
          if ((bufferIterator == null || !bufferIterator.hasNext) && nextRowAvailable) {
            fetchNextPartition()
          }

          val join = new JoinedRow

          bufferIterator.zipWithIndex.map {
            case (current, index) =>
              var frameIndex = 0
              while (frameIndex < numFrames) {
                frames(frameIndex).write(index, current)
                indexRow.setInt(beginOffsetIndex(frameIndex), frames(frameIndex).currentLowIndex())
                indexRow.setInt(endOffsetIndex(frameIndex), frames(frameIndex).currentHighIndex())
                frameIndex += 1
              }

              val r = pythonInputProj(join(indexRow, current))
              r
          }
        }
      }

      val windowFunctionResult = new ArrowPythonRunner(
        pyFuncs,
        bufferSize,
        reuseWorker,
        PythonEvalType.SQL_BOUNDED_WINDOW_AGG_PANDAS_UDF,
        argOffsets,
        windowInputSchema,
        sessionLocalTimeZone,
        pythonRunnerConf).compute(pythonInput, context.partitionId(), context)

      val joined = new JoinedRow

      windowFunctionResult.flatMap(_.rowIterator.asScala).map { windowOutput =>
        val leftRow = queue.remove()
        val joinedRow = joined(leftRow, windowOutput)
        resultProj(joinedRow)
      }
    }
  }
}
