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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{ExternalAppendOnlyUnsafeRowArray, SparkPlan}
import org.apache.spark.sql.execution.window._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.Utils

/**
 * This class calculates and outputs windowed aggregates over the rows in a single partition.
 *
 * This is similar to [[WindowExec]]. The main difference is that this node does not compute
 * any window aggregation values. Instead, it computes the lower and upper bound for each window
 * (i.e. window bounds) and pass the data and indices to Python worker to do the actual window
 * aggregation.
 *
 * It currently materializes all data associated with the same partition key and passes them to
 * Python worker. This is not strictly necessary for sliding windows and can be improved (by
 * possibly slicing data into overlapping chunks and stitching them together).
 *
 * This class groups window expressions by their window boundaries so that window expressions
 * with the same window boundaries can share the same window bounds. The window bounds are
 * prepended to the data passed to the python worker.
 *
 * For example, if we have:
 *     avg(v) over specifiedwindowframe(RowFrame, -5, 5),
 *     avg(v) over specifiedwindowframe(RowFrame, UnboundedPreceding, UnboundedFollowing),
 *     avg(v) over specifiedwindowframe(RowFrame, -3, 3),
 *     max(v) over specifiedwindowframe(RowFrame, -3, 3)
 *
 * The python input will look like:
 * (lower_bound_w1, upper_bound_w1, lower_bound_w3, upper_bound_w3, v)
 *
 * where w1 is specifiedwindowframe(RowFrame, -5, 5)
 *       w2 is specifiedwindowframe(RowFrame, UnboundedPreceding, UnboundedFollowing)
 *       w3 is specifiedwindowframe(RowFrame, -3, 3)
 *
 * Note that w2 doesn't have bound indices in the python input because it's unbounded window
 * so it's bound indices will always be the same.
 *
 * Bounded window and Unbounded window are evaluated differently in Python worker:
 * (1) Bounded window takes the window bound indices in addition to the input columns.
 *     Unbounded window takes only input columns.
 * (2) Bounded window evaluates the udf once per input row.
 *     Unbounded window evaluates the udf once per window partition.
 * This is controlled by Python runner conf "pandas_window_bound_types"
 *
 * The logic to compute window bounds is delegated to [[WindowFunctionFrame]] and shared with
 * [[WindowExec]]
 *
 * Note this doesn't support partial aggregation and all aggregation is computed from the entire
 * window.
 */
case class WindowInPandasExec(
    windowExpression: Seq[NamedExpression],
    partitionSpec: Seq[Expression],
    orderSpec: Seq[SortOrder],
    child: SparkPlan)
  extends WindowExecBase {

  override def output: Seq[Attribute] =
    child.output ++ windowExpression.map(_.toAttribute)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (partitionSpec.isEmpty) {
      // Only show warning when the number of bytes is larger than 100 MiB?
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

  /**
   * Helper functions and data structures for window bounds
   *
   * It contains:
   * (1) Total number of window bound indices in the python input row
   * (2) Function from frame index to its lower bound column index in the python input row
   * (3) Function from frame index to its upper bound column index in the python input row
   * (4) Seq from frame index to its window bound type
   */
  private type WindowBoundHelpers = (Int, Int => Int, Int => Int, Seq[WindowBoundType])

  /**
   * Enum for window bound types. Used only inside this class.
   */
  private sealed case class WindowBoundType(value: String)
  private object UnboundedWindow extends WindowBoundType("unbounded")
  private object BoundedWindow extends WindowBoundType("bounded")

  private val windowBoundTypeConf = "pandas_window_bound_types"

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

  /**
   * See [[WindowBoundHelpers]] for details.
   */
  private def computeWindowBoundHelpers(
      factories: Seq[InternalRow => WindowFunctionFrame]
  ): WindowBoundHelpers = {
    val functionFrames = factories.map(_(EmptyRow))

    val windowBoundTypes = functionFrames.map {
      case _: UnboundedWindowFunctionFrame => UnboundedWindow
      case _: UnboundedFollowingWindowFunctionFrame |
        _: SlidingWindowFunctionFrame |
        _: UnboundedPrecedingWindowFunctionFrame => BoundedWindow
      // It should be impossible to get other types of window function frame here
      case frame => throw new RuntimeException(s"Unexpected window function frame $frame.")
    }

    val requiredIndices = functionFrames.map {
      case _: UnboundedWindowFunctionFrame => 0
      case _ => 2
    }

    val upperBoundIndices = requiredIndices.scan(0)(_ + _).tail

    val boundIndices = requiredIndices.zip(upperBoundIndices).map { case (num, upperBoundIndex) =>
        if (num == 0) {
          // Sentinel values for unbounded window
          (-1, -1)
        } else {
          (upperBoundIndex - 2, upperBoundIndex - 1)
        }
    }

    def lowerBoundIndex(frameIndex: Int) = boundIndices(frameIndex)._1
    def upperBoundIndex(frameIndex: Int) = boundIndices(frameIndex)._2

    (requiredIndices.sum, lowerBoundIndex, upperBoundIndex, windowBoundTypes)
  }

  protected override def doExecute(): RDD[InternalRow] = {
    // Unwrap the expressions and factories from the map.
    val expressionsWithFrameIndex =
      windowFrameExpressionFactoryPairs.map(_._1).zipWithIndex.flatMap {
        case (buffer, frameIndex) => buffer.map(expr => (expr, frameIndex))
      }

    val expressions = expressionsWithFrameIndex.map(_._1)
    val expressionIndexToFrameIndex =
      expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap

    val factories = windowFrameExpressionFactoryPairs.map(_._2).toArray

    // Helper functions
    val (numBoundIndices, lowerBoundIndex, upperBoundIndex, frameWindowBoundTypes) =
      computeWindowBoundHelpers(factories)
    val isBounded = { frameIndex: Int => lowerBoundIndex(frameIndex) >= 0 }
    val numFrames = factories.length

    val inMemoryThreshold = conf.windowExecBufferInMemoryThreshold
    val spillThreshold = conf.windowExecBufferSpillThreshold
    val sessionLocalTimeZone = conf.sessionLocalTimeZone

    // Extract window expressions and window functions
    val windowExpressions = expressions.flatMap(_.collect { case e: WindowExpression => e })
    val udfExpressions = windowExpressions.map(_.windowFunction.asInstanceOf[PythonUDF])

    // We shouldn't be chaining anything here.
    // All chained python functions should only contain one function.
    val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip
    require(pyFuncs.length == expressions.length)

    val udfWindowBoundTypes = pyFuncs.indices.map(i =>
      frameWindowBoundTypes(expressionIndexToFrameIndex(i)))
    val pythonRunnerConf: Map[String, String] = (ArrowUtils.getPythonRunnerConfMap(conf)
      + (windowBoundTypeConf -> udfWindowBoundTypes.map(_.value).mkString(",")))

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs. This is similar to how other Python UDF node
    // handles UDF inputs.
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

    // In addition to UDF inputs, we will prepend window bounds for each UDFs.
    // For bounded windows, we prepend lower bound and upper bound. For unbounded windows,
    // we no not add window bounds. (strictly speaking, we only need to lower or upper bound
    // if the window is bounded only on one side, this can be improved in the future)

    // Setting window bounds for each window frames. Each window frame has different bounds so
    // each has its own window bound columns.
    val windowBoundsInput = factories.indices.flatMap { frameIndex =>
      if (isBounded(frameIndex)) {
        Seq(
          BoundReference(lowerBoundIndex(frameIndex), IntegerType, nullable = false),
          BoundReference(upperBoundIndex(frameIndex), IntegerType, nullable = false)
        )
      } else {
        Seq.empty
      }
    }

    // Setting the window bounds argOffset for each UDF. For UDFs with bounded window, argOffset
    // for the UDF is (lowerBoundOffset, upperBoundOffset, inputOffset1, inputOffset2, ...)
    // For UDFs with unbounded window, argOffset is (inputOffset1, inputOffset2, ...)
    pyFuncs.indices.foreach { exprIndex =>
      val frameIndex = expressionIndexToFrameIndex(exprIndex)
      if (isBounded(frameIndex)) {
        argOffsets(exprIndex) =
          Array(lowerBoundIndex(frameIndex), upperBoundIndex(frameIndex)) ++
            argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      } else {
        argOffsets(exprIndex) = argOffsets(exprIndex).map(_ + windowBoundsInput.length)
      }
    }

    val allInputs = windowBoundsInput ++ dataInputs
    val allInputTypes = allInputs.map(_.dataType)

    // Start processing.
    child.execute().mapPartitions { iter =>
      val context = TaskContext.get()

      // Get all relevant projections.
      val resultProj = createResultProjection(expressions)
      val pythonInputProj = UnsafeProjection.create(
        allInputs,
        windowBoundsInput.map(ref =>
          AttributeReference(s"i_${ref.ordinal}", ref.dataType)()) ++ child.output
      )
      val pythonInputSchema = StructType(
        allInputTypes.zipWithIndex.map { case (dt, i) =>
          StructField(s"_$i", dt)
        }
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
        private[this] def fetchNextRow(): Unit = {
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

        val indexRow = new SpecificInternalRow(Array.fill(numBoundIndices)(IntegerType))

        val frames = factories.map(_(indexRow))

        private[this] def fetchNextPartition(): Unit = {
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
                // If the window is unbounded we don't need to write out window bounds.
                if (isBounded(frameIndex)) {
                  indexRow.setInt(
                    lowerBoundIndex(frameIndex), frames(frameIndex).currentLowerBound())
                  indexRow.setInt(
                    upperBoundIndex(frameIndex), frames(frameIndex).currentUpperBound())
                }
                frameIndex += 1
              }

              pythonInputProj(join(indexRow, current))
          }
        }
      }

      val windowFunctionResult = new ArrowPythonRunner(
        pyFuncs,
        PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
        argOffsets,
        pythonInputSchema,
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
