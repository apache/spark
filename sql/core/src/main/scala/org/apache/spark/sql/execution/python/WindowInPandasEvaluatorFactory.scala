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

import org.apache.spark.{JobArtifactSet, PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, EmptyRow, Expression, JoinedRow, NamedArgumentExpression, NamedExpression, PythonFuncExpression, PythonUDAF, SortOrder, SpecificInternalRow, UnsafeProjection, UnsafeRow, WindowExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.execution.window.{SlidingWindowFunctionFrame, UnboundedFollowingWindowFunctionFrame, UnboundedPrecedingWindowFunctionFrame, UnboundedWindowFunctionFrame, WindowEvaluatorFactoryBase, WindowFunctionFrame}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, IntegerType, StructField, StructType}
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.Utils

class WindowInPandasEvaluatorFactory(
    val windowExpression: Seq[NamedExpression],
    val partitionSpec: Seq[Expression],
    val orderSpec: Seq[SortOrder],
    val childOutput: Seq[Attribute],
    val spillSize: SQLMetric,
    pythonMetrics: Map[String, SQLMetric],
    profiler: Option[String])
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] with WindowEvaluatorFactoryBase {

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

  private def collectFunctions(
      udf: PythonFuncExpression): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
    udf.children match {
      case Seq(u: PythonFuncExpression) =>
        val ((chained, _), children) = collectFunctions(u)
        ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
      case children =>
        // There should not be any other UDFs, or the children can't be evaluated directly.
        assert(children.forall(!_.exists(_.isInstanceOf[PythonFuncExpression])))
        ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
    }
  }

  // Helper functions
  /**
   * See [[WindowBoundHelpers]] for details.
   */
  private def computeWindowBoundHelpers(
      factories: Seq[InternalRow => WindowFunctionFrame]): WindowBoundHelpers = {
    val functionFrames = factories.map(_ (EmptyRow))

    val windowBoundTypes = functionFrames.map {
      case _: UnboundedWindowFunctionFrame => UnboundedWindow
      case _: UnboundedFollowingWindowFunctionFrame |
           _: SlidingWindowFunctionFrame |
           _: UnboundedPrecedingWindowFunctionFrame => BoundedWindow
      // It should be impossible to get other types of window function frame here
      case frame => throw QueryExecutionErrors.unexpectedWindowFunctionFrameError(frame.toString)
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

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] = {
    new WindowInPandasPartitionEvaluator()
  }

  class WindowInPandasPartitionEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    private val conf: SQLConf = SQLConf.get

    // Unwrap the expressions and factories from the map.
    private val expressionsWithFrameIndex =
      windowFrameExpressionFactoryPairs.map(_._1).zipWithIndex.flatMap {
        case (buffer, frameIndex) => buffer.map(expr => (expr, frameIndex))
      }

    private val expressions = expressionsWithFrameIndex.map(_._1)
    private val expressionIndexToFrameIndex =
      expressionsWithFrameIndex.map(_._2).zipWithIndex.map(_.swap).toMap

    private val factories = windowFrameExpressionFactoryPairs.map(_._2).toArray

    private val (numBoundIndices, lowerBoundIndex, upperBoundIndex, frameWindowBoundTypes) =
      computeWindowBoundHelpers(factories.toImmutableArraySeq)
    private val isBounded = { frameIndex: Int => lowerBoundIndex(frameIndex) >= 0 }
    private val numFrames = factories.length

    private val inMemoryThreshold = conf.windowExecBufferInMemoryThreshold
    private val spillThreshold = conf.windowExecBufferSpillThreshold
    private val sessionLocalTimeZone = conf.sessionLocalTimeZone
    private val largeVarTypes = conf.arrowUseLargeVarTypes

    // Extract window expressions and window functions
    private val windowExpressions = expressions.flatMap(_.collect { case e: WindowExpression => e })
    private val udfExpressions = windowExpressions.map { e =>
      e.windowFunction.asInstanceOf[AggregateExpression].aggregateFunction.asInstanceOf[PythonUDAF]
    }

    // We shouldn't be chaining anything here.
    // All chained python functions should only contain one function.
    private val (pyFuncs, inputs) = udfExpressions.map(collectFunctions).unzip
    require(pyFuncs.length == expressions.length)

    private val udfWindowBoundTypes = pyFuncs.indices.map(i =>
      frameWindowBoundTypes(expressionIndexToFrameIndex(i)))
    private val pythonRunnerConf: Map[String, String] =
      (ArrowPythonRunner.getPythonRunnerConfMap(conf)
      + (windowBoundTypeConf -> udfWindowBoundTypes.map(_.value).mkString(",")))

    // Filter child output attributes down to only those that are UDF inputs.
    // Also eliminate duplicate UDF inputs. This is similar to how other Python UDF node
    // handles UDF inputs.
    private val dataInputs = new ArrayBuffer[Expression]
    private val dataInputTypes = new ArrayBuffer[DataType]
    private val argMetas = inputs.map { input =>
      input.map { e =>
        val (key, value) = e match {
          case NamedArgumentExpression(key, value) =>
            (Some(key), value)
          case _ =>
            (None, e)
        }
        if (dataInputs.exists(_.semanticEquals(value))) {
          ArgumentMetadata(dataInputs.indexWhere(_.semanticEquals(value)), key)
        } else {
          dataInputs += value
          dataInputTypes += value.dataType
          ArgumentMetadata(dataInputs.length - 1, key)
        }
      }.toArray
    }.toArray

    // In addition to UDF inputs, we will prepend window bounds for each UDFs.
    // For bounded windows, we prepend lower bound and upper bound. For unbounded windows,
    // we no not add window bounds. (strictly speaking, we only need to lower or upper bound
    // if the window is bounded only on one side, this can be improved in the future)

    // Setting window bounds for each window frames. Each window frame has different bounds so
    // each has its own window bound columns.
    private val windowBoundsInput = factories.indices.flatMap { frameIndex =>
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
        argMetas(exprIndex) =
          Array(
            ArgumentMetadata(lowerBoundIndex(frameIndex), None),
            ArgumentMetadata(upperBoundIndex(frameIndex), None)) ++
          argMetas(exprIndex).map(
            meta => ArgumentMetadata(meta.offset + windowBoundsInput.length, meta.name))
      } else {
        argMetas(exprIndex) = argMetas(exprIndex).map(
          meta => ArgumentMetadata(meta.offset + windowBoundsInput.length, meta.name))
      }
    }

    private val allInputs = windowBoundsInput ++ dataInputs
    private val allInputTypes = allInputs.map(_.dataType)
    private val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val iter = inputs.head
      val context = TaskContext.get()

      // Get all relevant projections.
      val resultProj = createResultProjection(expressions)
      val pythonInputProj = UnsafeProjection.create(
        allInputs,
        windowBoundsInput.map(ref =>
          AttributeReference(s"i_${ref.ordinal}", ref.dataType)()) ++ childOutput
      )
      val pythonInputSchema = StructType(
        allInputTypes.zipWithIndex.map { case (dt, i) =>
          StructField(s"_$i", dt)
        }
      )
      val grouping = UnsafeProjection.create(partitionSpec, childOutput)

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), childOutput.length)
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

        val indexRow =
          new SpecificInternalRow(Array.fill(numBoundIndices)(IntegerType).toImmutableArraySeq)

        val frames = factories.map(_ (indexRow))

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

        override final def hasNext: Boolean = {
          val found = (bufferIterator != null && bufferIterator.hasNext) || nextRowAvailable
          if (!found) {
            // clear final partition
            buffer.clear()
            spillSize += buffer.spillSize
          }
          found
        }

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

      val windowFunctionResult = new ArrowPythonWithNamedArgumentRunner(
        pyFuncs,
        PythonEvalType.SQL_WINDOW_AGG_PANDAS_UDF,
        argMetas,
        pythonInputSchema,
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        profiler).compute(pythonInput, context.partitionId(), context)

      val joined = new JoinedRow

      windowFunctionResult.flatMap(_.rowIterator.asScala).map { windowOutput =>
        val leftRow = queue.remove()
        val joinedRow = joined(leftRow, windowOutput)
        resultProj(joinedRow)
      }
    }
  }
}
