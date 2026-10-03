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

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.Utils

/**
 * A physical plan that evaluates a [[PythonUDTF]], one partition of tuples at a time.
 * This is similar to [[EvalPythonExec]].
 */
trait EvalPythonUDTFExec extends UnaryExecNode {
  def udtf: PythonUDTF

  def requiredChildOutput: Seq[Attribute]

  def resultAttrs: Seq[Attribute]

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected def evaluate(
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]]

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())

    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
      val buffer = new UDTFInputBuffer(context, child.output.length)
      val (argMetas, allInputs) = flattenArguments()
      val projection = MutableProjection.create(allInputs, child.output)
      projection.initialize(context.partitionId())

      // Add rows to the buffer to join later with the result.
      val projectedRowIter = iter.map { inputRow =>
        buffer.add(inputRow.asInstanceOf[UnsafeRow])
        projection(inputRow)
      }

      val outputRowIterator = evaluate(argMetas, projectedRowIter, argumentSchema(allInputs),
        context)
      joinWithInput(outputRowIterator, buffer, child.output)
    }
  }

  /**
   * Flattens all the arguments of the UDTF: returns the metadata of each argument, and the
   * distinct argument expressions it refers to.
   */
  protected def flattenArguments(): (Array[ArgumentMetadata], Seq[Expression]) = {
    val allInputs = new ArrayBuffer[Expression]
    val argMetas = udtf.children.zip(
      udtf.tableArguments.getOrElse(Seq.fill(udtf.children.length)(false))
    ).map { case (e: Expression, isTableArg: Boolean) =>
      val (key, value) = e match {
        case NamedArgumentExpression(key, value) =>
          (Some(key), value)
        case _ =>
          (None, e)
      }
      if (allInputs.exists(_.semanticEquals(value))) {
        ArgumentMetadata(allInputs.indexWhere(_.semanticEquals(value)), key, isTableArg)
      } else {
        allInputs += value
        ArgumentMetadata(allInputs.length - 1, key, isTableArg)
      }
    }.toArray
    (argMetas, allInputs.toSeq)
  }

  /** The schema of the flattened UDTF arguments sent to the Python worker. */
  protected def argumentSchema(allInputs: Seq[Expression]): StructType =
    StructType(allInputs.zipWithIndex.map { case (e, i) =>
      StructField(s"_$i", e.dataType)
    }.toArray)

  /**
   * Joins the output of the UDTF (one iterator of rows per input row, followed by the rows of the
   * `terminate()` call) with the input rows in `buffer`, whose schema is `bufferedOutput`.
   */
  protected def joinWithInput(
      outputRowIterator: Iterator[Iterator[InternalRow]],
      buffer: UDTFInputBuffer,
      bufferedOutput: Seq[Attribute]): Iterator[InternalRow] = {
    val pruneChildForResult: InternalRow => InternalRow =
      if (AttributeSet(bufferedOutput) == AttributeSet(requiredChildOutput)) {
        identity
      } else {
        UnsafeProjection.create(requiredChildOutput, bufferedOutput)
      }

    val joined = new JoinedRow
    val nullRow = new GenericInternalRow(udtf.elementSchema.length)
    val resultProj = UnsafeProjection.create(output, output)

    outputRowIterator.flatMap { outputRows =>
      // If there are remaining input rows in the buffer, the output rows of the UDTF are joined
      // with the corresponding input row.
      if (buffer.numRows > 0) {
        joined.withLeft(pruneChildForResult(buffer.remove()))
      }
      // If all input rows have been consumed, any additional rows from the UDTF are from the
      // `terminate()` call. We leave the left side as the last element of its child output to
      // keep it consistent with the Generate implementation and Hive UDTFs.
      outputRows.map { r =>
        // When the UDTF's result is None, such as `def eval(): yield`,
        // we join it with a null row to avoid NullPointerException.
        if (r == null) {
          resultProj(joined.withRight(nullRow))
        } else {
          resultProj(joined.withRight(r))
        }
      }
    }
  }
}

/**
 * Buffers the input rows of a UDTF to join them with its output, and counts the rows that are not
 * joined yet.
 */
private[python] class UDTFInputBuffer(context: TaskContext, numFields: Int) {
  // In pipelined mode add() runs in the writer thread and remove() runs in the task thread; use
  // lock-free mode to skip per-row synchronization.
  private val queue = HybridRowQueue(context.taskMemoryManager(),
    new File(Utils.getLocalDir(SparkEnv.get.conf)), numFields,
    lockFree = SparkEnv.get.conf.get(PYTHON_UDF_PIPELINED_EXECUTION))
  context.addTaskCompletionListener[Unit] { _ =>
    queue.close()
  }

  // The number of rows added to the queue and not removed yet. This is needed to process extra
  // output rows from the `terminate()` call of the UDTF.
  private var count = 0L

  def numRows: Long = count

  def add(row: UnsafeRow): Unit = {
    queue.add(row)
    count += 1
  }

  def remove(): UnsafeRow = {
    count -= 1
    queue.remove()
  }
}
