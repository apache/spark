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
import org.apache.spark.sql.types.{DataType, StructField, StructType}
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

      // The queue used to buffer required input columns so we can drain it to
      // combine input with output from Python.
      // In pipelined mode the queue's add() runs in the writer thread and remove() runs in
      // the task thread; use lock-free mode to skip per-row synchronization.
      val (queue, pruneChildForResult) = if (requiredChildOutput.nonEmpty) {
        val pipelined = SparkEnv.get.conf.get(PYTHON_UDF_PIPELINED_EXECUTION)
        val q = HybridRowQueue(context.taskMemoryManager(),
          new File(Utils.getLocalDir(SparkEnv.get.conf)),
          requiredChildOutput.length, lockFree = pipelined)
        context.addTaskCompletionListener[Unit] { _ =>
          q.close()
        }
        val prune: InternalRow => UnsafeRow =
          if (requiredChildOutput == child.output) {
            _.asInstanceOf[UnsafeRow]
          } else {
            val proj = UnsafeProjection.create(requiredChildOutput, child.output)
            proj.initialize(context.partitionId())
            proj.apply _
          }
        (q, prune)
      } else {
        (null, null)
      }

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
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
          dataTypes += value.dataType
          ArgumentMetadata(allInputs.length - 1, key, isTableArg)
        }
      }.toArray
      val projection = MutableProjection.create(allInputs.toSeq, child.output)
      projection.initialize(context.partitionId())
      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toArray)

      // Add required columns to the queue to join later with the result.
      // Also keep track of the number rows added to the queue.
      // This is needed to process extra output rows from the `terminate()` call of the UDTF.
      var count = 0L
      val projectedRowIter = iter.map { inputRow =>
        if (queue != null) {
          queue.add(pruneChildForResult(inputRow))
          count += 1
        }
        projection(inputRow)
      }

      val outputRowIterator = evaluate(argMetas, projectedRowIter, schema, context)

      val joined = new JoinedRow
      val nullRow = new GenericInternalRow(udtf.elementSchema.length)
      val resultProj = UnsafeProjection.create(output, output)

      outputRowIterator.flatMap { outputRows =>
        if (queue != null) {
          // If `count` is greater than zero, it means there are remaining input rows in the queue.
          // In this case, the output rows of the UDTF are joined with the corresponding input row
          // in the queue.
          if (count > 0) {
            val left = queue.remove()
            count -= 1
            joined.withLeft(left)
          }
          // If `count` is zero, it means all input rows have been consumed. Any additional rows
          // from the UDTF are from the `terminate()` call. We leave the left side as the last
          // element of its child output to keep it consistent with the Generate implementation
          // and Hive UDTFs.
          outputRows.map { r =>
            // When the UDTF's result is None, such as `def eval(): yield`,
            // we join it with a null row to avoid NullPointerException.
            if (r == null) {
              resultProj(joined.withRight(nullRow))
            } else {
              resultProj(joined.withRight(r))
            }
          }
        } else {
          outputRows.map { r =>
            if (r == null) {
              resultProj(nullRow)
            } else {
              resultProj(r)
            }
          }
        }
      }
    }
  }
}
