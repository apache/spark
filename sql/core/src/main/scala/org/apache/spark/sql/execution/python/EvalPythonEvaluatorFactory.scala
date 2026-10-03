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

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, SparkEnv, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.internal.config.Python.PYTHON_UDF_PIPELINED_EXECUTION
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

abstract class EvalPythonEvaluatorFactory(
    childOutput: Seq[Attribute],
    udfs: Seq[PythonUDF],
    output: Seq[Attribute])
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  protected def evaluate(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow]

  /**
   * Evaluates the UDFs over the input rows and returns the output rows: each input row's
   * columns followed by its results, as unsafe rows that remain valid after the next call.
   * Returns None to let the evaluator buffer the input rows and join them with the results of
   * `evaluate`, which receives only the projected arguments.
   *
   * @param inputs the UDF arguments, which `argMetas` and `schema` refer to by position
   */
  protected def evaluateJoined(
      funcs: Seq[(ChainedPythonFunctions, Long)],
      argMetas: Array[Array[ArgumentMetadata]],
      iter: Iterator[InternalRow],
      inputs: Seq[Expression],
      schema: StructType,
      context: TaskContext): Option[Iterator[InternalRow]] = None

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new EvalPythonPartitionEvaluator

  private class EvalPythonPartitionEvaluator
      extends PartitionEvaluator[InternalRow, InternalRow] {
    private def collectFunctions(
        udf: PythonUDF): ((ChainedPythonFunctions, Long), Seq[Expression]) = {
      udf.children match {
        case Seq(u: PythonUDF) =>
          val ((chained, _), children) = collectFunctions(u)
          ((ChainedPythonFunctions(chained.funcs ++ Seq(udf.func)), udf.resultId.id), children)
        case children =>
          // There should not be any other UDFs, or the children can't be evaluated directly.
          assert(children.forall(!_.exists(_.isInstanceOf[PythonUDF])))
          ((ChainedPythonFunctions(Seq(udf.func)), udf.resultId.id), udf.children)
      }
    }
    override def eval(
        partitionIndex: Int,
        iters: Iterator[InternalRow]*): Iterator[InternalRow] = {
      val iter = iters.head
      val context = TaskContext.get()

      val (pyFuncs, inputs) = udfs.map(collectFunctions).unzip

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argMetas = inputs.map { input =>
        input.map { e =>
          val (key, value) = e match {
            case NamedArgumentExpression(key, value) =>
              (Some(key), value)
            case _ =>
              (None, e)
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
      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toArray)

      val joinedRows = evaluateJoined(pyFuncs, argMetas, iter, allInputs.toSeq, schema, context)
      if (joinedRows.isDefined) return joinedRows.get

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      // In pipelined mode, add() runs in the writer thread and remove() in the task thread.
      // Use lock-free mode to avoid synchronized overhead (memory visibility is guaranteed
      // by the blocking socket I/O between the two threads).
      val pipelined = SparkEnv.get.conf.get(PYTHON_UDF_PIPELINED_EXECUTION)
      val queue = HybridRowQueue(
        context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)),
        childOutput.length,
        lockFree = pipelined)
      context.addTaskCompletionListener[Unit] { ctx =>
        queue.close()
      }

      val projection = MutableProjection.create(allInputs.toSeq, childOutput)
      projection.initialize(context.partitionId())

      // Add rows to queue to join later with the result.
      val projectedRowIter = iter.map { inputRow =>
        queue.add(inputRow.asInstanceOf[UnsafeRow])
        projection(inputRow)
      }

      val outputRowIterator =
        evaluate(pyFuncs, argMetas, projectedRowIter, schema, context)

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      outputRowIterator.map { outputRow =>
        resultProj(joined(queue.remove(), outputRow))
      }
    }
  }
}
