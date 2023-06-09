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

import net.razorvine.pickle.Unpickler

import org.apache.spark.{ContextAwareIterator, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.util.Utils

/**
 * A physical plan that evaluates a [[PythonUDTF]]. This is similar to [[BatchEvalPythonExec]].
 *
 * @param udtf the user-defined Python function
 * @param requiredChildOutput the required output of the child plan. It's used for omitting data
 *                            generation that will be discarded next by a projection.
 * @param resultAttrs the output schema of the Python UDTF.
 * @param child the child plan
 */
case class BatchEvalPythonUDTFExec(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode with PythonSQLMetrics {

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())

    inputRDD.mapPartitions { iter =>
      val context = TaskContext.get()
      val contextAwareIterator = new ContextAwareIterator(context, iter)

      // The queue used to buffer input rows so we can drain it to
      // combine input with output from Python.
      val queue = HybridRowQueue(context.taskMemoryManager(),
        new File(Utils.getLocalDir(SparkEnv.get.conf)), child.output.length)
      context.addTaskCompletionListener[Unit] { ctx =>
        queue.close()
      }

      val inputs = Seq(udtf.children)

      // flatten all the arguments
      val allInputs = new ArrayBuffer[Expression]
      val dataTypes = new ArrayBuffer[DataType]
      val argOffsets = inputs.map { input =>
        input.map { e =>
          if (allInputs.exists(_.semanticEquals(e))) {
            allInputs.indexWhere(_.semanticEquals(e))
          } else {
            allInputs += e
            dataTypes += e.dataType
            allInputs.length - 1
          }
        }.toArray
      }.toArray
      val projection = MutableProjection.create(allInputs.toSeq, child.output)
      projection.initialize(context.partitionId())
      val schema = StructType(dataTypes.zipWithIndex.map { case (dt, i) =>
        StructField(s"_$i", dt)
      }.toArray)

      // Add rows to the queue to join later with the result.
      // Also keep track of the number rows added to the queue.
      var count = 0L
      val projectedRowIter = contextAwareIterator.map { inputRow =>
        queue.add(inputRow.asInstanceOf[UnsafeRow])
        count += 1
        projection(inputRow)
      }

      val outputRowIterator = evaluate(udtf, argOffsets, projectedRowIter, schema, context)

      val pruneChildForResult: InternalRow => InternalRow =
        if (child.outputSet == AttributeSet(requiredChildOutput)) {
          identity
        } else {
          UnsafeProjection.create(requiredChildOutput, child.output)
        }

      val joined = new JoinedRow
      val resultProj = UnsafeProjection.create(output, output)

      outputRowIterator.flatMap { outputRows =>
        if (count > 0) {
          val left = queue.remove()
          count -= 1
          joined.withLeft(pruneChildForResult(left))
        }
        outputRows.map(r => resultProj(joined.withRight(r)))
      }
    }
  }

  private def evaluate(
      udtf: PythonUDTF,
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]] = {
    EvaluatePython.registerPicklers()  // register pickler for Row

    // Input iterator to Python.
    val inputIterator = BatchEvalPythonExec.getInputIterator(iter, schema)

    // Output iterator for results from Python.
    val funcs = Seq(ChainedPythonFunctions(Seq(udtf.func)))
    val outputIterator =
      new PythonUDFRunner(funcs, PythonEvalType.SQL_TABLE_UDF, argOffsets, pythonMetrics)
        .compute(inputIterator, context.partitionId(), context)

    val unpickle = new Unpickler

    // The return type of a UDTF is an array of struct.
    val resultType = udtf.dataType
    val fromJava = EvaluatePython.makeFromJava(resultType)

    outputIterator.flatMap { pickedResult =>
      val unpickledBatch = unpickle.loads(pickedResult)
      unpickledBatch.asInstanceOf[java.util.ArrayList[Any]].asScala
    }.map { results =>
      assert(results.getClass.isArray)
      val res = results.asInstanceOf[Array[_]]
      pythonMetrics("pythonNumRowsReceived") += res.length
      fromJava(results).asInstanceOf[GenericArrayData]
        .array.map(_.asInstanceOf[InternalRow]).toIterator
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BatchEvalPythonUDTFExec =
    copy(child = newChild)
}
