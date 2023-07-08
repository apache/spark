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

import java.io.DataOutputStream
import java.net.Socket
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._

import net.razorvine.pickle.Unpickler

import org.apache.spark.{JobArtifactSet, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.types.StructType

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
  extends EvalPythonUDTFExec with PythonSQLMetrics {

  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  /**
   * Evaluates a Python UDTF. It computes the results using the PythonUDFRunner, and returns
   * an iterator of internal rows for every input row.
   */
  override protected def evaluate(
      argOffsets: Array[Int],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]] = {
    EvaluatePython.registerPicklers()  // register pickler for Row

    // Input iterator to Python.
    val inputIterator = BatchEvalPythonExec.getInputIterator(iter, schema)

    // Output iterator for results from Python.
    val outputIterator =
      new PythonUDTFRunner(udtf, argOffsets, pythonMetrics, jobArtifactUUID)
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

class PythonUDTFRunner(
    udtf: PythonUDTF,
    argOffsets: Array[Int],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
  extends BasePythonUDFRunner(
    Seq(ChainedPythonFunctions(Seq(udtf.func))),
    PythonEvalType.SQL_TABLE_UDF, Array(argOffsets), pythonMetrics, jobArtifactUUID) {

  protected override def newWriterThread(
      env: SparkEnv,
      worker: Socket,
      inputIterator: Iterator[Array[Byte]],
      partitionIndex: Int,
      context: TaskContext): WriterThread = {
    new PythonUDFWriterThread(env, worker, inputIterator, partitionIndex, context) {

      protected override def writeCommand(dataOut: DataOutputStream): Unit = {
        PythonUDTFRunner.writeUDTF(dataOut, udtf, argOffsets)
      }
    }
  }
}

object PythonUDTFRunner {

  def writeUDTF(dataOut: DataOutputStream, udtf: PythonUDTF, argOffsets: Array[Int]): Unit = {
    dataOut.writeInt(argOffsets.length)
    argOffsets.foreach { offset =>
      dataOut.writeInt(offset)
    }
    dataOut.writeInt(udtf.func.command.length)
    dataOut.write(udtf.func.command.toArray)
    val schemaBytes = udtf.elementSchema.json.getBytes(UTF_8)
    dataOut.writeInt(schemaBytes.length)
    dataOut.write(schemaBytes)
  }
}
