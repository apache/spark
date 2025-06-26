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

import scala.jdk.CollectionConverters._

import net.razorvine.pickle.Unpickler

import org.apache.spark.{JobArtifactSet, SparkEnv, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonWorkerUtils}
import org.apache.spark.internal.config.BUFFER_SIZE
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
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
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]] = {
    EvaluatePython.registerPicklers()  // register pickler for Row

    // Input iterator to Python.
    // For Python UDTF, we don't have a separate configuration for the batch size yet.
    val inputIterator = BatchEvalPythonExec.getInputIterator(iter, schema, 100)

    // Output iterator for results from Python.
    val outputIterator =
      new PythonUDTFRunner(udtf, argMetas, pythonMetrics, jobArtifactUUID)
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
        .array.map(_.asInstanceOf[InternalRow]).iterator
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): BatchEvalPythonUDTFExec =
    copy(child = newChild)
}

class PythonUDTFRunner(
    udtf: PythonUDTF,
    argMetas: Array[ArgumentMetadata],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
  extends BasePythonUDFRunner(
    Seq((ChainedPythonFunctions(Seq(udtf.func)), udtf.resultId.id)),
    PythonEvalType.SQL_TABLE_UDF, Array(argMetas.map(_.offset)), pythonMetrics, jobArtifactUUID) {

  // Overriding here to NOT use the same value of UDF config in UDTF.
  override val bufferSize: Int = SparkEnv.get.conf.get(BUFFER_SIZE)

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    PythonUDTFRunner.writeUDTF(dataOut, udtf, argMetas)
  }
}

object PythonUDTFRunner {

  def writeUDTF(
      dataOut: DataOutputStream,
      udtf: PythonUDTF,
      argMetas: Array[ArgumentMetadata]): Unit = {
    // Write the argument types of the UDTF.
    dataOut.writeInt(argMetas.length)
    argMetas.foreach {
      case ArgumentMetadata(offset, name) =>
        dataOut.writeInt(offset)
        name match {
          case Some(name) =>
            dataOut.writeBoolean(true)
            PythonWorkerUtils.writeUTF(name, dataOut)
          case _ =>
            dataOut.writeBoolean(false)
        }
    }
    // Write the zero-based indexes of the projected results of all PARTITION BY expressions within
    // the TABLE argument of the Python UDTF call, if applicable.
    udtf.pythonUDTFPartitionColumnIndexes match {
      case Some(partitionColumnIndexes) =>
        dataOut.writeInt(partitionColumnIndexes.partitionChildIndexes.length)
        assert(partitionColumnIndexes.partitionChildIndexes.nonEmpty)
        partitionColumnIndexes.partitionChildIndexes.foreach(dataOut.writeInt)
      case None =>
        dataOut.writeInt(0)
    }
    // Write the pickled AnalyzeResult buffer from the UDTF "analyze" method, if any.
    dataOut.writeBoolean(udtf.pickledAnalyzeResult.nonEmpty)
    udtf.pickledAnalyzeResult.foreach(PythonWorkerUtils.writeBytes(_, dataOut))
    // Write the contents of the Python script itself.
    PythonWorkerUtils.writePythonFunction(udtf.func, dataOut)
    // Write the result schema of the UDTF call.
    PythonWorkerUtils.writeUTF(udtf.elementSchema.json, dataOut)
    // Write the UDTF name.
    PythonWorkerUtils.writeUTF(udtf.name, dataOut)
  }
}
