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

import scala.jdk.CollectionConverters._

import org.apache.spark.{JobArtifactSet, TaskContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * A physical plan that evaluates a [[PythonUDTF]] using Apache Arrow.
 * This is similar to [[ArrowEvalPythonExec]].
 *
 * @param udtf the user-defined Python function.
 * @param requiredChildOutput the required output of the child plan. It's used for omitting data
 *                            generation that will be discarded next by a projection.
 * @param resultAttrs the output schema of the Python UDTF.
 * @param child the child plan.
 * @param evalType the Python eval type.
 */
case class ArrowEvalPythonUDTFExec(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan,
    evalType: Int)
  extends EvalPythonUDTFExec with PythonSQLMetrics {

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val largeVarTypes = conf.arrowUseLargeVarTypes
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  override protected def evaluate(
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]] = {

    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val outputTypes = resultAttrs.map(_.dataType)

    val columnarBatchIter = new ArrowPythonUDTFRunner(
      udtf,
      evalType,
      argMetas,
      schema,
      sessionLocalTimeZone,
      largeVarTypes,
      pythonRunnerConf,
      pythonMetrics,
      jobArtifactUUID).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.map { batch =>
      // UDTF returns a StructType column in ColumnarBatch. Flatten the columnar batch here.
      val columnVector = batch.column(0).asInstanceOf[ArrowColumnVector]
      val outputVectors = resultAttrs.indices.map(columnVector.getChild)
      val flattenedBatch = new ColumnarBatch(outputVectors.toArray)

      val actualDataTypes = (0 until flattenedBatch.numCols()).map(
        i => flattenedBatch.column(i).dataType())
      assert(outputTypes == actualDataTypes, "Invalid schema from arrow-enabled Python UDTF: " +
        s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")

      flattenedBatch.setNumRows(batch.numRows())
      flattenedBatch.rowIterator().asScala
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}
