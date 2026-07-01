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

import org.apache.spark.api.python._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Arrow Python runner that accepts [[ColumnarBatch]] input directly instead of
 * [[Iterator[InternalRow]]]. When the batch's columns are Arrow-backed, the underlying
 * Arrow FieldVectors are extracted and serialized to Arrow IPC without row conversion.
 *
 * This is the columnar counterpart of [[ArrowPythonWithNamedArgumentRunner]].
 */
private[python] class ColumnarArrowPythonWithNamedArgumentRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argMetas: Array[Array[ArgumentMetadata]],
    override protected val schema: StructType,
    override protected val timeZoneId: String,
    override protected val largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String],
    override protected val inputColumnIndices: Array[Int])
  extends BaseArrowPythonRunner[ColumnarBatch, ColumnarBatch](
    funcs, evalType, argMetas.map(_.map(_.offset)), schema, timeZoneId, largeVarTypes,
    pythonMetrics, jobArtifactUUID, sessionUUID)
  with ColumnarArrowPythonInput
  with BasicPythonArrowOutput {

  override protected def runnerConf: Map[String, String] = super.runnerConf ++ pythonRunnerConf

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    if (evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF) {
      PythonWorkerUtils.writeUTF(schema.json, dataOut)
    }
    PythonUDFRunner.writeUDFs(dataOut, funcs, argMetas)
  }
}
