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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.PythonUDTF
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * Similar to [[ArrowPythonRunner]], but for [[PythonUDTF]]s.
 */
class ArrowPythonUDTFRunner(
    udtf: PythonUDTF,
    evalType: Int,
    argMetas: Array[ArgumentMetadata],
    protected override val schema: StructType,
    protected override val timeZoneId: String,
    protected override val largeVarTypes: Boolean,
    protected override val workerConf: Map[String, String],
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
  extends BasePythonRunner[Iterator[InternalRow], ColumnarBatch](
      Seq(ChainedPythonFunctions(Seq(udtf.func))), evalType, Array(argMetas.map(_.offset)),
      jobArtifactUUID, pythonMetrics)
  with BatchedPythonArrowInput
  with BasicPythonArrowOutput {

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    PythonUDTFRunner.writeUDTF(dataOut, udtf, argMetas)
  }

  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(
      funcs.head.funcs.head.pythonExec)

  override val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled
  override val idleTimeoutSeconds: Long = SQLConf.get.pythonUDFWorkerIdleTimeoutSeconds
  override val killOnIdleTimeout: Boolean = SQLConf.get.pythonUDFWorkerKillOnIdleTimeout

  override val errorOnDuplicatedFieldNames: Boolean = true

  override val hideTraceback: Boolean = SQLConf.get.pysparkHideTraceback
  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")
}
