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
import java.util

import org.apache.spark.api.python._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

abstract class BaseArrowPythonRunner[IN, OUT <: AnyRef](
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    _timeZoneId: String,
    protected override val largeVarTypes: Boolean,
    override val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
  extends BasePythonRunner[IN, OUT](
    funcs.map(_._1), evalType, argOffsets, jobArtifactUUID, pythonMetrics)
  with PythonArrowInput[IN]
  with PythonArrowOutput[OUT] {

  override val envVars: util.Map[String, String] = {
    val envVars = new util.HashMap(funcs.head._1.funcs.head.envVars)
    sessionUUID.foreach { uuid =>
      envVars.put("PYSPARK_SPARK_SESSION_UUID", uuid)
    }
    envVars
  }
  override val pythonExec: String =
    SQLConf.get.pysparkWorkerPythonExecutable.getOrElse(
      funcs.head._1.funcs.head.pythonExec)

  override val faultHandlerEnabled: Boolean = SQLConf.get.pythonUDFWorkerFaulthandlerEnabled
  override val idleTimeoutSeconds: Long = SQLConf.get.pythonUDFWorkerIdleTimeoutSeconds
  override val killOnIdleTimeout: Boolean = SQLConf.get.pythonUDFWorkerKillOnIdleTimeout
  override val tracebackDumpIntervalSeconds: Long =
    SQLConf.get.pythonUDFWorkerTracebackDumpIntervalSeconds
  override val killWorkerOnFlushFailure: Boolean =
    SQLConf.get.pythonUDFDaemonKillWorkerOnFlushFailure

  override val errorOnDuplicatedFieldNames: Boolean = true

  override val hideTraceback: Boolean = SQLConf.get.pysparkHideTraceback
  override val simplifiedTraceback: Boolean = SQLConf.get.pysparkSimplifiedTraceback

  // Use lazy val to initialize the fields before these are accessed in [[PythonArrowInput]]'s
  // constructor.
  override protected lazy val timeZoneId: String = _timeZoneId
  override protected lazy val schema: StructType = _schema
  override val bufferSize: Int = SQLConf.get.pandasUDFBufferSize
  require(
    bufferSize >= 4,
    "Pandas execution requires more than 4 bytes. Please set higher buffer. " +
      s"Please change '${SQLConf.PANDAS_UDF_BUFFER_SIZE.key}'.")
}

abstract class RowInputArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    _timeZoneId: String,
    largeVarTypes: Boolean,
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String])
  extends BaseArrowPythonRunner[Iterator[InternalRow], ColumnarBatch](
    funcs, evalType, argOffsets, _schema, _timeZoneId, largeVarTypes,
    pythonMetrics, jobArtifactUUID, sessionUUID)
  with BasicPythonArrowInput
  with BasicPythonArrowOutput

/**
 * Similar to `PythonUDFRunner`, but exchange data with Python worker via Arrow stream.
 */
class ArrowPythonRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argOffsets: Array[Array[Int]],
    _schema: StructType,
    _timeZoneId: String,
    largeVarTypes: Boolean,
    protected override val runnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String],
    profiler: Option[String])
  extends RowInputArrowPythonRunner(
    funcs, evalType, argOffsets, _schema, _timeZoneId, largeVarTypes,
    pythonMetrics, jobArtifactUUID, sessionUUID) {

  override protected def writeUDF(dataOut: DataOutputStream): Unit =
    PythonUDFRunner.writeUDFs(dataOut, funcs, argOffsets, profiler)
}

/**
 * Similar to `PythonUDFWithNamedArgumentsRunner`, but exchange data with Python worker
 * via Arrow stream.
 */
class ArrowPythonWithNamedArgumentRunner(
    funcs: Seq[(ChainedPythonFunctions, Long)],
    evalType: Int,
    argMetas: Array[Array[ArgumentMetadata]],
    _schema: StructType,
    _timeZoneId: String,
    largeVarTypes: Boolean,
    protected override val runnerConf: Map[String, String],
    pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String],
    sessionUUID: Option[String],
    profiler: Option[String])
  extends RowInputArrowPythonRunner(
    funcs, evalType, argMetas.map(_.map(_.offset)), _schema, _timeZoneId, largeVarTypes,
    pythonMetrics, jobArtifactUUID, sessionUUID) {

  override protected def writeUDF(dataOut: DataOutputStream): Unit = {
    if (evalType == PythonEvalType.SQL_ARROW_BATCHED_UDF) {
      PythonWorkerUtils.writeUTF(schema.json, dataOut)
    }
    PythonUDFRunner.writeUDFs(dataOut, funcs, argMetas, profiler)
  }
}

object ArrowPythonRunner {
  /** Return Map with conf settings to be used in ArrowPythonRunner */
  def getPythonRunnerConfMap(conf: SQLConf): Map[String, String] = {
    val timeZoneConf = Seq(SQLConf.SESSION_LOCAL_TIMEZONE.key -> conf.sessionLocalTimeZone)
    val pandasColsByName = Seq(SQLConf.PANDAS_GROUPED_MAP_ASSIGN_COLUMNS_BY_NAME.key ->
      conf.pandasGroupedMapAssignColumnsByName.toString)
    val arrowSafeTypeCheck = Seq(SQLConf.PANDAS_ARROW_SAFE_TYPE_CONVERSION.key ->
      conf.arrowSafeTypeConversion.toString)
    val arrowAyncParallelism = conf.pythonUDFArrowConcurrencyLevel.map(v =>
      Seq(SQLConf.PYTHON_UDF_ARROW_CONCURRENCY_LEVEL.key -> v.toString)
    ).getOrElse(Seq.empty)
    val useLargeVarTypes = Seq(SQLConf.ARROW_EXECUTION_USE_LARGE_VAR_TYPES.key ->
      conf.arrowUseLargeVarTypes.toString)
    val legacyPandasConversion = Seq(
      SQLConf.PYTHON_TABLE_UDF_LEGACY_PANDAS_CONVERSION_ENABLED.key ->
      conf.legacyPandasConversion.toString)
    val legacyPandasConversionUDF = Seq(
      SQLConf.PYTHON_UDF_LEGACY_PANDAS_CONVERSION_ENABLED.key ->
      conf.legacyPandasConversionUDF.toString)
    val intToDecimalCoercion = Seq(
      SQLConf.PYTHON_UDF_PANDAS_INT_TO_DECIMAL_COERCION_ENABLED.key ->
      conf.getConf(SQLConf.PYTHON_UDF_PANDAS_INT_TO_DECIMAL_COERCION_ENABLED, false).toString)
    val binaryAsBytes = Seq(
      SQLConf.PYSPARK_BINARY_AS_BYTES.key ->
      conf.pysparkBinaryAsBytes.toString)
    Map(timeZoneConf ++ pandasColsByName ++ arrowSafeTypeCheck ++
      arrowAyncParallelism ++ useLargeVarTypes ++
      intToDecimalCoercion ++ binaryAsBytes ++
      legacyPandasConversion ++ legacyPandasConversionUDF: _*)
  }
}
