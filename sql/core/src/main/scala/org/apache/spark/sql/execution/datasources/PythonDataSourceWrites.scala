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

package org.apache.spark.sql.execution.datasources

import java.io.{DataInputStream, DataOutputStream}

import net.razorvine.pickle.Pickler

import org.apache.spark.api.python.{PythonEvalType, PythonFunction, PythonWorkerUtils, SimplePythonFunction, SpecialLengths}
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalyst.expressions.{PredicateHelper, PythonUDF}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MapInPandas}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.python.PythonPlannerRunner
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}


/**
 * A rule that constructs logical writes for Python data sources.
 */
object PythonDataSourceWrites extends Rule[LogicalPlan] with PredicateHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case c @ SaveIntoPythonDataSourceCommand(query, dataSourceCls, provider, options, mode, None) =>

      // Create a logical plan for the write (mapInArrow)
      val runner = new SaveIntoPythonDataSourceRunner(
        dataSourceCls, provider, options, mode, query)

      val result = runner.runInPython()

      // Construct the Python UDF.
      val func = SimplePythonFunction(
        command = result.func,
        envVars = dataSourceCls.envVars,
        pythonIncludes = dataSourceCls.pythonIncludes,
        pythonExec = dataSourceCls.pythonExec,
        pythonVer = dataSourceCls.pythonVer,
        broadcastVars = dataSourceCls.broadcastVars,
        accumulator = dataSourceCls.accumulator)

      val dataType = StructType(Array(StructField("message", BinaryType)))

      val pythonUDF = PythonUDF(
        name = "save_into_data_source",
        func = func,
        dataType = dataType,
        children = query.output,
        evalType = PythonEvalType.SQL_MAP_PANDAS_ITER_UDF,
        udfDeterministic = true) // TODO: change to false

      // Construct the plan.
      val plan = MapInPandas(
        pythonUDF,
        toAttributes(dataType),
        query,
        isBarrier = false)

      // TODO: think about a different way to make it idempotent
      c.copy(query = plan, write = Some(plan))
  }
}

case class PythonDataSourceSaveResult(func: Array[Byte])

class SaveIntoPythonDataSourceRunner(
    dataSourceCls: PythonFunction,
    provider: String,
    options: Map[String, String],
    mode: SaveMode,
    plan: LogicalPlan) extends PythonPlannerRunner[PythonDataSourceSaveResult](dataSourceCls) {

  override val workerModule: String = "pyspark.sql.worker.save_into_data_source"

  override protected def writeToPython(dataOut: DataOutputStream, pickler: Pickler): Unit = {
    // Send Python data source
    PythonWorkerUtils.writePythonFunction(dataSourceCls, dataOut)

    // Send the provider name
    PythonWorkerUtils.writeUTF(provider, dataOut)

    // Send the output schema
    PythonWorkerUtils.writeUTF(plan.schema.json, dataOut)

    // Send the options
    dataOut.writeInt(options.size)
    options.iterator.foreach { case (key, value) =>
      PythonWorkerUtils.writeUTF(key, dataOut)
      PythonWorkerUtils.writeUTF(value, dataOut)
    }

    // Send the mode
    PythonWorkerUtils.writeUTF(mode.toString, dataOut)
  }

  override protected def receiveFromPython(
    dataIn: DataInputStream): PythonDataSourceSaveResult = {

    // Receive the picked UDF or an exception raised in Python worker.
    val length = dataIn.readInt()
    if (length == SpecialLengths.PYTHON_EXCEPTION_THROWN) {
      val msg = PythonWorkerUtils.readUTF(dataIn)
      throw QueryCompilationErrors.failToPlanDataSourceError(
        action = "plan", tpe = "write", msg = msg)
    }

    // Receive the pickled data source.
    val writeUdf: Array[Byte] = PythonWorkerUtils.readBytes(length, dataIn)

    PythonDataSourceSaveResult(func = writeUdf)
  }
}
