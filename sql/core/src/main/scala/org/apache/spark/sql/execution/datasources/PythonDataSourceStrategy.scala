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

import org.apache.spark.api.python.{PythonEvalType, PythonFunction, SimplePythonFunction}
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.PythonUDTF
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.UserDefinedPythonDataSourceReadRunner

object PythonDataSourceStrategy extends Strategy with Logging {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case p @ logical.PythonDataSource(dataSource: PythonFunction, schema, _) =>
      val info = new UserDefinedPythonDataSourceReadRunner(dataSource, schema).runInPython()

      val readerFunc = SimplePythonFunction(
        command = info.func.toSeq,
        envVars = dataSource.envVars,
        pythonIncludes = dataSource.pythonIncludes,
        pythonExec = dataSource.pythonExec,
        pythonVer = dataSource.pythonVer,
        broadcastVars = dataSource.broadcastVars,
        accumulator = dataSource.accumulator
      )

      val partitionPlan = logical.PythonDataSourcePartition(info.partitions)

      // Construct a Python UDTF for the reader function.
      val pythonUDTF = PythonUDTF(
        name = "PythonDataSourceReaderUDTF",
        func = readerFunc,
        elementSchema = schema,
        children = partitionPlan.output,
        evalType = PythonEvalType.SQL_TABLE_UDF,
        udfDeterministic = false,
        pickledAnalyzeResult = None)

      // Use batch eval UDTF to avoid pandas dependency and
      // unnecessary pandas serialization cost.
      val pythonEval = logical.BatchEvalPythonUDTF(
        pythonUDTF,
        Nil,  // Do not include child output (partition bytes)
        p.output,
        partitionPlan)

      planLater(pythonEval) :: Nil

    case _ => Nil
  }
}
