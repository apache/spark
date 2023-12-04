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
import org.apache.spark.sql.catalyst.expressions.PythonUDTF
import org.apache.spark.sql.catalyst.plans.logical.{Generate, LogicalPlan, Project, PythonDataSource, PythonDataSourcePartitions}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.PYTHON_DATA_SOURCE
import org.apache.spark.sql.execution.python.UserDefinedPythonDataSourceReadRunner
import org.apache.spark.util.ArrayImplicits._

/**
 * A logical rule to plan reads from a Python data source.
 *
 * This rule creates a Python process and invokes the `DataSource.reader` method to create an
 * instance of the user-defined data source reader, generates partitions if any, and returns
 * the information back to JVM (this rule) to construct the logical plan for Python data source.
 *
 * For example, prior to applying this rule, the plan might look like:
 *
 *   PythonDataSource(dataSource, schema, output)
 *
 * Here, `dataSource` is a serialized Python function that contains an instance of the DataSource
 * class. Post this rule, the plan is transformed into:
 *
 *  Project [output]
 *  +- Generate [python_data_source_read_udtf, ...]
 *     +- PythonDataSourcePartitions [partition_bytes]
 *
 * The PythonDataSourcePartitions contains a list of serialized partition values for the data
 * source. The `DataSourceReader.read` method will be planned as a UDTF that accepts a partition
 * value and yields the scanning output.
 */
object PlanPythonDataSourceScan extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDownWithPruning(
    _.containsPattern(PYTHON_DATA_SOURCE)) {
    case ds @ PythonDataSource(dataSource: PythonFunction, schema, _) =>
      val info = new UserDefinedPythonDataSourceReadRunner(dataSource, schema).runInPython()

      val readerFunc = SimplePythonFunction(
        command = info.func.toImmutableArraySeq,
        envVars = dataSource.envVars,
        pythonIncludes = dataSource.pythonIncludes,
        pythonExec = dataSource.pythonExec,
        pythonVer = dataSource.pythonVer,
        broadcastVars = dataSource.broadcastVars,
        accumulator = dataSource.accumulator)

      val partitionPlan = PythonDataSourcePartitions(
        PythonDataSourcePartitions.getOutputAttrs, info.partitions)

      // Construct a Python UDTF for the reader function.
      val pythonUDTF = PythonUDTF(
        name = "python_data_source_read",
        func = readerFunc,
        elementSchema = schema,
        children = partitionPlan.output,
        evalType = PythonEvalType.SQL_TABLE_UDF,
        udfDeterministic = false,
        pickledAnalyzeResult = None)

      // Later the rule `ExtractPythonUDTFs` will turn this Generate
      // into a evaluable Python UDTF node.
      val generate = Generate(
        generator = pythonUDTF,
        unrequiredChildIndex = Nil,
        outer = false,
        qualifier = None,
        generatorOutput = ds.output,
        child = partitionPlan)

      // Project out partition values.
      Project(ds.output, generate)
  }
}
