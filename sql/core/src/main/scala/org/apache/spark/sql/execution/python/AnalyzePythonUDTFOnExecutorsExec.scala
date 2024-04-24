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

import org.apache.spark.TaskContext
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, FunctionTableSubqueryArgumentExpression, GenericInternalRow, PythonUDTF, PythonUDTFAnalyzeResult, UnsafeProjection}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.GenericArrayData
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.python.EvalPythonExec.ArgumentMetadata
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

/**
 * A physical plan that calls the 'analyze' method of a [[PythonUDTF]] on executors, when the
 * ANALYZE_PYTHON_UDTF SQL function is called in a query.
 *
 * @param udtf the user-defined Python function
 * @param requiredChildOutput the required output of the child plan. It's used for omitting data
 *                            generation that will be discarded next by a projection.
 * @param resultAttrs the output schema of the Python UDTF.
 * @param child the child plan
 */
case class AnalyzePythonUDTFOnExecutorsExec(
    udtf: PythonUDTF,
    requiredChildOutput: Seq[Attribute],
    resultAttrs: Seq[Attribute],
    child: SparkPlan)
  extends EvalPythonUDTFExec with PythonSQLMetrics {
  override def withNewChildInternal(newChild: SparkPlan): AnalyzePythonUDTFOnExecutorsExec = {
    copy(child = newChild)
  }

  override def evaluate(
      argMetas: Array[ArgumentMetadata],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]] = {
    val tableArgs = udtf.children.map(_.isInstanceOf[FunctionTableSubqueryArgumentExpression])
    val parser = CatalystSqlParser
    val runner = new UserDefinedPythonTableFunctionAnalyzeRunner(
      udtf.name, udtf.func, udtf.children, tableArgs, parser)
    val result: PythonUDTFAnalyzeResult = runner.runInPython()
    val row = new GenericInternalRow(Array(
      UTF8String.fromString(schema.json),
      result.withSinglePartition,
      new GenericArrayData(result.partitionByStrings),
      new GenericArrayData(result.orderByStrings),
      new GenericArrayData(result.selectedInputStrings),
      new GenericArrayData(result.pickledAnalyzeResult)))
    Iterator(Iterator(row))
  }

  // Simply return one row for the output of this operator containing the single row produced above.
  private lazy val resultProj = UnsafeProjection.create(output, output)
  override def consumeEvaluateResult(
      outputRowIterator: Iterator[Iterator[InternalRow]],
      queue: HybridRowQueue): Iterator[InternalRow] = {
    outputRowIterator.next().map(resultProj)
  }
}
