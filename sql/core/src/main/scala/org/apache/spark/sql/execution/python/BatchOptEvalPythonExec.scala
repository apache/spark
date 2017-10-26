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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.DataType

/**
 * A physical plan that evaluates a [[PythonUDF]]. Different to [[BatchEvalPythonExec]], this plan
 * overrides the way to compute argument offsets and adds conditional expressions into the end of
 * the offsets of the udf, if any. On Python side, the udf can be optionally run depending on the
 * evaluated values of conditional expressions.
 */
case class BatchOptEvalPythonExec(
      udfs: Seq[PythonUDF],
      output: Seq[Attribute],
      child: SparkPlan,
      udfConditionsMap: Map[PythonUDF, Seq[Expression]])
  extends BatchEvalPythonExecBase(udfs, output, child) {

  protected override val evalType: Int = PythonEvalType.SQL_BATCHED_OPT_UDF

  protected override def computeArgOffsets(
      inputs: Seq[Seq[Expression]],
      allInputs: ArrayBuffer[Expression],
      dataTypes: ArrayBuffer[DataType]): Array[Array[Int]] = {
    inputs.zipWithIndex.map { case (input, idx) =>
      var funcArgs = input.map(mapExpressionIntoFuncInputs(_, allInputs, dataTypes)).toArray
      udfConditionsMap.get(udfs(idx)).foreach { conditions =>
        conditions.reduceOption(Or).foreach { cond =>
          val condArgOffset = mapExpressionIntoFuncInputs(cond, allInputs, dataTypes)
          funcArgs = funcArgs :+ condArgOffset
        }
      }
      funcArgs
    }.toArray
  }
}
