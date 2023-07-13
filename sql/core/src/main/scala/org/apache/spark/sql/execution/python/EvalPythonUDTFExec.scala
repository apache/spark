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
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.StructType

/**
 * A physical plan that evaluates a [[PythonUDTF]], one partition of tuples at a time.
 * This is similar to [[EvalPythonExec]].
 */
trait EvalPythonUDTFExec extends UnaryExecNode {
  def udtf: PythonUDTF

  def requiredChildOutput: Seq[Attribute]

  def resultAttrs: Seq[Attribute]

  protected def getPythonUDTFEvaluator: EvalPythonUDTFEvaluator

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    val evaluatorFactory =
      new EvalPythonUDTFEvaluatorFactory(
        child.output,
        udtf,
        child.outputSet,
        requiredChildOutput: Seq[Attribute],
        output: Seq[Attribute],
        getPythonUDTFEvaluator)
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitions { iter =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(0, iter)
      }
    }
  }
}

abstract class EvalPythonUDTFEvaluator {
  def evaluate(
      argOffsets: Array[Int],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[Iterator[InternalRow]]
}
