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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.UnaryExecNode

/**
 * A physical plan that evaluates a [[PythonUDTF]], one partition of tuples at a time.
 * This is similar to [[EvalPythonExec]].
 */
trait EvalPythonUDTFExec extends UnaryExecNode {
  def udtf: PythonUDTF

  def requiredChildOutput: Seq[Attribute]

  def resultAttrs: Seq[Attribute]

  protected def evaluatorFactory: EvalPythonUDTFEvaluatorFactory

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute().map(_.copy())
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }
}
