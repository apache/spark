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

object EvalPythonExec {
  /**
   * Metadata for arguments of Python UDTF.
   *
   * @param offset the offset of the argument
   * @param name the name of the argument if it's a `NamedArgumentExpression`
   */
  case class ArgumentMetadata(offset: Int, name: Option[String])
}

/**
 * A physical plan that evaluates a [[PythonUDF]], one partition of tuples at a time.
 *
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the original row.
 *
 * For each row we send to Python, we also put it in a queue first. For each output row from Python,
 * we drain the queue to find the original input row. Note that if the Python process is way too
 * slow, this could lead to the queue growing unbounded and spill into disk when run out of memory.
 *
 * Here is a diagram to show how this works:
 *
 *            Downstream (for parent)
 *             /      \
 *            /     socket  (output of UDF)
 *           /         \
 *        RowQueue    Python
 *           \         /
 *            \     socket  (input of UDF)
 *             \     /
 *          upstream (from child)
 *
 * The rows sent to and received from Python are packed into batches (100 rows) and serialized,
 * there should be always some rows buffered in the socket or Python process, so the pulling from
 * RowQueue ALWAYS happened after pushing into it.
 */
trait EvalPythonExec extends UnaryExecNode {
  def udfs: Seq[PythonUDF]
  def resultAttrs: Seq[Attribute]

  protected def evaluatorFactory: EvalPythonEvaluatorFactory

  override def output: Seq[Attribute] = child.output ++ resultAttrs

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
