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
   * @param isTableArg whether this argument is a table argument
   */
  case class ArgumentMetadata(offset: Int, name: Option[String], isTableArg: Boolean = false)
}

/**
 * A physical plan that evaluates a [[PythonUDF]], one partition of tuples at a time.
 *
 * Python evaluation works by sending the necessary (projected) input data via a socket to an
 * external Python process, and combine the result from the Python process with the columns of the
 * original row that are passed through (`requiredChildOutput`).
 *
 * For each row we send to Python, we also put its pass-through columns in a queue first. For each
 * output row from Python, we drain the queue to find them. Note that if the Python process is way
 * too slow, this could lead to the queue growing unbounded and spill into disk when run out of
 * memory. If no column is passed through, the results from Python are the output rows.
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

  /** The columns of the child that are passed through to the output. */
  def requiredChildOutput: Seq[Attribute]

  def resultAttrs: Seq[Attribute]

  /** Returns a copy that passes through `requiredChildOutput` of the child. */
  def withRequiredChildOutput(requiredChildOutput: Seq[Attribute]): EvalPythonExec

  protected def evaluatorFactory: EvalPythonEvaluatorFactory

  override def output: Seq[Attribute] = requiredChildOutput ++ resultAttrs

  override def producedAttributes: AttributeSet = AttributeSet(resultAttrs)

  /** The input rows of the evaluator, copied unless it consumes each before the next. */
  protected def evaluatorInput: RDD[InternalRow] = child.execute().map(_.copy())

  protected override def doExecute(): RDD[InternalRow] = {
    val inputRDD = evaluatorInput
    if (conf.usePartitionEvaluator) {
      inputRDD.mapPartitionsWithEvaluator(evaluatorFactory)
    } else {
      inputRDD.mapPartitionsWithIndexInternal { (index, iter) =>
        evaluatorFactory.createEvaluator().eval(index, iter)
      }
    }
  }
}
