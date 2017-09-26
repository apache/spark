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
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType, PythonRunner}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.arrow.{ArrowConverters, ArrowPayload}
import org.apache.spark.sql.types.StructType

/**
 * A physical plan that evaluates a [[PythonUDF]],
 */
case class ArrowEvalPythonExec(udfs: Seq[PythonUDF], output: Seq[Attribute], child: SparkPlan)
  extends EvalPythonExec(udfs, output, child) {

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      bufferSize: Int,
      reuseWorker: Boolean,
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {
    val inputIterator = ArrowConverters.toPayloadIterator(
      iter, schema, conf.arrowMaxRecordsPerBatch, context).map(_.asPythonSerializable)

    // Output iterator for results from Python.
    val outputIterator = new PythonRunner(
        funcs, bufferSize, reuseWorker, PythonEvalType.SQL_PANDAS_UDF, argOffsets)
      .compute(inputIterator, context.partitionId(), context)

    val outputRowIterator = ArrowConverters.fromPayloadIterator(
      outputIterator.map(new ArrowPayload(_)), context)

    // Verify that the output schema is correct
    if (outputRowIterator.hasNext) {
      val schemaOut = StructType.fromAttributes(output.drop(child.output.length).zipWithIndex
        .map { case (attr, i) => attr.withName(s"_$i") })
      assert(schemaOut.equals(outputRowIterator.schema),
        s"Invalid schema from pandas_udf: expected $schemaOut, got ${outputRowIterator.schema}")
    }

    outputRowIterator
  }
}
