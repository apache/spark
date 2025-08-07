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

import scala.jdk.CollectionConverters._

import org.apache.spark.{PartitionEvaluator, PartitionEvaluatorFactory, TaskContext}
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StructField, StructType}
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

class MapInBatchEvaluatorFactory(
    output: Seq[Attribute],
    chainedFunc: Seq[(ChainedPythonFunctions, Long)],
    inputSchema: StructType,
    outputSchema: DataType,
    batchSize: Int,
    pythonEvalType: Int,
    sessionLocalTimeZone: String,
    largeVarTypes: Boolean,
    pythonRunnerConf: Map[String, String],
    val pythonMetrics: Map[String, SQLMetric],
    jobArtifactUUID: Option[String])
  extends PartitionEvaluatorFactory[InternalRow, InternalRow] {

  override def createEvaluator(): PartitionEvaluator[InternalRow, InternalRow] =
    new MapInBatchEvaluator

  private class MapInBatchEvaluator extends PartitionEvaluator[InternalRow, InternalRow] {
    override def eval(
        partitionIndex: Int,
        inputs: Iterator[InternalRow]*): Iterator[InternalRow] = {
      assert(inputs.length == 1)
      val inputIter = inputs.head
      // Single function with one struct.
      val argOffsets = Array(Array(0))
      val context = TaskContext.get()

      // Here we wrap it via another row so that Python sides understand it
      // as a DataFrame.
      val wrappedIter = inputIter.map(InternalRow(_))

      val batchIter = Iterator(wrappedIter)

      val pyRunner = new ArrowPythonRunner(
        chainedFunc,
        pythonEvalType,
        argOffsets,
        StructType(Array(StructField("struct", inputSchema))),
        sessionLocalTimeZone,
        largeVarTypes,
        pythonRunnerConf,
        pythonMetrics,
        jobArtifactUUID,
        None) with BatchedPythonArrowInput
      val columnarBatchIter = pyRunner.compute(batchIter, context.partitionId(), context)

      val unsafeProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
        if (SQLConf.get.pysparkArrowValidateSchema) {
          // Ensure the schema matches the expected schema, but allowing nullable fields in the
          // output schema to become non-nullable in the actual schema.
          val actualSchema = batch.column(0).dataType()
          val isCompatible =
            DataType.equalsIgnoreCompatibleNullability(from = actualSchema, to = outputSchema)
          if (!isCompatible) {
            throw QueryExecutionErrors.arrowDataTypeMismatchError(
              PythonEvalType.toString(pythonEvalType), Seq(outputSchema), Seq(actualSchema))
          }
        }

        // Scalar Iterator UDF returns a StructType column in ColumnarBatch, select
        // the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = output.indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())
        flattenedBatch.rowIterator.asScala
      }.map(unsafeProj)
    }
  }
}
