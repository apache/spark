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

import scala.collection.JavaConverters._

import org.apache.spark.{ContextAwareIterator, TaskContext}
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.UnaryExecNode
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * A relation produced by applying a function that takes an iterator of batches
 * such as pandas DataFrame or PyArrow's record batches, and outputs an iterator of them.
 *
 * This is somewhat similar with [[FlatMapGroupsInPandasExec]] and
 * `org.apache.spark.sql.catalyst.plans.logical.MapPartitionsInRWithArrow`
 */
trait MapInBatchExec extends UnaryExecNode with PythonSQLMetrics {
  protected val func: Expression
  protected val pythonEvalType: Int

  private val pythonFunction = func.asInstanceOf[PythonUDF].func

  override def producedAttributes: AttributeSet = AttributeSet(output)

  private val batchSize = conf.arrowMaxRecordsPerBatch

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitionsInternal { inputIter =>
      // Single function with one struct.
      val argOffsets = Array(Array(0))
      val chainedFunc = Seq(ChainedPythonFunctions(Seq(pythonFunction)))
      val sessionLocalTimeZone = conf.sessionLocalTimeZone
      val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
      val outputTypes = child.schema

      val context = TaskContext.get()
      val contextAwareIterator = new ContextAwareIterator(context, inputIter)

      // Here we wrap it via another row so that Python sides understand it
      // as a DataFrame.
      val wrappedIter = contextAwareIterator.map(InternalRow(_))

      // DO NOT use iter.grouped(). See BatchIterator.
      val batchIter =
        if (batchSize > 0) new BatchIterator(wrappedIter, batchSize) else Iterator(wrappedIter)

      val columnarBatchIter = new ArrowPythonRunner(
        chainedFunc,
        pythonEvalType,
        argOffsets,
        StructType(Array(StructField("struct", outputTypes))),
        sessionLocalTimeZone,
        pythonRunnerConf,
        pythonMetrics).compute(batchIter, context.partitionId(), context)

      val unsafeProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
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
