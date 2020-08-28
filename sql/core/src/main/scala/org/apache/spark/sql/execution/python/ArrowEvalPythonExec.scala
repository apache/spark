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

import org.apache.spark.TaskContext
import org.apache.spark.api.python.ChainedPythonFunctions
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils

/**
 * Grouped a iterator into batches.
 * This is similar to iter.grouped but returns Iterator[T] instead of Seq[T].
 * This is necessary because sometimes we cannot hold reference of input rows
 * because the some input rows are mutable and can be reused.
 */
private[spark] class BatchIterator[T](iter: Iterator[T], batchSize: Int)
  extends Iterator[Iterator[T]] {

  override def hasNext: Boolean = iter.hasNext

  override def next(): Iterator[T] = {
    new Iterator[T] {
      var count = 0

      override def hasNext: Boolean = iter.hasNext && count < batchSize

      override def next(): T = {
        if (!hasNext) {
          Iterator.empty.next()
        } else {
          count += 1
          iter.next()
        }
      }
    }
  }
}

/**
 * A physical plan that evaluates a [[PythonUDF]].
 */
case class ArrowEvalPythonExec(udfs: Seq[PythonUDF], resultAttrs: Seq[Attribute], child: SparkPlan,
    evalType: Int)
  extends EvalPythonExec {

  private val batchSize = conf.arrowMaxRecordsPerBatch
  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  protected override def evaluate(
      funcs: Seq[ChainedPythonFunctions],
      argOffsets: Array[Array[Int]],
      iter: Iterator[InternalRow],
      schema: StructType,
      context: TaskContext): Iterator[InternalRow] = {

    val outputTypes = output.drop(child.output.length).map(_.dataType)

    // DO NOT use iter.grouped(). See BatchIterator.
    val batchIter = if (batchSize > 0) new BatchIterator(iter, batchSize) else Iterator(iter)

    val columnarBatchIter = new ArrowPythonRunner(
      funcs,
      evalType,
      argOffsets,
      schema,
      sessionLocalTimeZone,
      pythonRunnerConf).compute(batchIter, context.partitionId(), context)

    columnarBatchIter.flatMap { batch =>
      val actualDataTypes = (0 until batch.numCols()).map(i => batch.column(i).dataType())
      assert(outputTypes == actualDataTypes, "Invalid schema from pandas_udf: " +
        s"expected ${outputTypes.mkString(", ")}, got ${actualDataTypes.mkString(", ")}")
      batch.rowIterator.asScala
    }
  }
}
