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
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.TaskContext
import org.apache.spark.api.python.BasePythonRunner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, NamedExpression, UnsafeProjection}
import org.apache.spark.sql.execution.GroupedIterator
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}

/**
 * Base functionality for plans which execute grouped python udfs.
 */
private[python] object PandasGroupUtils {
  /**
   * passes the data to the python runner and coverts the resulting
   * columnarbatch into internal rows.
   */
  def executePython[T](
      data: Iterator[T],
      output: Seq[Attribute],
      runner: BasePythonRunner[T, ColumnarBatch]): Iterator[InternalRow] = {

    val context = TaskContext.get()
    val columnarBatchIter = runner.compute(data, context.partitionId(), context)
    val unsafeProj = UnsafeProjection.create(output, output)

    columnarBatchIter.flatMap { batch =>
      //  UDF returns a StructType column in ColumnarBatch, select the children here
      val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
      val outputVectors = output.indices.map(structVector.getChild)
      val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
      flattenedBatch.setNumRows(batch.numRows())
      flattenedBatch.rowIterator.asScala
    }.map(unsafeProj)
  }

  /**
   * groups according to grouping attributes and then projects into the deduplicated schema
   */
  def groupAndProject(
      input: Iterator[InternalRow],
      groupingExprs: Seq[NamedExpression],
      inputSchema: Seq[Attribute],
      dedupSchema: Seq[NamedExpression]): Iterator[(InternalRow, Iterator[InternalRow])] = {
    val groupedIter = GroupedIterator(input, groupingExprs, inputSchema)
    val dedupProj = UnsafeProjection.create(dedupSchema, inputSchema)
    groupedIter.map {
      case (k, groupedRowIter) => (k, groupedRowIter.map(dedupProj))
    }
  }

  /**
   * Returns a the deduplicated named expressions of the spark plan and the arg offsets of the
   * keys and values.
   *
   * The deduplicated expressions are needed because the spark plan may contain an expression
   * twice; once in the key and once in the value.  For any such expression we need to
   * deduplicate.
   *
   * The arg offsets are used to distinguish grouping expressions and data expressions
   * as following:
   *
   * argOffsets[0] is the length of the argOffsets array
   *
   * argOffsets[1] is the length of grouping expression
   * argOffsets[2 .. argOffsets[0]+2] is the arg offsets for grouping expressions
   *
   * argOffsets[argOffsets[0]+2 .. ] is the arg offsets for data expressions
   */
  def resolveArgOffsets(
      dataExprs: Seq[NamedExpression], groupingExprs: Seq[NamedExpression])
    : (Seq[NamedExpression], Array[Int]) = {

    val groupingIndicesInData = groupingExprs.map { expression =>
      dataExprs.indexWhere(expression.semanticEquals)
    }

    val groupingArgOffsets = new ArrayBuffer[Int]
    val nonDupGroupingExprs = new ArrayBuffer[NamedExpression]
    val nonDupGroupingSize = groupingIndicesInData.count(_ == -1)

    groupingExprs.zip(groupingIndicesInData).foreach {
      case (expression, index) =>
        if (index == -1) {
          groupingArgOffsets += nonDupGroupingExprs.length
          nonDupGroupingExprs += expression
        } else {
          groupingArgOffsets += index + nonDupGroupingSize
        }
    }

    val dataArgOffsets = nonDupGroupingExprs.length until
      (nonDupGroupingExprs.length + dataExprs.length)

    val argOffsetsLength = groupingExprs.length + dataArgOffsets.length + 1
    val argOffsets = Array(argOffsetsLength,
      groupingExprs.length) ++ groupingArgOffsets ++ dataArgOffsets

    // Expressions after deduplication
    val dedupExprs = nonDupGroupingExprs ++ dataExprs
    (dedupExprs, argOffsets)
  }
}
