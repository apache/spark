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
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan}
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
      groupingAttributes: Seq[Attribute],
      inputSchema: Seq[Attribute],
      dedupSchema: Seq[Attribute]): Iterator[(InternalRow, Iterator[InternalRow])] = {
    val groupedIter = GroupedIterator(input, groupingAttributes, inputSchema)
    val dedupProj = UnsafeProjection.create(dedupSchema, inputSchema)
    groupedIter.map {
      case (k, groupedRowIter) => (k, groupedRowIter.map(dedupProj))
    }
  }

  /**
   * Returns a the deduplicated attributes of the spark plan and the arg offsets of the
   * keys and values.
   *
   * The deduplicated attributes are needed because the spark plan may contain an attribute
   * twice; once in the key and once in the value.  For any such attribute we need to
   * deduplicate.
   *
   * The arg offsets are used to distinguish grouping grouping attributes and data attributes
   * as following:
   *
   * argOffsets[0] is the length of the argOffsets array
   *
   * argOffsets[1] is the length of grouping attribute
   * argOffsets[2 .. argOffsets[0]+2] is the arg offsets for grouping attributes
   *
   * argOffsets[argOffsets[0]+2 .. ] is the arg offsets for data attributes
   */
  def resolveArgOffsets(
    child: SparkPlan, groupingAttributes: Seq[Attribute]): (Seq[Attribute], Array[Int]) = {

    val dataAttributes = child.output.drop(groupingAttributes.length)
    val groupingIndicesInData = groupingAttributes.map { attribute =>
      dataAttributes.indexWhere(attribute.semanticEquals)
    }

    val groupingArgOffsets = new ArrayBuffer[Int]
    val nonDupGroupingAttributes = new ArrayBuffer[Attribute]
    val nonDupGroupingSize = groupingIndicesInData.count(_ == -1)

    groupingAttributes.zip(groupingIndicesInData).foreach {
      case (attribute, index) =>
        if (index == -1) {
          groupingArgOffsets += nonDupGroupingAttributes.length
          nonDupGroupingAttributes += attribute
        } else {
          groupingArgOffsets += index + nonDupGroupingSize
        }
    }

    val dataArgOffsets = nonDupGroupingAttributes.length until
      (nonDupGroupingAttributes.length + dataAttributes.length)

    val argOffsetsLength = groupingAttributes.length + dataArgOffsets.length + 1
    val argOffsets = Array(argOffsetsLength,
          groupingAttributes.length) ++ groupingArgOffsets ++ dataArgOffsets

    // Attributes after deduplication
    val dedupAttributes = nonDupGroupingAttributes ++ dataAttributes
    (dedupAttributes.toSeq, argOffsets)
  }
}
