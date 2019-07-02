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
import org.apache.spark.api.python.{BasePythonRunner, ChainedPythonFunctions}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeSet, Expression, PythonUDF, UnsafeProjection}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch}


abstract class BasePandasGroupExec(func: Expression,
                                   output: Seq[Attribute]) extends SparkPlan {

  protected val sessionLocalTimeZone = conf.sessionLocalTimeZone

  protected val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

  protected val pandasFunction = func.asInstanceOf[PythonUDF].func

  protected val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))

  override def producedAttributes: AttributeSet = AttributeSet(output)


  protected def executePython[T]
  (data: Iterator[T], runner: BasePythonRunner[T, ColumnarBatch]): Iterator[InternalRow] = {

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

  protected def groupAndDedup(
      input: Iterator[InternalRow], groupingAttributes: Seq[Attribute],
      inputSchema: Seq[Attribute], dedupSchema: Seq[Attribute]
                             ): Iterator[(InternalRow, Iterator[InternalRow])] = {
    val groupedIter = GroupedIterator(input, groupingAttributes, inputSchema)
    val dedupProj = UnsafeProjection.create(dedupSchema, inputSchema)
    groupedIter.map {
      case (k, groupedRowIter) => (k, groupedRowIter.map(dedupProj))
    }
  }

  protected def createSchema(child: SparkPlan, groupingAttributes: Seq[Attribute])
    : (StructType, Seq[Attribute], Array[Int]) = {

    // Deduplicate the grouping attributes.
    // If a grouping attribute also appears in data attributes, then we don't need to send the
    // grouping attribute to Python worker. If a grouping attribute is not in data attributes,
    // then we need to send this grouping attribute to python worker.
    //
    // We use argOffsets to distinguish grouping attributes and data attributes as following:
    //
    // argOffsets[0] is the length of grouping attributes
    // argOffsets[1 .. argOffsets[0]+1] is the arg offsets for grouping attributes
    // argOffsets[argOffsets[0]+1 .. ] is the arg offsets for data attributes

    val dataAttributes = child.output.drop(groupingAttributes.length)
    val groupingIndicesInData = groupingAttributes.map { attribute =>
      dataAttributes.indexWhere(attribute.semanticEquals)
    }

    val groupingArgOffsets = new ArrayBuffer[Int]
    val nonDupGroupingAttributes = new ArrayBuffer[Attribute]
    val nonDupGroupingSize = groupingIndicesInData.count(_ == -1)

    // Non duplicate grouping attributes are added to nonDupGroupingAttributes and
    // their offsets are 0, 1, 2 ...
    // Duplicate grouping attributes are NOT added to nonDupGroupingAttributes and
    // their offsets are n + index, where n is the total number of non duplicate grouping
    // attributes and index is the index in the data attributes that the grouping attribute
    // is a duplicate of.

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
    val dedupSchema = StructType.fromAttributes(dedupAttributes)
    (dedupSchema, dedupAttributes, argOffsets)
  }

}
