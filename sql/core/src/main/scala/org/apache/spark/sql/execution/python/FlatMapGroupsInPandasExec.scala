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
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.arrow.ArrowUtils
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}

/**
 * Physical node for [[org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInPandas]]
 *
 * Rows in each group are passed to the Python worker as an Arrow record batch.
 * The Python worker turns the record batch to a `pandas.DataFrame`, invoke the
 * user-defined function, and passes the resulting `pandas.DataFrame`
 * as an Arrow record batch. Finally, each record batch is turned to
 * Iterator[InternalRow] using ColumnarBatch.
 *
 * Note on memory usage:
 * Both the Python worker and the Java executor need to have enough memory to
 * hold the largest group. The memory on the Java side is used to construct the
 * record batch (off heap memory). The memory on the Python side is used for
 * holding the `pandas.DataFrame`. It's possible to further split one group into
 * multiple record batches to reduce the memory footprint on the Java side, this
 * is left as future work.
 */
case class FlatMapGroupsInPandasExec(
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends UnaryExecNode {

  private val pandasFunction = func.asInstanceOf[PythonUDF].func

  override def outputPartitioning: Partitioning = child.outputPartitioning

  override def producedAttributes: AttributeSet = AttributeSet(output)

  override def requiredChildDistribution: Seq[Distribution] = {
    if (groupingAttributes.isEmpty) {
      AllTuples :: Nil
    } else {
      ClusteredDistribution(groupingAttributes) :: Nil
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    val inputRDD = child.execute()

    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
    val sessionLocalTimeZone = conf.sessionLocalTimeZone
    val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)

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

    val argOffsets = Array(Array(groupingAttributes.length) ++ groupingArgOffsets ++ dataArgOffsets)

    // Attributes after deduplication
    val dedupAttributes = nonDupGroupingAttributes ++ dataAttributes
    val dedupSchema = StructType.fromAttributes(dedupAttributes)

    inputRDD.mapPartitionsInternal { iter =>
      val grouped = if (groupingAttributes.isEmpty) {
        Iterator(iter)
      } else {
        val groupedIter = GroupedIterator(iter, groupingAttributes, child.output)
        val dedupProj = UnsafeProjection.create(dedupAttributes, child.output)
        groupedIter.map {
          case (_, groupedRowIter) => groupedRowIter.map(dedupProj)
        }
      }

      val context = TaskContext.get()

      val columnarBatchIter = new ArrowPythonRunner(
        chainedFunc,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF,
        argOffsets,
        dedupSchema,
        sessionLocalTimeZone,
        pythonRunnerConf).compute(grouped, context.partitionId(), context)

      val unsafeProj = UnsafeProjection.create(output, output)

      columnarBatchIter.flatMap { batch =>
        // Grouped Map UDF returns a StructType column in ColumnarBatch, select the children here
        val structVector = batch.column(0).asInstanceOf[ArrowColumnVector]
        val outputVectors = output.indices.map(structVector.getChild)
        val flattenedBatch = new ColumnarBatch(outputVectors.toArray)
        flattenedBatch.setNumRows(batch.numRows())
        flattenedBatch.rowIterator.asScala
      }.map(unsafeProj)
    }
  }
}
