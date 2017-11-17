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
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.types.StructType

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

    val bufferSize = inputRDD.conf.getInt("spark.buffer.size", 65536)
    val reuseWorker = inputRDD.conf.getBoolean("spark.python.worker.reuse", defaultValue = true)
    val chainedFunc = Seq(ChainedPythonFunctions(Seq(pandasFunction)))
    val argOffsets = Array((0 until (child.output.length - groupingAttributes.length)).toArray)
    val schema = StructType(child.schema.drop(groupingAttributes.length))
    val sessionLocalTimeZone = conf.sessionLocalTimeZone

    inputRDD.mapPartitionsInternal { iter =>
      val grouped = if (groupingAttributes.isEmpty) {
        Iterator(iter)
      } else {
        val groupedIter = GroupedIterator(iter, groupingAttributes, child.output)
        val dropGrouping =
          UnsafeProjection.create(child.output.drop(groupingAttributes.length), child.output)
        groupedIter.map {
          case (_, groupedRowIter) => groupedRowIter.map(dropGrouping)
        }
      }

      val context = TaskContext.get()

      val columnarBatchIter = new ArrowPythonRunner(
        chainedFunc, bufferSize, reuseWorker,
        PythonEvalType.SQL_PANDAS_GROUP_MAP_UDF, argOffsets, schema, sessionLocalTimeZone)
          .compute(grouped, context.partitionId(), context)

      columnarBatchIter.flatMap(_.rowIterator.asScala).map(UnsafeProjection.create(output, output))
    }
  }
}
