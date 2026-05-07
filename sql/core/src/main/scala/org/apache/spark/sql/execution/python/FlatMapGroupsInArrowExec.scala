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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.{StructField, StructType}


/**
 * Physical node for [[org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInArrow]]
 *
 * Rows in each group are passed to the Python worker as an iterator of Arrow record batches.
 * The Python worker passes the record batches either as a materialized `pyarrow.Table` or
 * an iterator of pyarrow.RecordBatch, depending on the eval type of the user-defined function.
 * The Python worker returns the resulting record batches which are turned into an
 * Iterator[InternalRow] using ColumnarBatch.
 *
 * Note on memory usage:
 * When using the `pyarrow.Table` API, the entire group is materialized in memory in the Python
 * worker, and the entire result for a group must also be fully materialized. The iterator of
 * record batches API can be used to avoid this limitation on the Python side.
 */
case class FlatMapGroupsInArrowExec(
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends FlatMapGroupsInBatchExec {

  protected val pythonEvalType: Int = {
    func.asInstanceOf[PythonUDF].evalType
  }

  override protected def groupedData(iter: Iterator[InternalRow], attrs: Seq[Attribute]):
      Iterator[Iterator[InternalRow]] =
    super.groupedData(iter, attrs)
      // Here we wrap it via another row so that Python sides understand it as a DataFrame.
      .map(_.map(InternalRow(_)))

  override protected def groupedSchema(attrs: Seq[Attribute]): StructType =
    StructType(StructField("struct", super.groupedSchema(attrs)) :: Nil)

  override protected def withNewChildInternal(newChild: SparkPlan): FlatMapGroupsInArrowExec =
    copy(child = newChild)
}
