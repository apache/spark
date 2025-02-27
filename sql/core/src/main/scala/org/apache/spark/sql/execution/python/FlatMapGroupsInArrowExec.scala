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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan


/**
 * Physical node for [[org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInArrow]]
 *
 * Rows in each group are passed to the Python worker as an Arrow record batch.
 * The Python worker turns the record batch to a `pyarrow.Table`, invokes the
 * user-defined function, and passes the resulting `pyarrow.Table`
 * as an Arrow record batch. Finally, each record batch is turned to
 * Iterator[InternalRow] using ColumnarBatch.
 *
 * Note on memory usage:
 * Both the Python worker and the Java executor need to have enough memory to
 * hold the largest group. The memory on the Java side is used to construct the
 * record batch (off heap memory). The memory on the Python side is used for
 * holding the `pyarrow.Table`. It's possible to further split one group into
 * multiple record batches to reduce the memory footprint on the Java side, this
 * is left as future work.
 */
case class FlatMapGroupsInArrowExec(
    groupingAttributes: Seq[Attribute],
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan)
  extends FlatMapGroupsInBatchExec {

  protected val pythonEvalType: Int = PythonEvalType.SQL_GROUPED_MAP_ARROW_UDF

  override protected def withNewChildInternal(newChild: SparkPlan): FlatMapGroupsInArrowExec =
    copy(child = newChild)
}
