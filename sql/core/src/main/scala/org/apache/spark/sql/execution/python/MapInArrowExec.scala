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
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.SparkPlan

/**
 * A relation produced by applying a function that takes an iterator of PyArrow's record batches
 * and outputs an iterator of PyArrow's record batches.
 */
case class MapInArrowExec(
    func: Expression,
    output: Seq[Attribute],
    child: SparkPlan,
    override val isBarrier: Boolean,
    override val profile: Option[ResourceProfile])
  extends MapInBatchExec {

  override protected val pythonEvalType: Int = PythonEvalType.SQL_MAP_ARROW_ITER_UDF

  override protected def withNewChildInternal(newChild: SparkPlan): MapInArrowExec =
    copy(child = newChild)
}
