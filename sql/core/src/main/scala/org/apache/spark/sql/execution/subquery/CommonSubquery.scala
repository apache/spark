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

package org.apache.spark.sql.execution.subquery

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Statistics}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.util.Utils

private[sql] case class CommonSubquery(
    output: Seq[Attribute],
    @transient child: SparkPlan)(
    @transient val logicalChild: LogicalPlan,
    private[sql] val _statistics: Statistics,
    @transient private[sql] var _computedOutput: RDD[InternalRow] = null)
  extends logical.LeafNode {

  override def argString: String = Utils.truncatedString(output, "[", ", ", "]")

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(logicalChild)

  override def producedAttributes: AttributeSet = outputSet

  override lazy val statistics: Statistics = _statistics

  lazy val numRows: Long = computedOutput.count

  def withOutput(newOutput: Seq[Attribute]): CommonSubquery = {
    CommonSubquery(newOutput, child)(logicalChild, _statistics, _computedOutput)
  }

  def computedOutput: RDD[InternalRow] = this.synchronized {
    if (_computedOutput == null) {
      _computedOutput = child.execute()
    }
    _computedOutput
  }

  override protected def otherCopyArgs: Seq[AnyRef] =
    Seq(logicalChild, _statistics, _computedOutput)
}
