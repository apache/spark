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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * An intermediate placeholder for EmptyRelation planning, will be replaced with
 * EmptyRelationExec eventually.
 */
case class EmptyRelationPlanLater(plan: LogicalPlan) extends PlanLaterBase

/**
 * A leaf node wrapper for propagated empty relation, which preserved the physical plan.
 */
case class EmptyRelationExec(plan: SparkPlan) extends LeafExecNode with InputRDDCodegen {
  private val rdd = sparkContext.emptyRDD[InternalRow]

  override def output: Seq[Attribute] = plan.output

  override protected def doExecute(): RDD[InternalRow] = rdd

  override def executeCollect(): Array[InternalRow] = Array.empty

  override def executeTake(limit: Int): Array[InternalRow] = Array.empty

  override def executeTail(limit: Int): Array[InternalRow] = Array.empty

  protected override def doExecuteColumnar(): RDD[ColumnarBatch] = sparkContext.emptyRDD

  override def inputRDD: RDD[InternalRow] = rdd

  override protected val createUnsafeProjection: Boolean = false

  protected override def stringArgs: Iterator[Any] = Iterator(s"[plan_id=$id]")

  override def generateTreeString(
    depth: Int,
    lastChildren: java.util.ArrayList[Boolean],
    append: String => Unit,
    verbose: Boolean,
    prefix: String = "",
    addSuffix: Boolean = false,
    maxFields: Int,
    printNodeId: Boolean,
    indent: Int = 0): Unit = {
    super.generateTreeString(depth,
      lastChildren,
      append,
      verbose,
      prefix,
      addSuffix,
      maxFields,
      printNodeId,
      indent)
    lastChildren.add(true)
    plan.generateTreeString(
      depth + 1, lastChildren, append, verbose, "", false, maxFields, printNodeId, indent)
    lastChildren.remove(lastChildren.size() - 1)
  }

  override def doCanonicalize(): SparkPlan = {
    this.copy(plan = LocalTableScanExec(plan.output, Nil))
  }

  override protected[sql] def cleanupResources(): Unit = {
    plan.cleanupResources()
    super.cleanupResources()
  }
}
