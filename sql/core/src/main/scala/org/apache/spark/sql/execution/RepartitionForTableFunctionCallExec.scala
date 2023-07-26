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

import org.apache.spark.SparkException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, Distribution}

/**
 * If a table-valued function call includes a TABLE argument with the PARTITION BY clause, we add
 * this physical operator to represent the repartitioning operation in the query plan. Its execution
 * is an identity projection, and overrides the 'requiredChildDistribution' method to specify the
 * repartitioning to perform.
 */
case class RepartitionForTableFunctionCallExec(
    override val child: SparkPlan,
    withSinglePartition: Boolean = false,
    partitionByExpressions: Seq[Expression] = Seq.empty,
    orderByExpressions: Seq[SortOrder] = Seq.empty)
  extends UnaryExecNode
    with CodegenSupport
    with PartitioningPreservingUnaryExecNode
    with OrderPreservingUnaryExecNode {

  // These methods of this operator by simply delegate to the child operator by passing through
  // tuples unmodified.
  override def output: Seq[Attribute] = {
    child.output
  }
  override protected def outputExpressions: Seq[NamedExpression] = {
    child.output
  }
  override protected def orderingExpressions: Seq[SortOrder] = {
    child.outputOrdering
  }
  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    child.asInstanceOf[CodegenSupport].inputRDDs()
  }
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan = {
    copy(child = newChild)
  }
  protected override def doProduce(ctx: CodegenContext): String = {
    throw executionNotImplementedYetError()
  }
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    throw executionNotImplementedYetError()
  }
  protected override def doExecute(): RDD[InternalRow] = {
    throw executionNotImplementedYetError()
  }
  private def executionNotImplementedYetError(): Throwable = {
    SparkException.internalError("query execution for table function calls with repartitioning")
  }

  // These methods forward the desired repartitioning and ordering properties to the optimizer.
  override def requiredChildDistribution: Seq[Distribution] = {
    if (withSinglePartition) {
      Seq(AllTuples)
    } else if (partitionByExpressions.nonEmpty) {
      Seq(ClusteredDistribution(partitionByExpressions))
    } else {
      super.requiredChildDistribution
    }
  }
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    if (orderByExpressions.nonEmpty) {
      Seq(orderByExpressions)
    } else {
      super.requiredChildOrdering
    }
  }
}
