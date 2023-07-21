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
    repartitioning: Seq[DistributionForTableFunctionCall])
  extends UnaryExecNode
    with CodegenSupport
    with PartitioningPreservingUnaryExecNode
    with OrderPreservingUnaryExecNode {

  // These methods of this operator by simply delegate by passing through tuples from its child.
  override def output: Seq[Attribute] = child.output
  override protected def outputExpressions: Seq[NamedExpression] = child.output
  override protected def orderingExpressions: Seq[SortOrder] = child.outputOrdering
  override def inputRDDs(): Seq[RDD[InternalRow]] =
    child.asInstanceOf[CodegenSupport].inputRDDs()
  protected override def doProduce(ctx: CodegenContext): String =
    child.asInstanceOf[CodegenSupport].produce(ctx, this)
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String =
    child.asInstanceOf[CodegenSupport].doConsume(ctx, input, row)
  protected override def doExecute(): RDD[InternalRow] = child.execute()

  override def requiredChildDistribution: Seq[Distribution] = {
    repartitioning.collectFirst {
      case WithSinglePartition =>
        Seq(AllTuples)
      case r: RepartitionBy =>
        Seq(ClusteredDistribution(r.expressions))
    }.getOrElse {
      super.requiredChildDistribution
    }
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = {
    repartitioning.collectFirst {
      case b: OrderBy =>
        Seq(b.expressions)
    }.getOrElse {
      super.requiredChildOrdering
    }
  }
}
