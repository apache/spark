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

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{OrderedDistribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.ScalaReflection

case class Project(projectList: Seq[NamedExpression], child: SparkPlan) extends UnaryNode {
  def output = projectList.map(_.toAttribute)

  def execute() = child.execute().mapPartitions { iter =>
    @transient val reusableProjection = new MutableProjection(projectList)
    iter.map(reusableProjection)
  }
}

case class Filter(condition: Expression, child: SparkPlan) extends UnaryNode {
  def output = child.output

  def execute() = child.execute().mapPartitions { iter =>
    iter.filter(condition.apply(_).asInstanceOf[Boolean])
  }
}

case class Sample(fraction: Double, withReplacement: Boolean, seed: Int, child: SparkPlan)
    extends UnaryNode {

  def output = child.output

  // TODO: How to pick seed?
  def execute() = child.execute().sample(withReplacement, fraction, seed)
}

case class Union(children: Seq[SparkPlan])(@transient sc: SparkContext) extends SparkPlan {
  // TODO: attributes output by union should be distinct for nullability purposes
  def output = children.head.output
  def execute() = sc.union(children.map(_.execute()))

  override def otherCopyArgs = sc :: Nil
}

case class StopAfter(limit: Int, child: SparkPlan)(@transient sc: SparkContext) extends UnaryNode {
  override def otherCopyArgs = sc :: Nil

  def output = child.output

  override def executeCollect() = child.execute().map(_.copy()).take(limit)

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  def execute() = sc.makeRDD(executeCollect(), 1)
}

case class TopK(limit: Int, sortOrder: Seq[SortOrder], child: SparkPlan)
               (@transient sc: SparkContext) extends UnaryNode {
  override def otherCopyArgs = sc :: Nil

  def output = child.output

  @transient
  lazy val ordering = new RowOrdering(sortOrder)

  override def executeCollect() = child.execute().map(_.copy()).takeOrdered(limit)(ordering)

  // TODO: Terminal split should be implemented differently from non-terminal split.
  // TODO: Pick num splits based on |limit|.
  def execute() = sc.makeRDD(executeCollect(), 1)
}


case class Sort(
    sortOrder: Seq[SortOrder],
    global: Boolean,
    child: SparkPlan)
  extends UnaryNode {
  override def requiredChildDistribution =
    if (global) OrderedDistribution(sortOrder) :: Nil else UnspecifiedDistribution :: Nil

  @transient
  lazy val ordering = new RowOrdering(sortOrder)

  def execute() = attachTree(this, "sort") {
    // TODO: Optimize sorting operation?
    child.execute()
      .mapPartitions(
        iterator => iterator.map(_.copy()).toArray.sorted(ordering).iterator,
        preservesPartitioning = true)
  }

  def output = child.output
}

object ExistingRdd {
  def convertToCatalyst(a: Any): Any = a match {
    case s: Seq[Any] => s.map(convertToCatalyst)
    case p: Product => new GenericRow(p.productIterator.map(convertToCatalyst).toArray)
    case other => other
  }

  def productToRowRdd[A <: Product](data: RDD[A]): RDD[Row] = {
    // TODO: Reuse the row, don't use map on the product iterator.  Maybe code gen?
    data.map(r => new GenericRow(r.productIterator.map(convertToCatalyst).toArray): Row)
  }

  def fromProductRdd[A <: Product : TypeTag](productRdd: RDD[A]) = {
    ExistingRdd(ScalaReflection.attributesFor[A], productToRowRdd(productRdd))
  }
}

case class ExistingRdd(output: Seq[Attribute], rdd: RDD[Row]) extends LeafNode {
  def execute() = rdd
}

