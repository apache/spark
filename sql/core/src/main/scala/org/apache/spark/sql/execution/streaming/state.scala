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
package org.apache.spark.sql.execution.streaming

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{Strategy, SQLContext, Column, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlanner, SparkPlan}

package object state {

  trait StateStore {
    def get(key: InternalRow): InternalRow
  }

  case class Window(
      eventtime: Expression,
      windowAttribute: AttributeReference,
      step: Int,
      closingTriggerDelay: Int,
      child: LogicalPlan) extends UnaryNode {
    override def output: Seq[Attribute] = windowAttribute +: child.output
    override def missingInput: AttributeSet = super.missingInput - windowAttribute
  }

  implicit object MaxWatermark extends AccumulatorParam[Watermark] {
    /**
     * Add additional data to the accumulator value. Is allowed to modify and return `r`
     * for efficiency (to avoid allocating objects).
     *
     * @param r the current value of the accumulator
     * @param t the data to be added to the accumulator
     * @return the new value of the accumulator
     */
    override def addAccumulator(r: Watermark, t: Watermark): Watermark = if (r > t) r else t

    /**
     * Merge two accumulated values together. Is allowed to modify and return the first value
     * for efficiency (to avoid allocating objects).
     *
     * @param r1 one set of accumulated data
     * @param r2 another set of accumulated data
     * @return both data sets merged together
     */
    override def addInPlace(r1: Watermark, r2: Watermark): Watermark = if (r1 > r2) r1 else r2

    /**
     * Return the "zero" (identity) value for an accumulator type, given its initial value. For
     * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
     */
    override def zero(initialValue: Watermark): Watermark = new Watermark(-1)
  }

  case class WindowAggregate(
      eventtime: Expression,
      eventtimeMax: Accumulator[Watermark],
      eventtimeWatermark: Watermark,
      windowAttribute: AttributeReference,
      step: Int,
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      child: SparkPlan) extends SparkPlan {
    /**
     * Overridden by concrete implementations of SparkPlan.
     * Produces the result of the query as an RDD[InternalRow]
     */
    override protected def doExecute(): RDD[InternalRow] = {
      child.execute().mapPartitions { iter =>
        val window =
          Multiply(Ceil(Divide(Cast(eventtime, DoubleType), Literal(step))), Literal(step))
        val windowAndGroupProjection =
          GenerateUnsafeProjection.generate(window +: groupingExpressions, child.output)
        iter.foreach { row =>
          val windowAndGroup = windowAndGroupProjection(row)
          println(windowAndGroup.toSeq((windowAttribute +: groupingExpressions).map(_.dataType)))

          eventtimeMax += new Watermark(windowAndGroup.getLong(0))
        }

        Iterator.empty
      }
    }

    override def output: Seq[Attribute] =
      windowAttribute +: aggregateExpressions.map(_.toAttribute)

    override def missingInput: AttributeSet = super.missingInput - windowAttribute

    /**
     * Returns a Seq of the children of this node.
     * Children should not change. Immutability required for containsChild optimization
     */
    override def children: Seq[SparkPlan] = child :: Nil
  }

  implicit class StatefulDataFrame(df: DataFrame) {
    def window(eventTime: Column, step: Int, closingTriggerDelay: Int): DataFrame =
      df.withPlan(
        Window(
          eventTime.expr,
          new AttributeReference("window", LongType)(),
          step,
          closingTriggerDelay,
          df.logicalPlan))
  }

  class StatefulPlanner(sqlContext: SQLContext, maxWatermark: Accumulator[Watermark])
      extends SparkPlanner(sqlContext) {

    override def strategies: Seq[Strategy] = WindowStrategy +: super.strategies

    object WindowStrategy extends Strategy with Logging {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case Aggregate(grouping, aggregate,
               Window(et, windowAttribute, step, trigger, child)) =>
          println(s"triggering ${maxWatermark.value - trigger}")

          WindowAggregate(
            et,
            maxWatermark,
            maxWatermark.value - trigger,
            windowAttribute,
            step,
            grouping,
            aggregate,
            planLater(child)) :: Nil

        case other => Nil
      }
    }
  }
}