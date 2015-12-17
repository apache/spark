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
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeRowJoiner, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{UnspecifiedDistribution, ClusteredDistribution, AllTuples, Distribution}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{StructField, StructType, DoubleType, LongType}
import org.apache.spark.sql.{Strategy, SQLContext, Column, DataFrame}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, UnaryNode}
import org.apache.spark.sql.execution.{SparkPlanner, SparkPlan}

package object state {

  object StateStore extends Logging {
    private val checkpoints =
      new scala.collection.mutable.HashMap[(Int, StreamProgress), StateStore]()

    /** Checkpoints the state for a partition at a given batchId. */
    def checkpoint(id: StreamProgress, store: StateStore): Unit = synchronized {
      val partitionId = TaskContext.get().partitionId()
      logInfo(s"Checkpointing ${(partitionId, id)}")
      checkpoints.put((partitionId, id), store)
    }

    /** Gets the state for a partition at a given batchId. */
    def getAt(id: StreamProgress): StateStore = synchronized {
      val partitionId = TaskContext.get().partitionId()

      if (id.isEmpty) {
        new StateStore
      } else {
        val copy = new StateStore
        checkpoints((partitionId, id)).data.foreach(copy.data.+=)
        copy
      }
    }
  }

  class StateStore {
    private val data = new scala.collection.mutable.HashMap[InternalRow, Long]()

    def get(key: InternalRow): Option[Long] = data.get(key)

    def put(key: InternalRow, value: Long): Unit = {
      data.put(key, value)
    }

    def triggerWindowsBefore(Offset: Long): Seq[(InternalRow, Long)] = {
      val triggeredWindows = data.filter(_._1.getLong(0) <= Offset).toArray
      triggeredWindows.map(_._1).foreach(data.remove)
      triggeredWindows.toSeq
    }
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

  implicit object MaxOffset extends AccumulatorParam[LongOffset] {
    /**
     * Add additional data to the accumulator value. Is allowed to modify and return `r`
     * for efficiency (to avoid allocating objects).
     *
     * @param r the current value of the accumulator
     * @param t the data to be added to the accumulator
     * @return the new value of the accumulator
     */
    override def addAccumulator(r: LongOffset, t: LongOffset): LongOffset =
      if (r > t) r else t

    /**
     * Merge two accumulated values together. Is allowed to modify and return the first value
     * for efficiency (to avoid allocating objects).
     *
     * @param r1 one set of accumulated data
     * @param r2 another set of accumulated data
     * @return both data sets merged together
     */
    override def addInPlace(r1: LongOffset, r2: LongOffset): LongOffset =
      if (r1 > r2) r1 else r2

    /**
     * Return the "zero" (identity) value for an accumulator type, given its initial value. For
     * example, if R was a vector of N dimensions, this would return a vector of N zeroes.
     */
    override def zero(initialValue: LongOffset): LongOffset =
      LongOffset(-1)
  }

  /**
   * Simple incremental windowing aggregate that supports a fixed delay.  Late data is dropped.
   * Only supports grouped count(*) :P.
   */
  case class WindowAggregate(
      eventtime: Expression,
      eventtimeMax: Accumulator[LongOffset],
      eventtimeOffset: LongOffset,
      lastCheckpoint: StreamProgress,
      nextCheckpoint: StreamProgress,
      windowAttribute: AttributeReference,
      step: Int,
      groupingExpressions: Seq[Expression],
      aggregateExpressions: Seq[NamedExpression],
      child: SparkPlan) extends SparkPlan {

    override def requiredChildDistribution: Seq[Distribution] =
      ClusteredDistribution(groupingExpressions.filterNot(_ == windowAttribute)) :: Nil

    /**
     * Overridden by concrete implementations of SparkPlan.
     * Produces the result of the query as an RDD[InternalRow]
     */
    override protected def doExecute(): RDD[InternalRow] = {
      child.execute().mapPartitions { iter =>
        val stateStore = StateStore.getAt(lastCheckpoint)

        // TODO: Move to window operator...
        val window =
          Multiply(Ceil(Divide(Cast(eventtime, DoubleType), Literal(step))), Literal(step))
        val windowAndGroupProjection =
          GenerateUnsafeProjection.generate(
            window +: groupingExpressions.filterNot(_ == windowAttribute), child.output)

        iter.foreach { row =>
          val windowAndGroup = windowAndGroupProjection(row)
          if (windowAndGroup.getLong(0) > eventtimeOffset.offset) {
            eventtimeMax += LongOffset(windowAndGroup.getLong(0))
            val newCount = stateStore.get(windowAndGroup).getOrElse(0L) + 1
            stateStore.put(windowAndGroup.copy(), newCount)
          }
        }

        StateStore.checkpoint(nextCheckpoint, stateStore)

        val buildRow = GenerateUnsafeProjection.generate(
          BoundReference(0, LongType, false) ::
          BoundReference(1, LongType, false) ::
          BoundReference(2, LongType, false) :: Nil)
        val joinedRow = new JoinedRow
        val countRow = new SpecificMutableRow(LongType :: Nil)

        logDebug(s"Triggering windows < $eventtimeOffset")
        stateStore.triggerWindowsBefore(eventtimeOffset.offset).toIterator.map {
          case (key, count) =>
            countRow.setLong(0, count)
            buildRow(joinedRow(key, countRow))
        }
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


  object GroupWindows extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      case Aggregate(grouping, aggregate,
              Window(et, windowAttribute, step, trigger, child))
          if !grouping.contains(windowAttribute) =>
        Aggregate(windowAttribute +: grouping, windowAttribute +: aggregate,
          Window(et, windowAttribute, step, trigger, child))
    }
  }


  class StatefulPlanner(
      sqlContext: SQLContext,
      maxOffset: Accumulator[LongOffset],
      lastCheckpoint: StreamProgress,
      nextCheckpoint: StreamProgress)
    extends SparkPlanner(sqlContext) {

    override def strategies: Seq[Strategy] = WindowStrategy +: super.strategies

    object WindowStrategy extends Strategy with Logging {
      def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
        case Aggregate(grouping, aggregate,
               Window(et, windowAttribute, step, triggerDelay, child)) =>
          WindowAggregate(
            et,
            maxOffset,
            maxOffset.value - triggerDelay,
            lastCheckpoint,
            nextCheckpoint,
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