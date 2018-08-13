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

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.{Partition, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ZippedPartitionsRDD2}
import org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper
import org.apache.spark.sql.catalyst.expressions.{Add, And, Attribute, AttributeReference, AttributeSet, BoundReference, Cast, CheckOverflow, Expression, ExpressionSet, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, Literal, Multiply, NamedExpression, PreciseTimestampConversion, PredicateHelper, Subtract, TimeAdd, TimeSub, UnaryMinus}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.WatermarkSupport.watermarkExpression
import org.apache.spark.sql.execution.streaming.state.{StateStoreCoordinatorRef, StateStoreProvider, StateStoreProviderId}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.CalendarInterval


/**
 * Helper object for [[StreamingSymmetricHashJoinExec]]. See that object for more details.
 */
object StreamingSymmetricHashJoinHelper extends Logging {

  sealed trait JoinSide
  case object LeftSide extends JoinSide { override def toString(): String = "left" }
  case object RightSide extends JoinSide { override def toString(): String = "right" }

  sealed trait JoinStateWatermarkPredicate {
    def expr: Expression
    def desc: String
    override def toString: String = s"$desc: $expr"
  }
  /** Predicate for watermark on state keys */
  case class JoinStateKeyWatermarkPredicate(expr: Expression)
    extends JoinStateWatermarkPredicate {
    def desc: String = "key predicate"
  }
  /** Predicate for watermark on state values */
  case class JoinStateValueWatermarkPredicate(expr: Expression)
    extends JoinStateWatermarkPredicate {
    def desc: String = "value predicate"
  }

  case class JoinStateWatermarkPredicates(
    left: Option[JoinStateWatermarkPredicate] = None,
    right: Option[JoinStateWatermarkPredicate] = None) {
    override def toString(): String = {
      s"state cleanup [ left ${left.map(_.toString).getOrElse("= null")}, " +
        s"right ${right.map(_.toString).getOrElse("= null")} ]"
    }
  }

  /**
   * Wrapper around various useful splits of the join condition.
   * left AND right AND joined is equivalent to full.
   *
   * Note that left and right do not necessarily contain *all* conjuncts which satisfy
   * their condition.
   *
   * @param leftSideOnly Deterministic conjuncts which reference only the left side of the join.
   * @param rightSideOnly Deterministic conjuncts which reference only the right side of the join.
   * @param bothSides Conjuncts which are nondeterministic, occur after a nondeterministic conjunct,
   *                  or reference both left and right sides of the join.
   * @param full The full join condition.
   */
  case class JoinConditionSplitPredicates(
      leftSideOnly: Option[Expression],
      rightSideOnly: Option[Expression],
      bothSides: Option[Expression],
      full: Option[Expression]) {
    override def toString(): String = {
      s"condition = [ leftOnly = ${leftSideOnly.map(_.toString).getOrElse("null")}, " +
        s"rightOnly = ${rightSideOnly.map(_.toString).getOrElse("null")}, " +
        s"both = ${bothSides.map(_.toString).getOrElse("null")}, " +
        s"full = ${full.map(_.toString).getOrElse("null")} ]"
    }
  }

  object JoinConditionSplitPredicates extends PredicateHelper {
    def apply(condition: Option[Expression], left: SparkPlan, right: SparkPlan):
        JoinConditionSplitPredicates = {
      // Split the condition into 3 parts:
      // * Conjuncts that can be evaluated on only the left input.
      // * Conjuncts that can be evaluated on only the right input.
      // * Conjuncts that require both left and right input.
      //
      // Note that we treat nondeterministic conjuncts as though they require both left and right
      // input. To maintain their semantics, they need to be evaluated exactly once per joined row.
      val (leftCondition, rightCondition, joinedCondition) = {
        if (condition.isEmpty) {
          (None, None, None)
        } else {
          // Span rather than partition, because nondeterministic expressions don't commute
          // across AND.
          val (deterministicConjuncts, nonDeterministicConjuncts) =
            splitConjunctivePredicates(condition.get).partition(_.deterministic)

          val (leftConjuncts, nonLeftConjuncts) = deterministicConjuncts.partition { cond =>
            cond.references.subsetOf(left.outputSet)
          }

          val (rightConjuncts, nonRightConjuncts) = deterministicConjuncts.partition { cond =>
            cond.references.subsetOf(right.outputSet)
          }

          (
            leftConjuncts.reduceOption(And),
            rightConjuncts.reduceOption(And),
            (nonLeftConjuncts.intersect(nonRightConjuncts) ++ nonDeterministicConjuncts)
              .reduceOption(And)
          )
        }
      }

      JoinConditionSplitPredicates(leftCondition, rightCondition, joinedCondition, condition)
    }
  }

  /** Get the predicates defining the state watermarks for both sides of the join */
  def getStateWatermarkPredicates(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      eventTimeWatermark: Option[Long]): JoinStateWatermarkPredicates = {


    // Join keys of both sides generate rows of the same fields, that is, same sequence of data
    // types. If one side (say left side) has a column (say timestamp) that has a watermark on it,
    // then it will never consider joining keys that are < state key watermark (i.e. event time
    // watermark). On the other side (i.e. right side), even if there is no watermark defined,
    // there has to be an equivalent column (i.e., timestamp). And any right side data that has the
    // timestamp < watermark will not match will not match with left side data, as the left side get
    // filtered with the explicitly defined watermark. So, the watermark in timestamp column in
    // left side keys effectively causes the timestamp on the right side to have a watermark.
    // We will use the ordinal of the left timestamp in the left keys to find the corresponding
    // right timestamp in the right keys.
    val joinKeyOrdinalForWatermark: Option[Int] = {
      leftKeys.zipWithIndex.collectFirst {
        case (ne: NamedExpression, index) if ne.metadata.contains(delayKey) => index
      } orElse {
        rightKeys.zipWithIndex.collectFirst {
          case (ne: NamedExpression, index) if ne.metadata.contains(delayKey) => index
        }
      }
    }

    def getOneSideStateWatermarkPredicate(
        oneSideInputAttributes: Seq[Attribute],
        oneSideJoinKeys: Seq[Expression],
        otherSideInputAttributes: Seq[Attribute]): Option[JoinStateWatermarkPredicate] = {
      val isWatermarkDefinedOnInput = oneSideInputAttributes.exists(_.metadata.contains(delayKey))
      val isWatermarkDefinedOnJoinKey = joinKeyOrdinalForWatermark.isDefined

      if (isWatermarkDefinedOnJoinKey) { // case 1 and 3 in the StreamingSymmetricHashJoinExec docs
        val keyExprWithWatermark = BoundReference(
          joinKeyOrdinalForWatermark.get,
          oneSideJoinKeys(joinKeyOrdinalForWatermark.get).dataType,
          oneSideJoinKeys(joinKeyOrdinalForWatermark.get).nullable)
        val expr = watermarkExpression(Some(keyExprWithWatermark), eventTimeWatermark)
        expr.map(JoinStateKeyWatermarkPredicate.apply _)

      } else if (isWatermarkDefinedOnInput) { // case 2 in the StreamingSymmetricHashJoinExec docs
        val stateValueWatermark = StreamingJoinHelper.getStateValueWatermark(
          attributesToFindStateWatermarkFor = AttributeSet(oneSideInputAttributes),
          attributesWithEventWatermark = AttributeSet(otherSideInputAttributes),
          condition,
          eventTimeWatermark)
        val inputAttributeWithWatermark = oneSideInputAttributes.find(_.metadata.contains(delayKey))
        val expr = watermarkExpression(inputAttributeWithWatermark, stateValueWatermark)
        expr.map(JoinStateValueWatermarkPredicate.apply _)

      } else {
        None
      }
    }

    val leftStateWatermarkPredicate =
      getOneSideStateWatermarkPredicate(leftAttributes, leftKeys, rightAttributes)
    val rightStateWatermarkPredicate =
      getOneSideStateWatermarkPredicate(rightAttributes, rightKeys, leftAttributes)
    JoinStateWatermarkPredicates(leftStateWatermarkPredicate, rightStateWatermarkPredicate)
  }

  /**
   * A custom RDD that allows partitions to be "zipped" together, while ensuring the tasks'
   * preferred location is based on which executors have the required join state stores already
   * loaded. This is class is a modified version of [[ZippedPartitionsRDD2]].
   */
  class StateStoreAwareZipPartitionsRDD[A: ClassTag, B: ClassTag, V: ClassTag](
      sc: SparkContext,
      f: (Iterator[A], Iterator[B]) => Iterator[V],
      rdd1: RDD[A],
      rdd2: RDD[B],
      stateInfo: StatefulOperatorStateInfo,
      stateStoreNames: Seq[String],
      @transient private val storeCoordinator: Option[StateStoreCoordinatorRef])
      extends ZippedPartitionsRDD2[A, B, V](sc, f, rdd1, rdd2) {

    /**
     * Set the preferred location of each partition using the executor that has the related
     * [[StateStoreProvider]] already loaded.
     */
    override def getPreferredLocations(partition: Partition): Seq[String] = {
      stateStoreNames.flatMap { storeName =>
        val stateStoreProviderId = StateStoreProviderId(stateInfo, partition.index, storeName)
        storeCoordinator.flatMap(_.getLocation(stateStoreProviderId))
      }.distinct
    }
  }

  implicit class StateStoreAwareZipPartitionsHelper[T: ClassTag](dataRDD: RDD[T]) {
    /**
     * Function used by `StreamingSymmetricHashJoinExec` to zip together the partitions of two
     * child RDDs for joining the data in corresponding partitions, while ensuring the tasks'
     * preferred location is based on which executors have the required join state stores already
     * loaded.
     */
    def stateStoreAwareZipPartitions[U: ClassTag, V: ClassTag](
        dataRDD2: RDD[U],
        stateInfo: StatefulOperatorStateInfo,
        storeNames: Seq[String],
        storeCoordinator: StateStoreCoordinatorRef
      )(f: (Iterator[T], Iterator[U]) => Iterator[V]): RDD[V] = {
      new StateStoreAwareZipPartitionsRDD(
        dataRDD.sparkContext, f, dataRDD, dataRDD2, stateInfo, storeNames, Some(storeCoordinator))
    }
  }
}
