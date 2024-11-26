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

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.{RDD, ZippedPartitionsBaseRDD, ZippedPartitionsPartition}
import org.apache.spark.sql.catalyst.analysis.StreamingJoinHelper
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeSet, BoundReference, Expression, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.streaming.WatermarkSupport.watermarkExpression
import org.apache.spark.sql.execution.streaming.state.{StateStoreCheckpointInfo, StateStoreCoordinatorRef, StateStoreProviderId}


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

  def getStateWatermark(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      eventTimeWatermarkForEviction: Option[Long],
      allowMultipleEventTimeColumns: Boolean): (Option[Long], Option[Long]) = {

    // Perform assertions against multiple event time columns in the same DataFrame. This method
    // assumes there is only one event time column per each side (left / right) and it is not very
    // clear to reason about the correctness if there are multiple event time columns. Disallow to
    // be conservative.
    WatermarkSupport.findEventTimeColumn(leftAttributes,
      allowMultipleEventTimeColumns = allowMultipleEventTimeColumns)
    WatermarkSupport.findEventTimeColumn(rightAttributes,
      allowMultipleEventTimeColumns = allowMultipleEventTimeColumns)

    val joinKeyOrdinalForWatermark: Option[Int] = findJoinKeyOrdinalForWatermark(
      leftKeys, rightKeys)

    def getOneSideStateWatermark(
        oneSideInputAttributes: Seq[Attribute],
        otherSideInputAttributes: Seq[Attribute]): Option[Long] = {
      val isWatermarkDefinedOnInput = oneSideInputAttributes.exists(_.metadata.contains(delayKey))
      val isWatermarkDefinedOnJoinKey = joinKeyOrdinalForWatermark.isDefined

      if (isWatermarkDefinedOnJoinKey) { // case 1 and 3 in the StreamingSymmetricHashJoinExec docs
        eventTimeWatermarkForEviction
      } else if (isWatermarkDefinedOnInput) { // case 2 in the StreamingSymmetricHashJoinExec docs
        StreamingJoinHelper.getStateValueWatermark(
          attributesToFindStateWatermarkFor = AttributeSet(oneSideInputAttributes),
          attributesWithEventWatermark = AttributeSet(otherSideInputAttributes),
          condition,
          eventTimeWatermarkForEviction)
      } else {
        None
      }
    }

    val leftStateWatermark = getOneSideStateWatermark(leftAttributes, rightAttributes)
    val rightStateWatermark = getOneSideStateWatermark(rightAttributes, leftAttributes)

    (leftStateWatermark, rightStateWatermark)
  }

  /** Get the predicates defining the state watermarks for both sides of the join */
  def getStateWatermarkPredicates(
      leftAttributes: Seq[Attribute],
      rightAttributes: Seq[Attribute],
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      condition: Option[Expression],
      eventTimeWatermarkForEviction: Option[Long],
      useFirstEventTimeColumn: Boolean): JoinStateWatermarkPredicates = {

    // Perform assertions against multiple event time columns in the same DataFrame. This method
    // assumes there is only one event time column per each side (left / right) and it is not very
    // clear to reason about the correctness if there are multiple event time columns. Disallow to
    // be conservative.
    WatermarkSupport.findEventTimeColumn(leftAttributes,
      allowMultipleEventTimeColumns = useFirstEventTimeColumn)
    WatermarkSupport.findEventTimeColumn(rightAttributes,
      allowMultipleEventTimeColumns = useFirstEventTimeColumn)

    val joinKeyOrdinalForWatermark: Option[Int] = findJoinKeyOrdinalForWatermark(
      leftKeys, rightKeys)

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
        val expr = watermarkExpression(Some(keyExprWithWatermark), eventTimeWatermarkForEviction)
        expr.map(JoinStateKeyWatermarkPredicate.apply _)

      } else if (isWatermarkDefinedOnInput) { // case 2 in the StreamingSymmetricHashJoinExec docs
        val stateValueWatermark = StreamingJoinHelper.getStateValueWatermark(
          attributesToFindStateWatermarkFor = AttributeSet(oneSideInputAttributes),
          attributesWithEventWatermark = AttributeSet(otherSideInputAttributes),
          condition,
          eventTimeWatermarkForEviction)
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

  private def findJoinKeyOrdinalForWatermark(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression]): Option[Int] = {
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
    leftKeys.zipWithIndex.collectFirst {
      case (ne: NamedExpression, index) if ne.metadata.contains(delayKey) => index
    } orElse {
      rightKeys.zipWithIndex.collectFirst {
        case (ne: NamedExpression, index) if ne.metadata.contains(delayKey) => index
      }
    }
  }

  /**
   * A custom RDD that allows partitions to be "zipped" together, while ensuring the tasks'
   * preferred location is based on which executors have the required join state stores already
   * loaded. This class is a variant of [[org.apache.spark.rdd.ZippedPartitionsRDD2]] which only
   * changes signature of `f`.
   */
  class StateStoreAwareZipPartitionsRDD[A: ClassTag, B: ClassTag, V: ClassTag](
      sc: SparkContext,
      var f: (Int, Iterator[A], Iterator[B]) => Iterator[V],
      var rdd1: RDD[A],
      var rdd2: RDD[B],
      stateInfo: StatefulOperatorStateInfo,
      stateStoreNames: Seq[String],
      @transient private val storeCoordinator: Option[StateStoreCoordinatorRef])
      extends ZippedPartitionsBaseRDD[V](sc, List(rdd1, rdd2)) {

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

    override def compute(s: Partition, context: TaskContext): Iterator[V] = {
      val partitions = s.asInstanceOf[ZippedPartitionsPartition].partitions
      if (partitions(0).index != partitions(1).index) {
        throw new IllegalStateException(s"Partition ID should be same in both side: " +
          s"left ${partitions(0).index} , right ${partitions(1).index}")
      }

      val partitionId = partitions(0).index
      f(partitionId, rdd1.iterator(partitions(0), context), rdd2.iterator(partitions(1), context))
    }

    override def clearDependencies(): Unit = {
      super.clearDependencies()
      rdd1 = null
      rdd2 = null
      f = null
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
      )(f: (Int, Iterator[T], Iterator[U]) => Iterator[V]): RDD[V] = {
      new StateStoreAwareZipPartitionsRDD(
        dataRDD.sparkContext, f, dataRDD, dataRDD2, stateInfo, storeNames, Some(storeCoordinator))
    }
  }

  case class JoinerStateStoreCkptInfo(
      keyToNumValues: StateStoreCheckpointInfo,
      valueToNumKeys: StateStoreCheckpointInfo)

  case class JoinStateStoreCkptInfo(
      left: JoinerStateStoreCkptInfo,
      right: JoinerStateStoreCkptInfo)

  case class JoinerStateStoreCheckpointId(
       keyToNumValues: Option[String],
       valueToNumKeys: Option[String])

  case class JoinStateStoreCheckpointId(
       left: JoinerStateStoreCheckpointId,
       right: JoinerStateStoreCheckpointId)

}
