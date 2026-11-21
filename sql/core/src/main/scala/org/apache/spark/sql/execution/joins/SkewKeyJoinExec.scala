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

package org.apache.spark.sql.execution.joins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryExecNode, FilterExec, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.exchange.EnsureRequirements
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Joins two sides on equi-join keys, where some keys are so frequent on both sides that all their
 * output rows would end up in a single task of a shuffled join. The shuffle blocks of such a key
 * can all be small, as the key is spread thinly over many mappers, so adaptive skew join handling,
 * which looks at the sizes of the shuffle blocks, does not help.
 *
 * This operator samples the skewed side to find the hot keys, and then splits the join in two:
 *  - the rows of the hot keys of both sides are joined with a [[BroadcastHashJoinExec]], which
 *    streams the skewed side in its existing partitioning, so that the output of a hot key is
 *    produced by all the tasks that hold its rows, and
 *  - all the other rows are joined with a [[SortMergeJoinExec]].
 * The results are unioned. As every row is joined in exactly one of the two branches, this gives
 * the same result as a single join. The rows with a null key are never hot, as they do not match
 * any row, and so are only handled by the sort merge join.
 *
 * Each side is read twice, once per branch, so both sides must be deterministic. The rows of the
 * hot keys of the side that is not skewed are broadcast, so this is expected to be a small number.
 * If no key is hot, the operator only runs the sort merge join.
 *
 * @param leftKeys the left join keys.
 * @param rightKeys the right join keys.
 * @param skewKeyIndices the indices of the join keys to find the hot keys on. These keys of the
 *                       skewed side must be attributes of the supported key types, and the
 *                       corresponding keys of the other side must have the same types.
 * @param joinType the type of the join: inner, or an outer join whose preserved side is the
 *                 skewed side, as the other side is broadcast.
 * @param condition the non equi-join condition.
 * @param skewLeft whether the left side is the skewed side, otherwise the right side is.
 */
case class SkewKeyJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    skewKeyIndices: Seq[Int],
    joinType: JoinType,
    condition: Option[Expression],
    skewLeft: Boolean,
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode {

  override lazy val metrics = Map(
    "numHotKeys" -> SQLMetrics.createMetric(sparkContext, "number of hot keys"))

  // The output of the join is the same for all the join strategies.
  override lazy val output: Seq[Attribute] = sortMergeJoin(left, right).output

  private def skewChild: SparkPlan = if (skewLeft) left else right

  private def skewSideKeys: Seq[Expression] = {
    val keys = if (skewLeft) leftKeys else rightKeys
    skewKeyIndices.map(keys)
  }

  private def otherSideKeys: Seq[Expression] = {
    val keys = if (skewLeft) rightKeys else leftKeys
    skewKeyIndices.map(keys)
  }

  private def sortMergeJoin(l: SparkPlan, r: SparkPlan): SparkPlan = {
    SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, l, r)
  }

  private def hotKeyFilter(keys: Seq[Expression], hotKeys: Set[Seq[Any]], hot: Boolean)
    : SparkPlan => SparkPlan = {
    FilterExec(SkewHotKeyFilter(keys, hotKeys, hot), _)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    val hotKeys = findHotKeys()
    longMetric("numHotKeys").set(hotKeys.size)
    val plan = if (hotKeys.isEmpty) {
      sortMergeJoin(left, right)
    } else {
      val (hotSkew, coldSkew) = (hotKeyFilter(skewSideKeys, hotKeys, hot = true),
        hotKeyFilter(skewSideKeys, hotKeys, hot = false))
      val (hotOther, coldOther) = (hotKeyFilter(otherSideKeys, hotKeys, hot = true),
        hotKeyFilter(otherSideKeys, hotKeys, hot = false))
      val (hotLeft, hotRight, coldLeft, coldRight) = if (skewLeft) {
        (hotSkew(left), hotOther(right), coldSkew(left), coldOther(right))
      } else {
        (hotOther(left), hotSkew(right), coldOther(left), coldSkew(right))
      }
      // The skewed side is streamed and the other side is broadcast.
      val buildSide = if (skewLeft) BuildRight else BuildLeft
      UnionExec(Seq(
        BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, buildSide, condition, hotLeft, hotRight),
        sortMergeJoin(coldLeft, coldRight)))
    }
    // The shuffles and the broadcast of the two branches are planned here, as which keys are hot
    // is only known once the skewed side has been sampled.
    EnsureRequirements().apply(plan).execute()
  }

  /**
   * Samples the skewed side to find the keys that are hot. A key is hot if its estimated number
   * of rows is at least `spark.sql.skewJoinHint.minHotKeyRows` and at least the average number of
   * rows of a shuffle partition, so that all the rows of the key alone fill a task. At most
   * `spark.sql.skewJoinHint.maxHotKeys` of the most frequent keys are returned.
   */
  private def findHotKeys(): Set[Seq[Any]] = {
    val fraction = conf.getConf(SQLConf.SKEW_JOIN_HINT_SAMPLE_FRACTION)
    val minHotKeyRows = conf.getConf(SQLConf.SKEW_JOIN_HINT_MIN_HOT_KEY_ROWS)
    val maxHotKeys = conf.getConf(SQLConf.SKEW_JOIN_HINT_MAX_HOT_KEYS)
    val numPartitions = conf.numShufflePartitions

    val keys = skewSideKeys
    val keyTypes = keys.map(_.dataType)
    val childOutput = skewChild.output
    val sampledRows = sparkContext.longAccumulator("sampled rows of the skewed side")

    val keyCounts = skewChild.execute()
      .sample(withReplacement = false, fraction, SkewKeyJoinExec.SAMPLE_SEED)
      .mapPartitionsInternal { iter =>
        val project = UnsafeProjection.create(keys, childOutput)
        iter.flatMap { row =>
          sampledRows.add(1L)
          val keyRow = project(row)
          if ((0 until keys.length).exists(keyRow.isNullAt)) {
            // A null key matches no row, so it is never hot.
            Iterator.empty
          } else {
            val key: Seq[Any] = keyRow.toSeq(keyTypes).map(InternalRow.copyValue).toVector
            Iterator.single((key, 1L))
          }
        }
      }
      .reduceByKey(_ + _)

    val candidates = keyCounts
      .filter { case (_, count) => count / fraction >= minHotKeyRows }
      .top(maxHotKeys)(Ordering.by[(Seq[Any], Long), Long](_._2))

    val estimatedRows = sampledRows.value / fraction
    val threshold = math.max(minHotKeyRows.toDouble, estimatedRows / numPartitions)
    candidates.collect { case (key, count) if count / fraction >= threshold => key }.toSet
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): SkewKeyJoinExec =
    copy(left = newLeft, right = newRight)
}

object SkewKeyJoinExec {
  // Fixed so that the hot keys do not change between runs on the same data.
  private val SAMPLE_SEED = 42L

  /**
   * Whether the values of a key of this type can be compared by value in a hash set, and are equal
   * in the hash set if and only if they are equal in a join.
   */
  def isSupportedKeyType(dataType: DataType): Boolean = dataType match {
    case BooleanType | ByteType | ShortType | IntegerType | LongType | DateType |
         TimestampType | TimestampNTZType | _: DecimalType => true
    case s: StringType => s.supportsBinaryEquality
    case _ => false
  }

  /**
   * Whether the join type can be split on the hot keys of the given side, which is the side that
   * is streamed in the broadcast hash join of the hot keys.
   */
  def supportsJoinType(joinType: JoinType, skewLeft: Boolean): Boolean = joinType match {
    case Inner => true
    case LeftOuter => skewLeft
    case RightOuter => !skewLeft
    case _ => false
  }
}

/**
 * Tests whether the values of `keys` are one of the `hotKeys`, if `hot` is true, or none of them,
 * otherwise. A null value is never a hot key, so the rows with a null key are always cold.
 */
case class SkewHotKeyFilter(keys: Seq[Expression], hotKeys: Set[Seq[Any]], hot: Boolean)
  extends Predicate with CodegenFallback {

  override def nullable: Boolean = false

  override def children: Seq[Expression] = keys

  override def eval(input: InternalRow): Any = {
    val key = keys.map(_.eval(input))
    val isHot = !key.contains(null) && hotKeys.contains(key)
    isHot == hot
  }

  override def toString: String = s"SkewHotKeyFilter(${keys.mkString(", ")}, hot=$hot)"

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): SkewHotKeyFilter = copy(keys = newChildren)
}
