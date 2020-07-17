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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf

private case class NotInSubqueryHashJoinParams(
    buildSideRelation: HashedRelation,
    buildSideIsNullExists: Boolean,
    buildSideIsEmpty: Boolean,
    streamedSideKey: Attribute,
    streamedSideKeyIndex: Int)

object NotInSubqueryConditionPattern {
  def singleColumnPatternMatch(condition: Option[Expression]): (Boolean, Attribute, Attribute) = {
    condition.get match {
      case Or(EqualTo(leftAttr: AttributeReference, rightAttr: AttributeReference),
        IsNull(EqualTo(tmpLeft: AttributeReference, tmpRight: AttributeReference)))
          if leftAttr.semanticEquals(tmpLeft) && rightAttr.semanticEquals(tmpRight) =>
        (true, leftAttr, rightAttr)
      case _ => (false, null, null)
    }
  }
}

case class BroadcastNullAwareHashJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends BaseJoinExec {

  // TODO support multi column not in subquery in future.
  // See. http://www.vldb.org/pvldb/vol2/vldb09-423.pdf Section 6
  // multi-column null aware anti join is much more complicated than single column ones.
  require(right.output.length == 1, "not in subquery hash join optimize only single column.")
  require(joinType == LeftAnti, "joinType must be LeftAnti.")
  require(buildSide == BuildRight, "buildSide must be BuildRight.")
  require(SQLConf.get.notInSubqueryHashJoinEnabled,
    "notInSubqueryHashJoinEnabled must turn on for BroadcastNullAwareHashJoinExec.")

  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val (streamed, broadcast) = (left, right)

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ${buildSide} ($opId)".trim
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  private[this] def genResultProjection: UnsafeProjection = {
    UnsafeProjection.create(output, output)
  }

  override def output: Seq[Attribute] = {
    left.output
  }

  private def buildParams(buildSideRows: Array[InternalRow]): NotInSubqueryHashJoinParams = {
    val (_, leftAttr, rightAttr) =
      NotInSubqueryConditionPattern.singleColumnPatternMatch(condition)

    val leftNotInKeyFound = left.output.exists(_.semanticEquals(leftAttr))
    val (streamedKey, streamedKeyIndex) = if (leftNotInKeyFound) {
      (leftAttr, AttributeSeq(left.output).indexOf(leftAttr.exprId))
    } else {
      (rightAttr, AttributeSeq(left.output).indexOf(rightAttr.exprId))
    }

    NotInSubqueryHashJoinParams(
      HashedRelation(buildSideRows.iterator,
        BindReferences.bindReferences[Expression](
          Seq(right.output.head), AttributeSeq(right.output)),
        buildSideRows.length),
      buildSideRows.exists(row => row.isNullAt(0)),
      buildSideRows.isEmpty,
      streamedKey,
      streamedKeyIndex
    )
  }

  private def leftAntiNullAwareJoin(
      relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    val params = buildParams(relation.value)
    streamed.execute().mapPartitionsInternal { streamedIter =>
      if (params.buildSideIsEmpty) {
        streamedIter
      } else if (params.buildSideIsNullExists) {
        Iterator.empty
      } else {
        val keyGenerator = UnsafeProjection.create(
          BindReferences.bindReferences[Expression](
            Seq(params.streamedSideKey),
            AttributeSeq(left.output))
        )
        streamedIter.filter(row => {
          val streamedRowIsNull = row.isNullAt(params.streamedSideKeyIndex)
          val lookupRow: UnsafeRow = keyGenerator(row)
          val notInKeyEqual = params.buildSideRelation.get(lookupRow) != null
          if (!streamedRowIsNull && !notInKeyEqual) {
            true
          } else {
            false
          }
        })
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (LeftAnti, BuildRight) =>
        leftAntiNullAwareJoin(broadcastedRelation)
      case _ =>
        throw new IllegalArgumentException(
          s"BroadcastNullAwareHashJoinExec support (LeftAnti + BuildRight) only for now.")
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }
}
