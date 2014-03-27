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

package org.apache.spark.sql
package execution

import scala.collection.mutable.{ArrayBuffer, BitSet}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import catalyst.errors._
import catalyst.expressions._
import catalyst.plans._
import catalyst.plans.physical.{ClusteredDistribution, Partitioning}

sealed abstract class BuildSide
case object BuildLeft extends BuildSide
case object BuildRight extends BuildSide

object InterpretCondition {
  def apply(expression: Expression): (Row => Boolean) = {
    (r: Row) => expression.apply(r).asInstanceOf[Boolean]
  }
}

case class HashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode {

  override def outputPartitioning: Partitioning = left.outputPartitioning

  override def requiredChildDistribution =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  val (buildPlan, streamedPlan) = buildSide match {
    case BuildLeft => (left, right)
    case BuildRight => (right, left)
  }

  val (buildKeys, streamedKeys) = buildSide match {
    case BuildLeft => (leftKeys, rightKeys)
    case BuildRight => (rightKeys, leftKeys)
  }

  def output = left.output ++ right.output

  @transient lazy val buildSideKeyGenerator = new Projection(buildKeys, buildPlan.output)
  @transient lazy val streamSideKeyGenerator =
    () => new MutableProjection(streamedKeys, streamedPlan.output)

  def execute() = {

    buildPlan.execute().zipPartitions(streamedPlan.execute()) { (buildIter, streamIter) =>
      val hashTable = new java.util.HashMap[Row, ArrayBuffer[Row]]()
      var currentRow: Row = null

      // Create a mapping of buildKeys -> rows
      while(buildIter.hasNext) {
        currentRow = buildIter.next()
        val rowKey = buildSideKeyGenerator(currentRow)
        if(!rowKey.anyNull) {
          val existingMatchList = hashTable.get(rowKey)
          val matchList = if (existingMatchList == null) {
            val newMatchList = new ArrayBuffer[Row]()
            hashTable.put(rowKey, newMatchList)
            newMatchList
          } else {
            existingMatchList
          }
          matchList += currentRow.copy()
        }
      }

      new Iterator[Row] {
        private[this] var currentRow: Row = _
        private[this] var currentMatches: ArrayBuffer[Row] = _
        private[this] var currentPosition: Int = -1

        // Mutable per row objects.
        private[this] val joinRow = new JoinedRow

        @transient private val joinKeys = streamSideKeyGenerator()

        def hasNext: Boolean =
          (currentPosition != -1 && currentPosition < currentMatches.size) ||
          (streamIter.hasNext && fetchNext())

        def next() = {
          val ret = joinRow(currentRow, currentMatches(currentPosition))
          currentPosition += 1
          ret
        }

        private def fetchNext(): Boolean = {
          currentMatches = null
          currentPosition = -1

          while (currentMatches == null && streamIter.hasNext) {
            currentRow = streamIter.next()
            if(!joinKeys(currentRow).anyNull) {
              currentMatches = hashTable.get(joinKeys.currentValue)
            }
          }

          if (currentMatches == null) {
            false
          } else {
            currentPosition = 0
            true
          }
        }
      }
    }
  }
}

case class CartesianProduct(left: SparkPlan, right: SparkPlan) extends BinaryNode {
  def output = left.output ++ right.output

  def execute() = left.execute().map(_.copy()).cartesian(right.execute().map(_.copy())).map {
    case (l: Row, r: Row) => buildRow(l ++ r)
  }
}

case class BroadcastNestedLoopJoin(
    streamed: SparkPlan, broadcast: SparkPlan, joinType: JoinType, condition: Option[Expression])
    (@transient sc: SparkContext)
  extends BinaryNode {
  // TODO: Override requiredChildDistribution.

  override def outputPartitioning: Partitioning = streamed.outputPartitioning

  override def otherCopyArgs = sc :: Nil

  def output = left.output ++ right.output

  /** The Streamed Relation */
  def left = streamed
  /** The Broadcast relation */
  def right = broadcast

  @transient lazy val boundCondition =
    InterpretCondition(
      condition
        .map(c => BindReferences.bindReference(c, left.output ++ right.output))
        .getOrElse(Literal(true)))


  def execute() = {
    val broadcastedRelation = sc.broadcast(broadcast.execute().map(_.copy()).collect().toIndexedSeq)

    val streamedPlusMatches = streamed.execute().mapPartitions { streamedIter =>
      val matchedRows = new ArrayBuffer[Row]
      val includedBroadcastTuples =
        new scala.collection.mutable.BitSet(broadcastedRelation.value.size)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        var matched = false

        while (i < broadcastedRelation.value.size) {
          // TODO: One bitset per partition instead of per row.
          val broadcastedRow = broadcastedRelation.value(i)
          if (boundCondition(joinedRow(streamedRow, broadcastedRow))) {
            matchedRows += buildRow(streamedRow ++ broadcastedRow)
            matched = true
            includedBroadcastTuples += i
          }
          i += 1
        }

        if (!matched && (joinType == LeftOuter || joinType == FullOuter)) {
          matchedRows += buildRow(streamedRow ++ Array.fill(right.output.size)(null))
        }
      }
      Iterator((matchedRows, includedBroadcastTuples))
    }

    val includedBroadcastTuples = streamedPlusMatches.map(_._2)
    val allIncludedBroadcastTuples =
      if (includedBroadcastTuples.count == 0) {
        new scala.collection.mutable.BitSet(broadcastedRelation.value.size)
      } else {
        streamedPlusMatches.map(_._2).reduce(_ ++ _)
      }

    val rightOuterMatches: Seq[Row] =
      if (joinType == RightOuter || joinType == FullOuter) {
        broadcastedRelation.value.zipWithIndex.filter {
          case (row, i) => !allIncludedBroadcastTuples.contains(i)
        }.map {
          // TODO: Use projection.
          case (row, _) => buildRow(Vector.fill(left.output.size)(null) ++ row)
        }
      } else {
        Vector()
      }

    // TODO: Breaks lineage.
    sc.union(
      streamedPlusMatches.flatMap(_._1), sc.makeRDD(rightOuterMatches))
  }
}
