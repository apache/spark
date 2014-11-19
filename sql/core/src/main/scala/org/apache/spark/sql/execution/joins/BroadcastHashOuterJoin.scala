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

import java.util.{HashMap => JavaHashMap}

import scala.collection.JavaConversions._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.collection.CompactBuffer


/**
 * :: DeveloperApi ::
 * Performs a hash based outer join for two child relations by shuffling the data using
 * the join keys. This operator requires loading the associated partition in both side into memory.
 */
@DeveloperApi
case class BroadcastHashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode {

  private val (streamedPlan, broadcastPlan) = joinType match {
    case LeftOuter => (left, right)
    case RightOuter => (right, left)
  }

  private val (streamedKeys, broadcastKeys) = joinType match {
    case LeftOuter => (leftKeys, rightKeys)
    case RightOuter => (rightKeys, leftKeys)
  }

  override def outputPartitioning: Partitioning = joinType match {
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case x => throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
  }

  override def output = {
    joinType match {
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case x =>
        throw new Exception(s"HashOuterJoin should not take $x as the JoinType")
    }
  }

  @transient private[this] lazy val BroadcastSideKeyGenerator: Projection =
    newProjection(broadcastKeys, broadcastPlan.output)


  @transient private[this] lazy val streamSideKeyGenerator: () => MutableProjection =
    newMutableProjection(streamedKeys, streamedPlan.output)

  // TODO we need to rewrite all of the iterators with our own implementation instead of the Scala
  // iterator for performance purpose.

  @transient private[this] lazy val DUMMY_LIST = Seq[Row](null)
  @transient private[this] lazy val EMPTY_LIST = Seq.empty[Row]


  private[this] def buildHashTable(
      iter: Iterator[Row], keyGenerator: Projection): JavaHashMap[Row, CompactBuffer[Row]] = {
    val hashTable = new JavaHashMap[Row, CompactBuffer[Row]]()
    while (iter.hasNext) {
      val currentRow = iter.next()
      val rowKey = keyGenerator(currentRow)

      var existingMatchList = hashTable.get(rowKey)
      if (existingMatchList == null) {
        existingMatchList = new CompactBuffer[Row]()
        hashTable.put(rowKey, existingMatchList)
      }

      existingMatchList += currentRow.copy()
    }

    hashTable
  }

  override def execute() = {

    val input: Array[Row] = broadcastPlan.execute().map(_.copy()).collect()
    val hashed = buildHashTable(input.iterator, BroadcastSideKeyGenerator)
    val broadcastHashTable = sparkContext.broadcast(hashed)

    val boundCondition =
      condition.map(newPredicate(_, left.output ++ right.output)).getOrElse((row: Row) => true)

    streamedPlan.execute().mapPartitions {
      streamedIter =>
        var i = 0
        val joinedRow = new JoinedRow
        val joinKey = streamSideKeyGenerator()
        val broadcastNulls = new GenericMutableRow(broadcastPlan.output.size)

        streamedIter.flatMap {
          streamedRow =>
            val rowKey = joinKey(streamedRow)
            val broadcastRow = broadcastHashTable.value.getOrElse(rowKey, EMPTY_LIST)
            joinType match {
              case LeftOuter =>
                joinedRow.withLeft(streamedRow)
                var matched = false
                //val rightNullRow = new GenericRow(right.output.length)
                (if (!rowKey.anyNull) broadcastRow.collect {
                  case r if (boundCondition(joinedRow.withRight(r))) =>
                    matched = true
                    joinedRow.copy
                } else {
                  Nil
                }) ++ DUMMY_LIST.filter(_ => !matched).map(_ => {
                  // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
                  // as we don't know whether we need to append it until finish iterating all of the
                  // records in right side.
                  // If we didn't get any proper row, then append a single row with empty right
                  joinedRow.withRight(broadcastNulls).copy
                })

              case RightOuter =>
                joinedRow.withRight(streamedRow)
                var matched = false
                //val leftNullRow = new GenericRow(left.output.length)
                (if (!rowKey.anyNull) broadcastRow.collect {
                  case r if (boundCondition(joinedRow.withLeft(r))) =>
                    matched = true
                    joinedRow.copy
                } else {
                  Nil
                }) ++ DUMMY_LIST.filter(_ => !matched).map(_ => {
                  // DUMMY_LIST.filter(_ => !matched) is a tricky way to add additional row,
                  // as we don't know whether we need to append it until finish iterating all of the
                  // records in right side.
                  // If we didn't get any proper row, then append a single row with empty right
                  joinedRow.withRight(broadcastNulls).copy
                })

              case _ => Nil
            }
        }

    }
  }
}

