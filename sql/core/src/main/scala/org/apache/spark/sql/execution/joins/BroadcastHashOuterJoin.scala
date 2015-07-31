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

import scala.concurrent._
import scala.concurrent.duration._

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, UnspecifiedDistribution}
import org.apache.spark.sql.catalyst.plans.{JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.execution.{BinaryNode, SparkPlan}
import org.apache.spark.util.ThreadUtils

/**
 * :: DeveloperApi ::
 * Performs a outer hash join for two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
@DeveloperApi
case class BroadcastHashOuterJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryNode with HashOuterJoin {

  val timeout = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  @transient
  private val broadcastFuture = future {
    // Note that we use .execute().collect() because we don't want to convert data to Scala types
    val input: Array[InternalRow] = buildPlan.execute().map(_.copy()).collect()
    val hashed = HashedRelation(input.iterator, buildKeyGenerator, input.size)
    sparkContext.broadcast(hashed)
  }(BroadcastHashOuterJoin.broadcastHashOuterJoinExecutionContext)

  override def doExecute(): RDD[InternalRow] = {
    val broadcastRelation = Await.result(broadcastFuture, timeout)

    streamedPlan.execute().mapPartitions { streamedIter =>
      val joinedRow = new JoinedRow()
      val hashTable = broadcastRelation.value
      val keyGenerator = streamedKeyGenerator

      joinType match {
        case LeftOuter =>
          streamedIter.flatMap(currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashTable.get(rowKey))
          })

        case RightOuter =>
          streamedIter.flatMap(currentRow => {
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashTable.get(rowKey), joinedRow)
          })

        case x =>
          throw new IllegalArgumentException(
            s"BroadcastHashOuterJoin should not take $x as the JoinType")
      }
    }
  }
}

object BroadcastHashOuterJoin {

  private val broadcastHashOuterJoinExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-hash-outer-join", 128))
}
