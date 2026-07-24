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

import java.io.InterruptedIOException
import java.nio.channels.ClosedByInterruptException
import java.util.IdentityHashMap
import java.util.concurrent.CancellationException

import scala.collection.mutable
import scala.util.control.{ControlThrowable, NonFatal}

import org.apache.spark.TaskKilledException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, Expression, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ResultQueryStageExec}
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeLike, ReusedExchangeExec}
import org.apache.spark.sql.execution.joins.{EmptyHashedRelation, HashedRelation, HashedRelationWithAllNullKeys, LongHashedRelation}

private[sql] case class BroadcastValueProjectionLimits(
    maxInputRows: Long,
    maxOutputBytes: Long,
    maxSourceBytes: Long)

private[sql] sealed trait BroadcastValueResult[+T]

private[sql] object BroadcastValueResult {
  case class Available[T](value: T) extends BroadcastValueResult[T]
  case object Unavailable extends BroadcastValueResult[Nothing]
}

/** Projects a complete, bounded value domain from rows already stored in a hash broadcast. */
private[sql] object BroadcastValueProjector {
  import BroadcastValueResult._

  def collectExactValueDomain(
      broadcast: Broadcast[HashedRelation],
      child: SparkPlan,
      valueExpression: Expression,
      limits: BroadcastValueProjectionLimits,
      onInputRow: () => Unit,
      onError: () => Unit): BroadcastValueResult[Array[InternalRow]] = {
    try {
      if (sourceBroadcastCannotBeSafelyRehydrated(child, limits)) {
        return Unavailable
      }

      val broadcastRelation = broadcast.value
      if (broadcastRelation == HashedRelationWithAllNullKeys) {
        return Unavailable
      }
      if (broadcastRelation == EmptyHashedRelation) {
        return Available(Array.empty[InternalRow])
      }

      val relation = broadcastRelation.asReadOnlyCopy()
      val projection = UnsafeProjection.create(
        BindReferences.bindReference(valueExpression, child.output))
      val projectedRows = mutable.LinkedHashSet.empty[UnsafeRow]
      val valueRows = relation match {
        case longRelation: LongHashedRelation =>
          longRelation.keys().flatMap { key =>
            val rows = longRelation.get(key)
            if (rows == null) Iterator.empty else rows
          }
        case otherRelation =>
          otherRelation.valuesWithKeyIndex().map(_.getValue)
      }
      var visitedRows = 0L
      var projectedBytes = 0L

      while (valueRows.hasNext) {
        if (visitedRows >= limits.maxInputRows) {
          return Unavailable
        }
        val valueRow = valueRows.next()
        visitedRows += 1
        onInputRow()
        val projected = projection(valueRow)
        if (!projected.isNullAt(0) && !projectedRows.contains(projected)) {
          val rowSize = projected.getSizeInBytes.toLong
          if (rowSize > limits.maxOutputBytes - projectedBytes) {
            return Unavailable
          }
          projectedBytes += rowSize
          projectedRows += projected.copy()
        }
      }

      Available(projectedRows.toArray[InternalRow])
    } catch {
      case NonFatal(error) if !mustPropagateFailure(error) =>
        onError()
        Unavailable
    }
  }

  private def sourceBroadcastRuntimeStatistics(plan: SparkPlan): Option[Statistics] = plan match {
    case exchange: BroadcastExchangeLike => Some(exchange.runtimeStatistics)
    case ReusedExchangeExec(_, exchange: BroadcastExchangeLike) =>
      Some(exchange.runtimeStatistics)
    case stage: BroadcastQueryStageExec => Some(stage.getRuntimeStatistics)
    case stage: ResultQueryStageExec =>
      sourceBroadcastRuntimeStatistics(stage.plan)
    case adaptive: AdaptiveSparkPlanExec =>
      sourceBroadcastRuntimeStatistics(adaptive.executedPlan)
    case _ => None
  }

  private def sourceBroadcastCannotBeSafelyRehydrated(
      child: SparkPlan,
      limits: BroadcastValueProjectionLimits): Boolean = {
    val maxRows = BigInt(limits.maxInputRows)
    val maxSourceBytes = BigInt(limits.maxSourceBytes)
    sourceBroadcastRuntimeStatistics(child) match {
      case Some(statistics) =>
        statistics.rowCount.forall(_ > maxRows) || statistics.sizeInBytes > maxSourceBytes
      case None => true
    }
  }

  private[sql] def mustPropagateFailure(error: Throwable): Boolean = {
    if (Thread.currentThread().isInterrupted) {
      return true
    }

    val seen = new IdentityHashMap[Throwable, java.lang.Boolean]()
    var current = error
    while (current != null && !seen.containsKey(current)) {
      seen.put(current, java.lang.Boolean.TRUE)
      current match {
        case _: InterruptedException | _: InterruptedIOException |
            _: ClosedByInterruptException | _: CancellationException | _: TaskKilledException |
            _: VirtualMachineError | _: ThreadDeath | _: LinkageError | _: ControlThrowable =>
          return true
        case _ => current = current.getCause
      }
    }
    false
  }
}
