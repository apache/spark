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

import java.util.concurrent.{Future => JFuture}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.Duration

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.joins.{HashedRelation, HashJoin, LongHashedRelation}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.util.ThreadUtils

/**
 * Physical plan for a custom subquery that collects and transforms the broadcast key values.
 * This subquery retrieves the partition keys from the broadcast results based on the type of
 * [[HashedRelation]] returned. If a key is packed inside a Long, we extract it through
 * bitwise operations, otherwise we return it from the appropriate index of the [[UnsafeRow]].
 *
 * @param indices the indices of the join keys in the list of keys from the build side
 * @param buildKeys the join keys from the build side of the join used
 * @param child the BroadcastExchange or the AdaptiveSparkPlan with BroadcastQueryStageExec
 *              from the build side of the join
 */
case class SubqueryBroadcastExec(
    name: String,
    indices: Seq[Int],
    buildKeys: Seq[Expression],
    child: SparkPlan) extends BaseSubqueryExec with UnaryExecNode {

  // `SubqueryBroadcastExec` is only used with `InSubqueryExec`. No one would reference this output,
  // so the exprId doesn't matter here. But it's important to correctly report the output length, so
  // that `InSubqueryExec` can know whether it's the single-column or multi-column execution mode.
  override def output: Seq[Attribute] = {
    indices.map { idx =>
      val key = buildKeys(idx)
      val name = key match {
        case n: NamedExpression => n.name
        case Cast(n: NamedExpression, _, _, _) => n.name
        case _ => s"key_$idx"
      }
      AttributeReference(name, key.dataType, key.nullable)()
    }
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "dataSize" -> SQLMetrics.createMetric(sparkContext, "data size (bytes)"),
    "collectTime" -> SQLMetrics.createMetric(sparkContext, "time to collect (ms)"))

  override def doCanonicalize(): SparkPlan = {
    val keys = buildKeys.map(k => QueryPlan.normalizeExpressions(k, child.output))
    SubqueryBroadcastExec("dpp", indices, keys, child.canonicalized)
  }

  @transient
  private lazy val relationFuture: JFuture[Array[InternalRow]] = {
    // relationFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLExecution.withThreadLocalCaptured[Array[InternalRow]](
      session, SubqueryBroadcastExec.executionContext) {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(session, executionId) {
        val beforeCollect = System.nanoTime()

        val broadcastRelation = child.executeBroadcast[HashedRelation]().value
        val exprs = if (broadcastRelation.isInstanceOf[LongHashedRelation]) {
          indices.map { idx => HashJoin.extractKeyExprAt(buildKeys, idx) }
        } else {
          indices.map { idx =>
            BoundReference(idx, buildKeys(idx).dataType, buildKeys(idx).nullable) }
        }

        val proj = UnsafeProjection.create(exprs)
        val iter = broadcastRelation.keys()
        val keyIter = iter.map(proj).map(_.copy())

        val rows = if (broadcastRelation.keyIsUnique) {
          keyIter.toArray[InternalRow]
        } else {
          keyIter.toArray[InternalRow].distinct
        }
        val beforeBuild = System.nanoTime()
        longMetric("collectTime") += (beforeBuild - beforeCollect) / 1000000
        val dataSize = rows.map(_.asInstanceOf[UnsafeRow].getSizeInBytes).sum
        longMetric("numOutputRows") += rows.length
        longMetric("dataSize") += dataSize
        SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)

        rows
      }
    }
  }

  protected override def doPrepare(): Unit = {
    relationFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    throw QueryExecutionErrors.executeCodePathUnsupportedError("SubqueryBroadcastExec")
  }

  override def executeCollect(): Array[InternalRow] = {
    ThreadUtils.awaitResult(relationFuture, Duration.Inf)
  }

  override def stringArgs: Iterator[Any] = super.stringArgs ++ Iterator(s"[id=#$id]")

  override protected def withNewChildInternal(newChild: SparkPlan): SubqueryBroadcastExec =
    copy(child = newChild)
}

object SubqueryBroadcastExec {
  private[execution] val executionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("dynamicpruning",
      SQLConf.get.getConf(StaticSQLConf.BROADCAST_EXCHANGE_MAX_THREAD_THRESHOLD)))
}
