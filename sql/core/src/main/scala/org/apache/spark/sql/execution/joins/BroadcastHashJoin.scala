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

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BindReferences, BoundReference, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryNode, CodegenSupport, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ThreadUtils
import org.apache.spark.util.collection.CompactBuffer

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcasted relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class BroadcastHashJoin(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin with CodegenSupport {

  override private[sql] lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createLongMetric(sparkContext, "number of output rows"))

  val timeout: Duration = {
    val timeoutValue = sqlContext.conf.broadcastTimeout
    if (timeoutValue < 0) {
      Duration.Inf
    } else {
      timeoutValue.seconds
    }
  }

  override def outputPartitioning: Partitioning = streamedPlan.outputPartitioning

  override def requiredChildDistribution: Seq[Distribution] =
    UnspecifiedDistribution :: UnspecifiedDistribution :: Nil

  // Use lazy so that we won't do broadcast when calling explain but still cache the broadcast value
  // for the same query.
  @transient
  private lazy val broadcastFuture = {
    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // Note that we use .execute().collect() because we don't want to convert data to Scala
        // types
        val input: Array[InternalRow] = buildPlan.execute().map { row =>
          row.copy()
        }.collect()
        // The following line doesn't run in a job so we cannot track the metric value. However, we
        // have already tracked it in the above lines. So here we can use
        // `SQLMetrics.nullLongMetric` to ignore it.
        // TODO: move this check into HashedRelation
        val hashed = if (canJoinKeyFitWithinLong) {
          LongHashedRelation(
            input.iterator, buildSideKeyGenerator, input.size)
        } else {
          HashedRelation(
            input.iterator, buildSideKeyGenerator, input.size)
        }
        sparkContext.broadcast(hashed)
      }
    }(BroadcastHashJoin.broadcastHashJoinExecutionContext)
  }

  protected override def doPrepare(): Unit = {
    broadcastFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = Await.result(broadcastFuture, timeout)

    streamedPlan.execute().mapPartitions { streamedIter =>
      val hashedRelation = broadcastRelation.value
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashedRelation.getMemorySize)
      hashJoin(streamedIter, hashedRelation, numOutputRows)
    }
  }

  private var broadcastRelation: Broadcast[HashedRelation] = _
  // the term for hash relation
  private var relationTerm: String = _

  override def upstream(): RDD[InternalRow] = {
    streamedPlan.asInstanceOf[CodegenSupport].upstream()
  }

  override def doProduce(ctx: CodegenContext): String = {
    // create a name for HashedRelation
    broadcastRelation = Await.result(broadcastFuture, timeout)
    val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
    relationTerm = ctx.freshName("relation")
    val clsName = broadcastRelation.value.getClass.getName
    ctx.addMutableState(clsName, relationTerm,
      s"""
         | $relationTerm = ($clsName) $broadcast.value();
         | incPeakExecutionMemory($relationTerm.getMemorySize());
       """.stripMargin)

    s"""
       | ${streamedPlan.asInstanceOf[CodegenSupport].produce(ctx, this)}
     """.stripMargin
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    // generate the key as UnsafeRow or Long
    ctx.currentVars = input
    val (keyVal, anyNull) = if (canJoinKeyFitWithinLong) {
      val expr = rewriteKeyExpr(streamedKeys).head
      val ev = BindReferences.bindReference(expr, streamedPlan.output).gen(ctx)
      (ev, ev.isNull)
    } else {
      val keyExpr = streamedKeys.map(BindReferences.bindReference(_, streamedPlan.output))
      val ev = GenerateUnsafeProjection.createCode(ctx, keyExpr)
      (ev, s"${ev.value}.anyNull()")
    }

    // find the matches from HashedRelation
    val matched = ctx.freshName("matched")

    // create variables for output
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    val buildColumns = buildPlan.output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable).gen(ctx)
    }
    val resultVars = buildSide match {
      case BuildLeft => buildColumns ++ input
      case BuildRight => input ++ buildColumns
    }

    val numOutput = metricTerm(ctx, "numOutputRows")
    val outputCode = if (condition.isDefined) {
      // filter the output via condition
      ctx.currentVars = resultVars
      val ev = BindReferences.bindReference(condition.get, this.output).gen(ctx)
      s"""
         | ${ev.code}
         | if (!${ev.isNull} && ${ev.value}) {
         |   $numOutput.add(1);
         |   ${consume(ctx, resultVars)}
         | }
       """.stripMargin
    } else {
      s"""
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    }

    if (broadcastRelation.value.isInstanceOf[UniqueHashedRelation]) {
      s"""
         | // generate join key
         | ${keyVal.code}
         | // find matches from HashedRelation
         | UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyVal.value});
         | if ($matched != null) {
         |   ${buildColumns.map(_.code).mkString("\n")}
         |   $outputCode
         | }
     """.stripMargin

    } else {
      val matches = ctx.freshName("matches")
      val bufferType = classOf[CompactBuffer[UnsafeRow]].getName
      val i = ctx.freshName("i")
      val size = ctx.freshName("size")
      s"""
         | // generate join key
         | ${keyVal.code}
         | // find matches from HashRelation
         | $bufferType $matches = ${anyNull} ? null :
         |  ($bufferType) $relationTerm.get(${keyVal.value});
         | if ($matches != null) {
         |   int $size = $matches.size();
         |   for (int $i = 0; $i < $size; $i++) {
         |     UnsafeRow $matched = (UnsafeRow) $matches.apply($i);
         |     ${buildColumns.map(_.code).mkString("\n")}
         |     $outputCode
         |   }
         | }
     """.stripMargin
    }
  }
}

object BroadcastHashJoin {

  private[joins] val broadcastHashJoinExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-hash-join", 128))
}
