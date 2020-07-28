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

import java.util.concurrent.TimeUnit._

import org.apache.spark.TaskContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.BuildSide
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs a hash join of two child relations by first shuffling the data using the join keys.
 */
case class ShuffledHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends HashJoin with ShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning

  /**
   * This is called by generated Java class, should be public.
   */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(
      iter, buildBoundKeys, taskMemoryManager = context.taskMemoryManager())
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener[Unit](_ => relation.close())
    relation
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
      val hashed = buildHashedRelation(buildIter)
      join(streamIter, hashed, numOutputRows)
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.execute() :: buildPlan.execute() :: Nil
  }

  override def needCopyResult: Boolean = true

  override protected def doProduce(ctx: CodegenContext): String = {
    // inline mutable state since not many join operations in a task
    val streamedInput = ctx.addMutableState(
      "scala.collection.Iterator", "streamedInput", v => s"$v = inputs[0];", forceInline = true)
    val buildInput = ctx.addMutableState(
      "scala.collection.Iterator", "buildInput", v => s"$v = inputs[1];", forceInline = true)
    val initRelation = ctx.addMutableState(
      CodeGenerator.JAVA_BOOLEAN, "initRelation", v => s"$v = false;", forceInline = true)
    val streamedRow = ctx.addMutableState(
      "InternalRow", "streamedRow", forceInline = true)

    val thisPlan = ctx.addReferenceObj("plan", this)
    val (relationTerm, _) = prepareRelation(ctx)
    val buildRelation = s"$relationTerm = $thisPlan.buildHashedRelation($buildInput);"
    val (streamInputVar, streamInputVarDecl) = createVars(ctx, streamedRow, streamedPlan.output)

    val join = joinType match {
      case _: InnerLike => codegenInner(ctx, streamInputVar)
      case LeftOuter | RightOuter => codegenOuter(ctx, streamInputVar)
      case LeftSemi => codegenSemi(ctx, streamInputVar)
      case LeftAnti => codegenAnti(ctx, streamInputVar)
      case _: ExistenceJoin => codegenExistence(ctx, streamInputVar)
      case x =>
        throw new IllegalArgumentException(
          s"ShuffledHashJoin should not take $x as the JoinType")
    }

    s"""
       |// construct hash map for shuffled hash join build side
       |if (!$initRelation) {
       |  $buildRelation
       |  $initRelation = true;
       |}
       |
       |while ($streamedInput.hasNext()) {
       |  $streamedRow = (InternalRow) $streamedInput.next();
       |  ${streamInputVarDecl.mkString("\n")}
       |  $join
       |
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }

  /**
   * Returns a tuple of variable name for HashedRelation,
   * and boolean false to indicate key not to be known unique in code-gen time.
   */
  protected override def prepareRelation(ctx: CodegenContext): (String, Boolean) = {
    if (relationTerm == null) {
      // Inline mutable state since not many join operations in a task
      relationTerm = ctx.addMutableState(
        "org.apache.spark.sql.execution.joins.HashedRelation", "relation", forceInline = true)
    }
    (relationTerm, false)
  }

  private var relationTerm: String = _
}
