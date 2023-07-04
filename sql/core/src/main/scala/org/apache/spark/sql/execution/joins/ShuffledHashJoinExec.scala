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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.collection.{BitSet, OpenHashSet}

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
    right: SparkPlan,
    isSkewJoin: Boolean = false)
  extends HashJoin with ShuffledJoin {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"),
    "buildDataSize" -> SQLMetrics.createSizeMetric(sparkContext, "data size of build side"),
    "buildTime" -> SQLMetrics.createTimingMetric(sparkContext, "time to build hash map"))

  override def output: Seq[Attribute] = super[ShuffledJoin].output

  override def outputPartitioning: Partitioning = super[ShuffledJoin].outputPartitioning

  override def outputOrdering: Seq[SortOrder] = joinType match {
    // For outer joins where the outer side is build-side, order cannot be guaranteed.
    // The algorithm performs an additional un-ordered iteration on build-side (HashedRelation)
    // to find unmatched rows to satisfy the outer join semantic.
    case FullOuter => Nil
    case LeftOuter if buildSide == BuildLeft => Nil
    case RightOuter if buildSide == BuildRight => Nil
    case _ => super.outputOrdering
  }

  // Exposed for testing
  @transient lazy val ignoreDuplicatedKey = joinType match {
    case LeftExistence(_) =>
      // For building hash relation, ignore duplicated rows with same join keys if:
      // 1. Join condition is empty, or
      // 2. Join condition only references streamed attributes and build join keys.
      val streamedOutputAndBuildKeys = AttributeSet(streamedOutput ++ buildKeys)
      condition.forall(_.references.subsetOf(streamedOutputAndBuildKeys))
    case _ => false
  }

  /**
   * This is called by generated Java class, should be public.
   */
  def buildHashedRelation(iter: Iterator[InternalRow]): HashedRelation = {
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val start = System.nanoTime()
    val context = TaskContext.get()
    val relation = HashedRelation(
      iter,
      buildBoundKeys,
      taskMemoryManager = context.taskMemoryManager(),
      // build-side or full outer join needs support for NULL key in HashedRelation.
      allowsNullKey = joinType == FullOuter ||
        (joinType == LeftOuter && buildSide == BuildLeft) ||
        (joinType == RightOuter && buildSide == BuildRight),
      ignoresDuplicatedKey = ignoreDuplicatedKey)
    buildTime += NANOSECONDS.toMillis(System.nanoTime() - start)
    buildDataSize += relation.estimatedSize
    // This relation is usually used until the end of task.
    context.addTaskCompletionListener[Unit](_ => relation.close())
    relation
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")
    val buildDataSize = longMetric("buildDataSize")
    val buildTime = longMetric("buildTime")
    val evaluatorFactory = new ShuffledHashJoinEvaluatorFactory(
      output,
      condition,
      buildKeys,
      streamedKeys,
      buildPlan.output,
      streamedPlan.output,
      joinType,
      buildSide,
      streamedOutput,
      buildOutput,
      numOutputRows,
      buildDataSize,
      buildTime)
    if (conf.usePartitionEvaluator) {
      streamedPlan.execute().zipPartitionsWithEvaluator(buildPlan.execute(), evaluatorFactory)
    } else {
      streamedPlan.execute().zipPartitions(buildPlan.execute()) { (streamIter, buildIter) =>
        val evaluator = evaluatorFactory.createEvaluator()
        evaluator.eval(0, streamIter, buildIter)
      }
    }
  }

  override def supportCodegen: Boolean = joinType match {
    case FullOuter => conf.getConf(SQLConf.ENABLE_FULL_OUTER_SHUFFLED_HASH_JOIN_CODEGEN)
    case LeftOuter if buildSide == BuildLeft =>
      conf.getConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN)
    case RightOuter if buildSide == BuildRight =>
      conf.getConf(SQLConf.ENABLE_BUILD_SIDE_OUTER_SHUFFLED_HASH_JOIN_CODEGEN)
    case _ => true
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.execute() :: buildPlan.execute() :: Nil
  }

  override def needCopyResult: Boolean = true

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val thisPlan = ctx.addReferenceObj("plan", this)
    val clsName = classOf[HashedRelation].getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"$v = $thisPlan.buildHashedRelation(inputs[1]);", forceInline = true)
    HashedRelationInfo(relationTerm, keyIsUnique = false, isEmpty = false)
  }

  override def doProduce(ctx: CodegenContext): String = {
    // Specialize `doProduce` code for full outer join and build-side outer join,
    // because we need to iterate streamed and build side separately.
    val specializedProduce = joinType match {
      case FullOuter => true
      case LeftOuter if buildSide == BuildLeft => true
      case RightOuter if buildSide == BuildRight => true
      case _ => false
    }
    if (!specializedProduce) {
      return super.doProduce(ctx)
    }

    val HashedRelationInfo(relationTerm, _, _) = prepareRelation(ctx)

    // Inline mutable state since not many join operations in a task
    val keyIsUnique = ctx.addMutableState("boolean", "keyIsUnique",
      v => s"$v = $relationTerm.keyIsUnique();", forceInline = true)
    val streamedInput = ctx.addMutableState("scala.collection.Iterator", "streamedInput",
      v => s"$v = inputs[0];", forceInline = true)
    val buildInput = ctx.addMutableState("scala.collection.Iterator", "buildInput",
      v => s"$v = $relationTerm.valuesWithKeyIndex();", forceInline = true)
    val streamedRow = ctx.addMutableState("InternalRow", "streamedRow", forceInline = true)
    val buildRow = ctx.addMutableState("InternalRow", "buildRow", forceInline = true)

    // Generate variables and related code from streamed side
    val streamedVars = genOneSideJoinVars(ctx, streamedRow, streamedPlan, setDefaultValue = false)
    val streamedKeyVariables = evaluateRequiredVariables(streamedOutput, streamedVars,
      AttributeSet.fromAttributeSets(streamedKeys.map(_.references)))
    ctx.currentVars = streamedVars
    val streamedKeyExprCode = GenerateUnsafeProjection.createCode(ctx, streamedBoundKeys)
    val streamedKeyEv =
      s"""
         |$streamedKeyVariables
         |${streamedKeyExprCode.code}
       """.stripMargin
    val streamedKeyAnyNull = s"${streamedKeyExprCode.value}.anyNull()"

    // Generate code for join condition
    val (_, conditionCheck, _) =
      getJoinCondition(ctx, streamedVars, streamedPlan, buildPlan, Some(buildRow))

    // Generate code for result output in separate function, as we need to output result from
    // multiple places in join code.
    val streamedResultVars = genOneSideJoinVars(
      ctx, streamedRow, streamedPlan, setDefaultValue = true)
    val buildResultVars = genOneSideJoinVars(
      ctx, buildRow, buildPlan, setDefaultValue = true)
    val resultVars = buildSide match {
      case BuildLeft => buildResultVars ++ streamedResultVars
      case BuildRight => streamedResultVars ++ buildResultVars
    }
    val consumeOuterJoinRow = ctx.freshName("consumeOuterJoinRow")
    ctx.addNewFunction(consumeOuterJoinRow,
      s"""
         |private void $consumeOuterJoinRow() throws java.io.IOException {
         |  ${metricTerm(ctx, "numOutputRows")}.add(1);
         |  ${consume(ctx, resultVars)}
         |}
       """.stripMargin)

    val isFullOuterJoin = joinType == FullOuter
    val joinWithUniqueKey = codegenBuildSideOrFullOuterJoinWithUniqueKey(
      ctx, (streamedRow, buildRow), (streamedInput, buildInput), streamedKeyEv, streamedKeyAnyNull,
      streamedKeyExprCode.value, relationTerm, conditionCheck, consumeOuterJoinRow,
      isFullOuterJoin)
    val joinWithNonUniqueKey = codegenBuildSideOrFullOuterJoinNonUniqueKey(
      ctx, (streamedRow, buildRow), (streamedInput, buildInput), streamedKeyEv, streamedKeyAnyNull,
      streamedKeyExprCode.value, relationTerm, conditionCheck, consumeOuterJoinRow,
      isFullOuterJoin)

    s"""
       |if ($keyIsUnique) {
       |  $joinWithUniqueKey
       |} else {
       |  $joinWithNonUniqueKey
       |}
     """.stripMargin
  }

  /**
   * Generates the code for build-side or full outer join with unique join keys.
   * This is code-gen version of `buildSideOrFullOuterJoinUniqueKey()`.
   */
  private def codegenBuildSideOrFullOuterJoinWithUniqueKey(
      ctx: CodegenContext,
      rows: (String, String),
      inputs: (String, String),
      streamedKeyEv: String,
      streamedKeyAnyNull: String,
      streamedKeyValue: ExprValue,
      relationTerm: String,
      conditionCheck: String,
      consumeOuterJoinRow: String,
      isFullOuterJoin: Boolean): String = {
    // Inline mutable state since not many join operations in a task
    val matchedKeySetClsName = classOf[BitSet].getName
    val matchedKeySet = ctx.addMutableState(matchedKeySetClsName, "matchedKeySet",
      v => s"$v = new $matchedKeySetClsName($relationTerm.maxNumKeysIndex());", forceInline = true)
    val rowWithIndexClsName = classOf[ValueRowWithKeyIndex].getName
    val rowWithIndex = ctx.freshName("rowWithIndex")
    val foundMatch = ctx.freshName("foundMatch")
    val (streamedRow, buildRow) = rows
    val (streamedInput, buildInput) = inputs

    val joinStreamSide =
      s"""
         |while ($streamedInput.hasNext()) {
         |  $streamedRow = (InternalRow) $streamedInput.next();
         |
         |  // generate join key for stream side
         |  $streamedKeyEv
         |
         |  // find matches from HashedRelation
         |  boolean $foundMatch = false;
         |  $buildRow = null;
         |  $rowWithIndexClsName $rowWithIndex = $streamedKeyAnyNull ? null:
         |    $relationTerm.getValueWithKeyIndex($streamedKeyValue);
         |
         |  if ($rowWithIndex != null) {
         |    $buildRow = $rowWithIndex.getValue();
         |    // check join condition
         |    $conditionCheck {
         |      // set key index in matched keys set
         |      $matchedKeySet.set($rowWithIndex.getKeyIndex());
         |      $foundMatch = true;
         |    }
         |
         |    if (!$foundMatch) {
         |      $buildRow = null;
         |    }
         |  }
         |
         |  if ($foundMatch || $isFullOuterJoin) {
         |    $consumeOuterJoinRow();
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    val filterBuildSide =
      s"""
         |$streamedRow = null;
         |
         |// find non-matched rows from HashedRelation
         |while ($buildInput.hasNext()) {
         |  $rowWithIndexClsName $rowWithIndex = ($rowWithIndexClsName) $buildInput.next();
         |
         |  // check if key index is not in matched keys set
         |  if (!$matchedKeySet.get($rowWithIndex.getKeyIndex())) {
         |    $buildRow = $rowWithIndex.getValue();
         |    $consumeOuterJoinRow();
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    s"""
       |$joinStreamSide
       |$filterBuildSide
     """.stripMargin
  }

  /**
   * Generates the code for build-side or full outer join with non-unique join keys.
   * This is code-gen version of `buildSideOrFullOuterJoinNonUniqueKey()`.
   */
  private def codegenBuildSideOrFullOuterJoinNonUniqueKey(
      ctx: CodegenContext,
      rows: (String, String),
      inputs: (String, String),
      streamedKeyEv: String,
      streamedKeyAnyNull: String,
      streamedKeyValue: ExprValue,
      relationTerm: String,
      conditionCheck: String,
      consumeOuterJoinRow: String,
      isFullOuterJoin: Boolean): String = {
    // Inline mutable state since not many join operations in a task
    val matchedRowSetClsName = classOf[OpenHashSet[_]].getName
    val matchedRowSet = ctx.addMutableState(matchedRowSetClsName, "matchedRowSet",
      v => s"$v = new $matchedRowSetClsName(scala.reflect.ClassTag$$.MODULE$$.Long());",
      forceInline = true)
    val prevKeyIndex = ctx.addMutableState("int", "prevKeyIndex",
      v => s"$v = -1;", forceInline = true)
    val valueIndex = ctx.addMutableState("int", "valueIndex",
      v => s"$v = -1;", forceInline = true)
    val rowWithIndexClsName = classOf[ValueRowWithKeyIndex].getName
    val rowWithIndex = ctx.freshName("rowWithIndex")
    val buildIterator = ctx.freshName("buildIterator")
    val foundMatch = ctx.freshName("foundMatch")
    val keyIndex = ctx.freshName("keyIndex")
    val (streamedRow, buildRow) = rows
    val (streamedInput, buildInput) = inputs

    val rowIndex = s"(((long)$keyIndex) << 32) | $valueIndex"

    val joinStreamSide =
      s"""
         |while ($streamedInput.hasNext()) {
         |  $streamedRow = (InternalRow) $streamedInput.next();
         |
         |  // generate join key for stream side
         |  $streamedKeyEv
         |
         |  // find matches from HashedRelation
         |  boolean $foundMatch = false;
         |  $buildRow = null;
         |  scala.collection.Iterator $buildIterator = $streamedKeyAnyNull ? null:
         |    $relationTerm.getWithKeyIndex($streamedKeyValue);
         |
         |  int $valueIndex = -1;
         |  while ($buildIterator != null && $buildIterator.hasNext()) {
         |    $rowWithIndexClsName $rowWithIndex = ($rowWithIndexClsName) $buildIterator.next();
         |    int $keyIndex = $rowWithIndex.getKeyIndex();
         |    $buildRow = $rowWithIndex.getValue();
         |    $valueIndex++;
         |
         |    // check join condition
         |    $conditionCheck {
         |      // set row index in matched row set
         |      $matchedRowSet.add($rowIndex);
         |      $foundMatch = true;
         |      $consumeOuterJoinRow();
         |    }
         |  }
         |
         |  if (!$foundMatch) {
         |    $buildRow = null;
         |    if ($isFullOuterJoin) {
         |      $consumeOuterJoinRow();
         |    }
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    val filterBuildSide =
      s"""
         |$streamedRow = null;
         |
         |// find non-matched rows from HashedRelation
         |while ($buildInput.hasNext()) {
         |  $rowWithIndexClsName $rowWithIndex = ($rowWithIndexClsName) $buildInput.next();
         |  int $keyIndex = $rowWithIndex.getKeyIndex();
         |  if ($prevKeyIndex == -1 || $keyIndex != $prevKeyIndex) {
         |    $valueIndex = 0;
         |    $prevKeyIndex = $keyIndex;
         |  } else {
         |    $valueIndex += 1;
         |  }
         |
         |  // check if row index is not in matched row set
         |  if (!$matchedRowSet.contains($rowIndex)) {
         |    $buildRow = $rowWithIndex.getValue();
         |    $consumeOuterJoinRow();
         |  }
         |
         |  if (shouldStop()) return;
         |}
       """.stripMargin

    s"""
       |$joinStreamSide
       |$filterBuildSide
     """.stripMargin
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): ShuffledHashJoinExec =
    copy(left = newLeft, right = newRight)
}
