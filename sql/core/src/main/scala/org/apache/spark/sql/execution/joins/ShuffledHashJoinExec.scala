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
import org.apache.spark.sql.execution.{RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
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
    case FullOuter => Nil
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
      // Full outer join needs support for NULL key in HashedRelation.
      allowsNullKey = joinType == FullOuter,
      ignoresDuplicatedKey = ignoreDuplicatedKey)
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
      joinType match {
        case FullOuter => fullOuterJoin(streamIter, hashed, numOutputRows)
        case _ => join(streamIter, hashed, numOutputRows)
      }
    }
  }

  private def fullOuterJoin(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      numOutputRows: SQLMetric): Iterator[InternalRow] = {
    val joinKeys = streamSideKeyGenerator()
    val joinRow = new JoinedRow
    val (joinRowWithStream, joinRowWithBuild) = {
      buildSide match {
        case BuildLeft => (joinRow.withRight _, joinRow.withLeft _)
        case BuildRight => (joinRow.withLeft _, joinRow.withRight _)
      }
    }
    val buildNullRow = new GenericInternalRow(buildOutput.length)
    val streamNullRow = new GenericInternalRow(streamedOutput.length)
    lazy val streamNullJoinRowWithBuild = {
      buildSide match {
        case BuildLeft =>
          joinRow.withRight(streamNullRow)
          joinRow.withLeft _
        case BuildRight =>
          joinRow.withLeft(streamNullRow)
          joinRow.withRight _
      }
    }

    val iter = if (hashedRelation.keyIsUnique) {
      fullOuterJoinWithUniqueKey(streamIter, hashedRelation, joinKeys, joinRowWithStream,
        joinRowWithBuild, streamNullJoinRowWithBuild, buildNullRow)
    } else {
      fullOuterJoinWithNonUniqueKey(streamIter, hashedRelation, joinKeys, joinRowWithStream,
        joinRowWithBuild, streamNullJoinRowWithBuild, buildNullRow)
    }

    val resultProj = UnsafeProjection.create(output, output)
    iter.map { r =>
      numOutputRows += 1
      resultProj(r)
    }
  }

  /**
   * Full outer shuffled hash join with unique join keys:
   * 1. Process rows from stream side by looking up hash relation.
   *    Mark the matched rows from build side be looked up.
   *    A bit set is used to track matched rows with key index.
   * 2. Process rows from build side by iterating hash relation.
   *    Filter out rows from build side being matched already,
   *    by checking key index from bit set.
   */
  private def fullOuterJoinWithUniqueKey(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      joinKeys: UnsafeProjection,
      joinRowWithStream: InternalRow => JoinedRow,
      joinRowWithBuild: InternalRow => JoinedRow,
      streamNullJoinRowWithBuild: => InternalRow => JoinedRow,
      buildNullRow: GenericInternalRow): Iterator[InternalRow] = {
    val matchedKeys = new BitSet(hashedRelation.maxNumKeysIndex)
    longMetric("buildDataSize") += matchedKeys.capacity / 8

    // Process stream side with looking up hash relation
    val streamResultIter = streamIter.map { srow =>
      joinRowWithStream(srow)
      val keys = joinKeys(srow)
      if (keys.anyNull) {
        joinRowWithBuild(buildNullRow)
      } else {
        val matched = hashedRelation.getValueWithKeyIndex(keys)
        if (matched != null) {
          val keyIndex = matched.getKeyIndex
          val buildRow = matched.getValue
          val joinRow = joinRowWithBuild(buildRow)
          if (boundCondition(joinRow)) {
            matchedKeys.set(keyIndex)
            joinRow
          } else {
            joinRowWithBuild(buildNullRow)
          }
        } else {
          joinRowWithBuild(buildNullRow)
        }
      }
    }

    // Process build side with filtering out the matched rows
    val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap {
      valueRowWithKeyIndex =>
        val keyIndex = valueRowWithKeyIndex.getKeyIndex
        val isMatched = matchedKeys.get(keyIndex)
        if (!isMatched) {
          val buildRow = valueRowWithKeyIndex.getValue
          Some(streamNullJoinRowWithBuild(buildRow))
        } else {
          None
        }
    }

    streamResultIter ++ buildResultIter
  }

  /**
   * Full outer shuffled hash join with non-unique join keys:
   * 1. Process rows from stream side by looking up hash relation.
   *    Mark the matched rows from build side be looked up.
   *    A [[OpenHashSet]] (Long) is used to track matched rows with
   *    key index (Int) and value index (Int) together.
   * 2. Process rows from build side by iterating hash relation.
   *    Filter out rows from build side being matched already,
   *    by checking key index and value index from [[OpenHashSet]].
   *
   * The "value index" is defined as the index of the tuple in the chain
   * of tuples having the same key. For example, if certain key is found thrice,
   * the value indices of its tuples will be 0, 1 and 2.
   * Note that value indices of tuples with different keys are incomparable.
   */
  private def fullOuterJoinWithNonUniqueKey(
      streamIter: Iterator[InternalRow],
      hashedRelation: HashedRelation,
      joinKeys: UnsafeProjection,
      joinRowWithStream: InternalRow => JoinedRow,
      joinRowWithBuild: InternalRow => JoinedRow,
      streamNullJoinRowWithBuild: => InternalRow => JoinedRow,
      buildNullRow: GenericInternalRow): Iterator[InternalRow] = {
    val matchedRows = new OpenHashSet[Long]
    TaskContext.get().addTaskCompletionListener[Unit](_ => {
      // At the end of the task, update the task's memory usage for this
      // [[OpenHashSet]] to track matched rows, which has two parts:
      // [[OpenHashSet._bitset]] and [[OpenHashSet._data]].
      val bitSetEstimatedSize = matchedRows.getBitSet.capacity / 8
      val dataEstimatedSize = matchedRows.capacity * 8
      longMetric("buildDataSize") += bitSetEstimatedSize + dataEstimatedSize
    })

    def markRowMatched(keyIndex: Int, valueIndex: Int): Unit = {
      val rowIndex: Long = (keyIndex.toLong << 32) | valueIndex
      matchedRows.add(rowIndex)
    }

    def isRowMatched(keyIndex: Int, valueIndex: Int): Boolean = {
      val rowIndex: Long = (keyIndex.toLong << 32) | valueIndex
      matchedRows.contains(rowIndex)
    }

    // Process stream side with looking up hash relation
    val streamResultIter = streamIter.flatMap { srow =>
      val joinRow = joinRowWithStream(srow)
      val keys = joinKeys(srow)
      if (keys.anyNull) {
        Iterator.single(joinRowWithBuild(buildNullRow))
      } else {
        val buildIter = hashedRelation.getWithKeyIndex(keys)
        new RowIterator {
          private var found = false
          private var valueIndex = -1
          override def advanceNext(): Boolean = {
            while (buildIter != null && buildIter.hasNext) {
              val buildRowWithKeyIndex = buildIter.next()
              val keyIndex = buildRowWithKeyIndex.getKeyIndex
              val buildRow = buildRowWithKeyIndex.getValue
              valueIndex += 1
              if (boundCondition(joinRowWithBuild(buildRow))) {
                markRowMatched(keyIndex, valueIndex)
                found = true
                return true
              }
            }
            // When we reach here, it means no match is found for this key.
            // So we need to return one row with build side NULL row,
            // to satisfy the full outer join semantic.
            if (!found) {
              joinRowWithBuild(buildNullRow)
              // Set `found` to be true as we only need to return one row
              // but no more.
              found = true
              return true
            }
            false
          }
          override def getRow: InternalRow = joinRow
        }.toScala
      }
    }

    // Process build side with filtering out the matched rows
    var prevKeyIndex = -1
    var valueIndex = -1
    val buildResultIter = hashedRelation.valuesWithKeyIndex().flatMap {
      valueRowWithKeyIndex =>
        val keyIndex = valueRowWithKeyIndex.getKeyIndex
        if (prevKeyIndex == -1 || keyIndex != prevKeyIndex) {
          valueIndex = 0
          prevKeyIndex = keyIndex
        } else {
          valueIndex += 1
        }

        val isMatched = isRowMatched(keyIndex, valueIndex)
        if (!isMatched) {
          val buildRow = valueRowWithKeyIndex.getValue
          Some(streamNullJoinRowWithBuild(buildRow))
        } else {
          None
        }
    }

    streamResultIter ++ buildResultIter
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
    // Specialize `doProduce` code for full outer join, because full outer join needs to
    // iterate streamed and build side separately.
    if (joinType != FullOuter) {
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
    val consumeFullOuterJoinRow = ctx.freshName("consumeFullOuterJoinRow")
    ctx.addNewFunction(consumeFullOuterJoinRow,
      s"""
         |private void $consumeFullOuterJoinRow() {
         |  ${metricTerm(ctx, "numOutputRows")}.add(1);
         |  ${consume(ctx, resultVars)}
         |}
       """.stripMargin)

    val joinWithUniqueKey = codegenFullOuterJoinWithUniqueKey(
      ctx, (streamedRow, buildRow), (streamedInput, buildInput), streamedKeyEv, streamedKeyAnyNull,
      streamedKeyExprCode.value, relationTerm, conditionCheck, consumeFullOuterJoinRow)
    val joinWithNonUniqueKey = codegenFullOuterJoinWithNonUniqueKey(
      ctx, (streamedRow, buildRow), (streamedInput, buildInput), streamedKeyEv, streamedKeyAnyNull,
      streamedKeyExprCode.value, relationTerm, conditionCheck, consumeFullOuterJoinRow)

    s"""
       |if ($keyIsUnique) {
       |  $joinWithUniqueKey
       |} else {
       |  $joinWithNonUniqueKey
       |}
     """.stripMargin
  }

  /**
   * Generates the code for full outer join with unique join keys.
   * This is code-gen version of `fullOuterJoinWithUniqueKey()`.
   */
  private def codegenFullOuterJoinWithUniqueKey(
      ctx: CodegenContext,
      rows: (String, String),
      inputs: (String, String),
      streamedKeyEv: String,
      streamedKeyAnyNull: String,
      streamedKeyValue: ExprValue,
      relationTerm: String,
      conditionCheck: String,
      consumeFullOuterJoinRow: String): String = {
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
         |  $consumeFullOuterJoinRow();
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
         |    $consumeFullOuterJoinRow();
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
   * Generates the code for full outer join with non-unique join keys.
   * This is code-gen version of `fullOuterJoinWithNonUniqueKey()`.
   */
  private def codegenFullOuterJoinWithNonUniqueKey(
      ctx: CodegenContext,
      rows: (String, String),
      inputs: (String, String),
      streamedKeyEv: String,
      streamedKeyAnyNull: String,
      streamedKeyValue: ExprValue,
      relationTerm: String,
      conditionCheck: String,
      consumeFullOuterJoinRow: String): String = {
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
         |      $consumeFullOuterJoinRow();
         |    }
         |  }
         |
         |  if (!$foundMatch) {
         |    $buildRow = null;
         |    $consumeFullOuterJoinRow();
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
         |    $consumeFullOuterJoinRow();
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
