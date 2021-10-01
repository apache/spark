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

import scala.collection.mutable

import org.apache.spark.TaskContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical.{BroadcastDistribution, Distribution, HashPartitioning, Partitioning, PartitioningCollection, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{CodegenSupport, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics

/**
 * Performs an inner hash join of two child relations.  When the output RDD of this operator is
 * being constructed, a Spark job is asynchronously started to calculate the values for the
 * broadcast relation.  This data is then placed in a Spark broadcast variable.  The streamed
 * relation is not shuffled.
 */
case class BroadcastHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan,
    isNullAwareAntiJoin: Boolean = false)
  extends HashJoin {

  if (isNullAwareAntiJoin) {
    require(leftKeys.length == 1, "leftKeys length should be 1")
    require(rightKeys.length == 1, "rightKeys length should be 1")
    require(joinType == LeftAnti, "joinType must be LeftAnti.")
    require(buildSide == BuildRight, "buildSide must be BuildRight.")
    require(condition.isEmpty, "null aware anti join optimize condition should be empty.")
  }

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(
      buildBoundKeys, isNullAwareAntiJoin, ignoreDuplicatedKey)
    buildSide match {
      case BuildLeft =>
        BroadcastDistribution(mode) :: UnspecifiedDistribution :: Nil
      case BuildRight =>
        UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
    }
  }

  override lazy val outputPartitioning: Partitioning = {
    joinType match {
      case _: InnerLike if conf.broadcastHashJoinOutputPartitioningExpandLimit > 0 =>
        streamedPlan.outputPartitioning match {
          case h: HashPartitioning => expandOutputPartitioning(h)
          case c: PartitioningCollection => expandOutputPartitioning(c)
          case other => other
        }
      case _ => streamedPlan.outputPartitioning
    }
  }

  // An one-to-many mapping from a streamed key to build keys.
  private lazy val streamedKeyToBuildKeyMapping = {
    val mapping = mutable.Map.empty[Expression, Seq[Expression]]
    streamedKeys.zip(buildKeys).foreach {
      case (streamedKey, buildKey) =>
        val key = streamedKey.canonicalized
        mapping.get(key) match {
          case Some(v) => mapping.put(key, v :+ buildKey)
          case None => mapping.put(key, Seq(buildKey))
        }
    }
    mapping.toMap
  }

  // Expands the given partitioning collection recursively.
  private def expandOutputPartitioning(
      partitioning: PartitioningCollection): PartitioningCollection = {
    PartitioningCollection(partitioning.partitionings.flatMap {
      case h: HashPartitioning => expandOutputPartitioning(h).partitionings
      case c: PartitioningCollection => Seq(expandOutputPartitioning(c))
      case other => Seq(other)
    })
  }

  // Expands the given hash partitioning by substituting streamed keys with build keys.
  // For example, if the expressions for the given partitioning are Seq("a", "b", "c")
  // where the streamed keys are Seq("b", "c") and the build keys are Seq("x", "y"),
  // the expanded partitioning will have the following expressions:
  // Seq("a", "b", "c"), Seq("a", "b", "y"), Seq("a", "x", "c"), Seq("a", "x", "y").
  // The expanded expressions are returned as PartitioningCollection.
  private def expandOutputPartitioning(partitioning: HashPartitioning): PartitioningCollection = {
    val maxNumCombinations = conf.broadcastHashJoinOutputPartitioningExpandLimit
    var currentNumCombinations = 0

    def generateExprCombinations(
        current: Seq[Expression],
        accumulated: Seq[Expression]): Seq[Seq[Expression]] = {
      if (currentNumCombinations >= maxNumCombinations) {
        Nil
      } else if (current.isEmpty) {
        currentNumCombinations += 1
        Seq(accumulated)
      } else {
        val buildKeysOpt = streamedKeyToBuildKeyMapping.get(current.head.canonicalized)
        generateExprCombinations(current.tail, accumulated :+ current.head) ++
          buildKeysOpt.map(_.flatMap(b => generateExprCombinations(current.tail, accumulated :+ b)))
            .getOrElse(Nil)
      }
    }

    PartitioningCollection(
      generateExprCombinations(partitioning.expressions, Nil)
        .map(HashPartitioning(_, partitioning.numPartitions)))
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    if (isNullAwareAntiJoin) {
      streamedPlan.execute().mapPartitionsInternal { streamedIter =>
        val hashed = broadcastRelation.value.asReadOnlyCopy()
        TaskContext.get().taskMetrics().incPeakExecutionMemory(hashed.estimatedSize)
        if (hashed == EmptyHashedRelation) {
          streamedIter
        } else if (hashed == HashedRelationWithAllNullKeys) {
          Iterator.empty
        } else {
          val keyGenerator = UnsafeProjection.create(
            BindReferences.bindReferences[Expression](
              leftKeys,
              AttributeSeq(left.output))
          )
          streamedIter.filter(row => {
            val lookupKey: UnsafeRow = keyGenerator(row)
            if (lookupKey.anyNull()) {
              false
            } else {
              // Anti Join: Drop the row on the streamed side if it is a match on the build
              hashed.get(lookupKey) == null
            }
          })
        }
      }
    } else {
      streamedPlan.execute().mapPartitions { streamedIter =>
        val hashed = broadcastRelation.value.asReadOnlyCopy()
        TaskContext.get().taskMetrics().incPeakExecutionMemory(hashed.estimatedSize)
        join(streamedIter, hashed, numOutputRows)
      }
    }
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamedPlan.asInstanceOf[CodegenSupport].inputRDDs()
  }

  private def multipleOutputForOneInput: Boolean = joinType match {
    case _: InnerLike | LeftOuter | RightOuter =>
      // For inner and outer joins, one row from the streamed side may produce multiple result rows,
      // if the build side has duplicated keys. Note that here we wait for the broadcast to be
      // finished, which is a no-op because it's already finished when we wait it in `doProduce`.
      !buildPlan.executeBroadcast[HashedRelation]().value.keyIsUnique

    // Other joins types(semi, anti, existence) can at most produce one result row for one input
    // row from the streamed side.
    case _ => false
  }

  // If the streaming side needs to copy result, this join plan needs to copy too. Otherwise,
  // this join plan only needs to copy result if it may output multiple rows for one input.
  override def needCopyResult: Boolean =
    streamedPlan.asInstanceOf[CodegenSupport].needCopyResult || multipleOutputForOneInput

  /**
   * Returns a tuple of Broadcast of HashedRelation and the variable name for it.
   */
  private def prepareBroadcast(ctx: CodegenContext): (Broadcast[HashedRelation], String) = {
    // create a name for HashedRelation
    val broadcastRelation = buildPlan.executeBroadcast[HashedRelation]()
    val broadcast = ctx.addReferenceObj("broadcast", broadcastRelation)
    val clsName = broadcastRelation.value.getClass.getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"""
         | $v = (($clsName) $broadcast.value()).asReadOnlyCopy();
         | incPeakExecutionMemory($v.estimatedSize());
       """.stripMargin, forceInline = true)
    (broadcastRelation, relationTerm)
  }

  protected override def prepareRelation(ctx: CodegenContext): HashedRelationInfo = {
    val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
    HashedRelationInfo(relationTerm,
      broadcastRelation.value.keyIsUnique,
      broadcastRelation.value == EmptyHashedRelation)
  }

  /**
   * Generates the code for anti join.
   * Handles NULL-aware anti join (NAAJ) separately here.
   */
  protected override def codegenAnti(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    if (isNullAwareAntiJoin) {
      val (broadcastRelation, relationTerm) = prepareBroadcast(ctx)
      val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
      val numOutput = metricTerm(ctx, "numOutputRows")

      if (broadcastRelation.value == EmptyHashedRelation) {
        s"""
           |// If the right side is empty, NAAJ simply returns the left side.
           |$numOutput.add(1);
           |${consume(ctx, input)}
         """.stripMargin
      } else if (broadcastRelation.value == HashedRelationWithAllNullKeys) {
        s"""
           |// If the right side contains any all-null key, NAAJ simply returns Nothing.
         """.stripMargin
      } else {
        s"""
           |// generate join key for stream side
           |${keyEv.code}
           |if (!$anyNull && $relationTerm.getValue(${keyEv.value}) == null) {
           |  $numOutput.add(1);
           |  ${consume(ctx, input)}
           |}
         """.stripMargin
      }
    } else {
      super.codegenAnti(ctx, input)
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): BroadcastHashJoinExec =
    copy(left = newLeft, right = newRight)
}
