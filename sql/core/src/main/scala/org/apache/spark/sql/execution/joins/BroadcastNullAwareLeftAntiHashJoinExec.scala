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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.optimizer.{BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.LongType

case class BroadcastNullAwareLeftAntiHashJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends BaseJoinExec with CodegenSupport {

  // TODO support multi column NULL-aware anti join in future.
  // See. http://www.vldb.org/pvldb/vol2/vldb09-423.pdf Section 6
  // multi-column null aware anti join is much more complicated than single column ones.
  require(leftKeys.length == 1, "leftKeys length should be 1")
  require(rightKeys.length == 1, "rightKeys length should be 1")
  require(joinType == LeftAnti, "joinType must be LeftAnti.")
  require(buildSide == BuildRight, "buildSide must be BuildRight.")
  require(SQLConf.get.nullAwareAntiJoinOptimizeEnabled,
    "nullAwareAntiJoinOptimizeEnabled must be on for null aware anti join optimize.")

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  private val (streamed, broadcast) = (left, right)

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType ${buildSide} ($opId)".trim
  }

  override def requiredChildDistribution: Seq[Distribution] = {
    val mode = HashedRelationBroadcastMode(
      BindReferences.bindReferences(HashJoin.rewriteKeyExpr(rightKeys), right.output)
    )
    UnspecifiedDistribution :: BroadcastDistribution(mode) :: Nil
  }

  private[this] def genResultProjection: UnsafeProjection = {
    UnsafeProjection.create(output, output)
  }

  override def output: Seq[Attribute] = {
    left.output
  }

  private def nullAwareLeftAntiJoin(
      relation: HashedRelation): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      if (relation.inputEmpty) {
        streamedIter
      } else if (relation.anyNullKeyExists) {
        Iterator.empty
      } else {
        val keyGenerator = UnsafeProjection.create(
          BindReferences.bindReferences[Expression](
            leftKeys,
            AttributeSeq(left.output))
        )
        streamedIter.filter(row => {
          val lookupKey: UnsafeRow = keyGenerator(row)
          val streamedRowIsNull = lookupKey.isNullAt(0)
          val notInKeyEqual = relation.get(lookupKey) != null
          !streamedRowIsNull && !notInKeyEqual
        })
      }
    }
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val relation: HashedRelation = broadcast.executeBroadcast[HashedRelation]().value
    val resultRdd = (joinType, buildSide) match {
      case (LeftAnti, BuildRight) =>
        nullAwareLeftAntiJoin(relation)
      case _ =>
        throw new IllegalArgumentException(
          s"BroadcastNullAwareLeftAntiHashJoinExec only supports (LeftAnti + BuildRight) for now.")
    }

    val numOutputRows = longMetric("numOutputRows")
    resultRdd.mapPartitionsWithIndexInternal { (index, iter) =>
      val resultProj = genResultProjection
      resultProj.initialize(index)
      iter.map { r =>
        numOutputRows += 1
        resultProj(r)
      }
    }
  }

  override def needCopyResult: Boolean =
    streamed.asInstanceOf[CodegenSupport].needCopyResult

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamed.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def doProduce(ctx: CodegenContext): String = {
    streamed.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  private def genStreamSideJoinKey(
      ctx: CodegenContext,
      input: Seq[ExprCode]): (ExprCode, String) = {
    ctx.currentVars = input
    if (leftKeys.length == 1 && leftKeys.head.dataType == LongType) {
      // generate the join key as Long
      val ev =
        BindReferences.bindReference[Expression](leftKeys.head, left.output).genCode(ctx)
      (ev, ev.isNull)
    } else {
      // generate the join key as UnsafeRow
      val ev = GenerateUnsafeProjection.createCode(ctx,
        BindReferences.bindReferences[Expression](leftKeys, left.output))
      (ev, s"${ev.value}.anyNull()")
    }
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    val relation: HashedRelation = broadcast.executeBroadcast[HashedRelation]().value
    val broadcastRef = ctx.addReferenceObj("broadcast", relation)
    val clsName = relation.getClass.getName

    // Inline mutable state since not many join operations in a task
    val relationTerm = ctx.addMutableState(clsName, "relation",
      v => s"""
              | $v = (($clsName) $broadcastRef).asReadOnlyCopy();
              | incPeakExecutionMemory($v.estimatedSize());
            """.stripMargin, forceInline = true)

    val (keyEv, anyNull) = genStreamSideJoinKey(ctx, input)
    val matched = ctx.freshName("matched")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val found = ctx.freshName("found")

    if (relation.inputEmpty) {
      s"""
         |// singleColumn NAAJ inputEmpty(true) accept all
         |$numOutput.add(1);
         |${consume(ctx, input)}
       """.stripMargin
    } else if (relation.anyNullKeyExists) {
      s"""
         |// singleColumn NAAJ inputEmpty(false) anyNullKeyExists(true) reject all
       """.stripMargin
    } else {
      s"""
         |// singleColumn NAAJ inputEmpty(false) anyNullKeyExists(false)
         |boolean $found = false;
         |// generate join key for stream side
         |${keyEv.code}
         |// Check if the key has nulls.
         |if (!($anyNull)) {
         |  // Check if the HashedRelation exists.
         |  UnsafeRow $matched = (UnsafeRow)$relationTerm.getValue(${keyEv.value});
         |  if ($matched != null) {
         |    $found = true;
         |  }
         |} else {
         |  $found = true;
         |}
         |
         |if (!$found) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
       """.stripMargin
    }
  }
}
