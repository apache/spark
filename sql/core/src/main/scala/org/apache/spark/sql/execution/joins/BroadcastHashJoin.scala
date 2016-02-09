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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode, GenerateUnsafeProjection}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnspecifiedDistribution}
import org.apache.spark.sql.execution.{BinaryNode, CodegenSupport, SQLExecution, SparkPlan}
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
    joinType: JoinType,
    buildSide: BuildSide,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan)
  extends BinaryNode with HashJoin with CodegenSupport {

  override private[sql] lazy val metrics = Map(
    "numLeftRows" -> SQLMetrics.createLongMetric(sparkContext, "number of left rows"),
    "numRightRows" -> SQLMetrics.createLongMetric(sparkContext, "number of right rows"),
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
    val numBuildRows = buildSide match {
      case BuildLeft => longMetric("numLeftRows")
      case BuildRight => longMetric("numRightRows")
    }

    // broadcastFuture is used in "doExecute". Therefore we can get the execution id correctly here.
    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    Future {
      // This will run in another thread. Set the execution id so that we can connect these jobs
      // with the correct execution.
      SQLExecution.withExecutionId(sparkContext, executionId) {
        // Note that we use .execute().collect() because we don't want to convert data to Scala
        // types
        val input: Array[InternalRow] = buildPlan.execute().map { row =>
          numBuildRows += 1
          row.copy()
        }.collect()
        // The following line doesn't run in a job so we cannot track the metric value. However, we
        // have already tracked it in the above lines. So here we can use
        // `SQLMetrics.nullLongMetric` to ignore it.
        // TODO: move this check into HashedRelation
        val hashed = if (canJoinKeyFitWithinLong) {
          LongHashedRelation(
            input.iterator, SQLMetrics.nullLongMetric, buildSideKeyGenerator, input.size)
        } else {
          HashedRelation(
            input.iterator, SQLMetrics.nullLongMetric, buildSideKeyGenerator, input.size)
        }
        sparkContext.broadcast(hashed)
      }
    }(BroadcastHashJoin.broadcastHashJoinExecutionContext)
  }

  protected override def doPrepare(): Unit = {
    broadcastFuture
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val numStreamedRows = buildSide match {
      case BuildLeft => longMetric("numRightRows")
      case BuildRight => longMetric("numLeftRows")
    }
    val numOutputRows = longMetric("numOutputRows")

    val broadcastRelation = Await.result(broadcastFuture, timeout)

    streamedPlan.execute().mapPartitions { streamedIter =>
      val joinedRow = new JoinedRow()
      val hashTable = broadcastRelation.value
      TaskContext.get().taskMetrics().incPeakExecutionMemory(hashTable.getMemorySize)
      val keyGenerator = streamSideKeyGenerator
      val resultProj = createResultProjection

      joinType match {
        case Inner =>
          hashJoin(streamedIter, numStreamedRows, hashTable, numOutputRows)

        case LeftOuter =>
          streamedIter.flatMap(currentRow => {
            numStreamedRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withLeft(currentRow)
            leftOuterIterator(rowKey, joinedRow, hashTable.get(rowKey), resultProj, numOutputRows)
          })

        case RightOuter =>
          streamedIter.flatMap(currentRow => {
            numStreamedRows += 1
            val rowKey = keyGenerator(currentRow)
            joinedRow.withRight(currentRow)
            rightOuterIterator(rowKey, hashTable.get(rowKey), joinedRow, resultProj, numOutputRows)
          })

        case x =>
          throw new IllegalArgumentException(
            s"BroadcastHashJoin should not take $x as the JoinType")
      }
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
    val valid = ctx.freshName("invalid")

    // create variables for output
    ctx.currentVars = null
    ctx.INPUT_ROW = matched
    val buildColumns = buildPlan.output.zipWithIndex.map { case (a, i) =>
      val ev = BoundReference(i, a.dataType, a.nullable).gen(ctx)
      if (joinType == Inner) {
        ev
      } else {
        val isNull = ctx.freshName("isNull")
        val value = ctx.freshName("value")
        val code = s"""
          |boolean $isNull = true;
          |${ctx.javaType(a.dataType)} $value = ${ctx.defaultValue(a.dataType)};
          |if ($matched != null) {
          |  ${ev.code}
          |  $isNull = ${ev.isNull};
          |  $value = ${ev.value};
          |}
         """.stripMargin
        ExprCode(code, isNull, value)
      }
    }

    // output variables
    val resultVars = buildSide match {
      case BuildLeft => buildColumns ++ input
      case BuildRight => input ++ buildColumns
    }

    if (joinType == Inner) {
      val outputCode = if (condition.isDefined) {
        // filter the output via condition
        ctx.currentVars = resultVars
        val ev = BindReferences.bindReference(condition.get, this.output).gen(ctx)
        s"""
         |${ev.code}
         |if (!${ev.isNull} && ${ev.value}) {
         |  ${consume(ctx, resultVars)}
         |}
         """.stripMargin
      } else {
        consume(ctx, resultVars)
      }

      if (broadcastRelation.value.isInstanceOf[UniqueHashedRelation]) {
        s"""
         |// generate join key
         |${keyVal.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyVal.value});
         |if ($matched != null) {
         |  ${buildColumns.map(_.code).mkString("\n")}
         |  $outputCode
         |}
         """.stripMargin

      } else {
        val matches = ctx.freshName("matches")
        val bufferType = classOf[CompactBuffer[UnsafeRow]].getName
        val i = ctx.freshName("i")
        val size = ctx.freshName("size")
        s"""
         |// generate join key
         |${keyVal.code}
         |// find matches from HashRelation
         |$bufferType $matches = $anyNull ? null : ($bufferType) $relationTerm.get(${keyVal.value});
         |if ($matches != null) {
         |  int $size = $matches.size();
         |  for (int $i = 0; $i < $size; $i++) {
         |    UnsafeRow $matched = (UnsafeRow) $matches.apply($i);
         |    ${buildColumns.map(_.code).mkString("\n")}
         |    $outputCode
         |  }
         |}
         """.stripMargin
      }

    } else {
      // LeftOuter and RightOuter

      // filter the output via condition
      val checkCondition = if (condition.isDefined) {
        ctx.currentVars = resultVars
        val ev = BindReferences.bindReference(condition.get, this.output).gen(ctx)
        s"""
         |boolean $valid = true;
         |if ($matched != null) {
         |  ${ev.code}
         |  $valid = !${ev.isNull} && ${ev.value};
         |}
         """.stripMargin
      } else {
        s"final boolean $valid = true;"
      }

      if (broadcastRelation.value.isInstanceOf[UniqueHashedRelation]) {
        s"""
         |// generate join key
         |${keyVal.code}
         |// find matches from HashedRelation
         |UnsafeRow $matched = $anyNull ? null: (UnsafeRow)$relationTerm.getValue(${keyVal.value});
         |${buildColumns.map(_.code).mkString("\n")}
         |${checkCondition.trim}
         |if (!$valid) {
         |  // reset to null
         |  ${buildColumns.map(v => s"${v.isNull} = true;").mkString("\n")}
         |}
         |${consume(ctx, resultVars)}
         """.stripMargin

      } else {
        val matches = ctx.freshName("matches")
        val bufferType = classOf[CompactBuffer[UnsafeRow]].getName
        val i = ctx.freshName("i")
        val size = ctx.freshName("size")
        val found = ctx.freshName("found")
        s"""
         |// generate join key
         |${keyVal.code}
         |// find matches from HashRelation
         |$bufferType $matches = $anyNull ? null : ($bufferType) $relationTerm.get(${keyVal.value});
         |int $size = $matches != null ? $matches.size() : 0;
         |boolean $found = false;
         |for (int $i = 0; $i <= $size; $i++) {
         |  UnsafeRow $matched = $i < $size ? (UnsafeRow) $matches.apply($i) : null;
         |  ${buildColumns.map(_.code).mkString("\n")}
         |  ${checkCondition.trim}
         |  if ($valid && ($i < $size || !$found)) {
         |    $found = true;
         |    ${consume(ctx, resultVars)}
         |  }
         |}
         """.stripMargin
      }
    }
  }
}

object BroadcastHashJoin {

  private[joins] val broadcastHashJoinExecutionContext = ExecutionContext.fromExecutorService(
    ThreadUtils.newDaemonCachedThreadPool("broadcast-hash-join", 128))
}
