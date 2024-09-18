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

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{CodegenSupport, ExplainUtils, SparkPlan}
import org.apache.spark.sql.execution.metric.SQLMetrics
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.collection.{BitSet, CompactBuffer}

case class BroadcastNestedLoopJoinExec(
    left: SparkPlan,
    right: SparkPlan,
    buildSide: BuildSide,
    joinType: JoinType,
    condition: Option[Expression]) extends JoinCodegenSupport {

  override def leftKeys: Seq[Expression] = Nil
  override def rightKeys: Seq[Expression] = Nil

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  /** BuildRight means the right relation is the broadcast relation. */
  private val (streamed, broadcast) = buildSide match {
    case BuildRight => (left, right)
    case BuildLeft => (right, left)
  }

  override def simpleStringWithNodeId(): String = {
    val opId = ExplainUtils.getOpId(this)
    s"$nodeName $joinType $buildSide ($opId)".trim
  }

  override def requiredChildDistribution: Seq[Distribution] = buildSide match {
    case BuildLeft =>
      BroadcastDistribution(IdentityBroadcastMode) :: UnspecifiedDistribution :: Nil
    case BuildRight =>
      UnspecifiedDistribution :: BroadcastDistribution(IdentityBroadcastMode) :: Nil
  }

  override def outputPartitioning: Partitioning = (joinType, buildSide) match {
    case (_: InnerLike, _) | (LeftOuter, BuildRight) | (RightOuter, BuildLeft) |
         (LeftSingle, BuildRight) | (LeftSemi, BuildRight) | (LeftAnti, BuildRight) =>
      streamed.outputPartitioning
    case _ => super.outputPartitioning
  }

  override def outputOrdering: Seq[SortOrder] = (joinType, buildSide) match {
    case (_: InnerLike, _) | (LeftOuter, BuildRight) | (RightOuter, BuildLeft) |
         (LeftSingle, BuildRight) | (LeftSemi, BuildRight) | (LeftAnti, BuildRight) =>
      streamed.outputOrdering
    case _ => Nil
  }

  private[this] def genResultProjection: UnsafeProjection = joinType match {
    case LeftExistence(_) =>
      UnsafeProjection.create(output, output)
    case _ =>
      // Always put the stream side on left to simplify implementation
      // both of left and right side could be null
      UnsafeProjection.create(
        output, (streamed.output ++ broadcast.output).map(_.withNullability(true)))
  }

  override def output: Seq[Attribute] = {
    joinType match {
      case _: InnerLike =>
        left.output ++ right.output
      case LeftOuter | LeftSingle =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        left.output.map(_.withNullability(true)) ++ right.output.map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not take $x as the JoinType")
    }
  }

  @transient private lazy val boundCondition = {
    if (condition.isDefined) {
      Predicate.create(condition.get, streamed.output ++ broadcast.output).eval _
    } else {
      (r: InternalRow) => true
    }
  }

  /**
   * The implementation for InnerJoin.
   */
  private def innerJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow

      streamedIter.flatMap { streamedRow =>
        val joinedRows = buildRows.iterator.map(r => joinedRow(streamedRow, r))
        if (condition.isDefined) {
          joinedRows.filter(boundCondition)
        } else {
          joinedRows
        }
      }
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildRight
   *   RightOuter with BuildLeft
   */
  private def outerJoin(
      relation: Broadcast[Array[InternalRow]],
      check: Int => Int): RDD[InternalRow] = {
    streamed.execute().mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      // Returns an iterator to avoid copy the rows.
      new Iterator[InternalRow] {
        // current row from stream side
        private var streamRow: InternalRow = null
        // have found a match for current row or not
        private var foundMatch: Boolean = false
        // the matched result row
        private var resultRow: InternalRow = null
        // the next index of buildRows to try
        private var nextIndex: Int = 0
        // number of build rows matching the current probe row
        private var matches: Int = 0

        @scala.annotation.tailrec
        private def findNextMatch(): Boolean = {
          if (streamRow == null) {
            if (!streamedIter.hasNext) {
              return false
            }
            streamRow = streamedIter.next()
            nextIndex = 0
            foundMatch = false
            matches = 0
          }
          while (nextIndex < buildRows.length) {
            resultRow = joinedRow(streamRow, buildRows(nextIndex))
            nextIndex += 1
            if (boundCondition(resultRow)) {
              foundMatch = true
              matches = check(matches)
              return true
            }
          }
          if (!foundMatch) {
            resultRow = joinedRow(streamRow, nulls)
            streamRow = null
            true
          } else {
            resultRow = null
            streamRow = null
            findNextMatch()
          }
        }

        override def hasNext: Boolean = {
          resultRow != null || findNextMatch()
        }
        override def next(): InternalRow = {
          val r = resultRow
          resultRow = null
          r
        }
      }
    }
  }

  /**
   * The implementation for LeftSemi and LeftAnti joins.
   */
  private def leftExistenceJoin(
      relation: Broadcast[Array[InternalRow]],
      exists: Boolean): RDD[InternalRow] = {
    buildSide match {
      case BuildRight =>
        streamed.execute().mapPartitionsInternal { streamedIter =>
          val buildRows = relation.value
          val joinedRow = new JoinedRow

          if (condition.isDefined) {
            streamedIter.filter(l =>
              buildRows.exists(r => boundCondition(joinedRow(l, r))) == exists
            )
          } else if (buildRows.nonEmpty == exists) {
            streamedIter
          } else {
            Iterator.empty
          }
        }
      case BuildLeft if condition.isEmpty =>
        // If condition is empty, do not need to read rows from streamed side at all.
        // Only need to know whether streamed side is empty or not.
        val streamExists = !streamed.executeTake(1).isEmpty
        if (streamExists == exists) {
          sparkContext.makeRDD(relation.value.toImmutableArraySeq)
        } else {
          sparkContext.emptyRDD
        }
      case _ => // BuildLeft
        val matchedBroadcastRows = getMatchedBroadcastRowsBitSet(streamed.execute(), relation)
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val buildRows = relation.value
        while (i < buildRows.length) {
          if (matchedBroadcastRows.get(i) == exists) {
            buf += buildRows(i).copy()
          }
          i += 1
        }
        sparkContext.makeRDD(buf)
    }
  }

  /**
   * The implementation for ExistenceJoin
   */
  private def existenceJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    buildSide match {
      case BuildRight =>
        streamed.execute().mapPartitionsInternal { streamedIter =>
          val buildRows = relation.value
          val joinedRow = new JoinedRow

          if (condition.isDefined) {
            val resultRow = new GenericInternalRow(Array[Any](null))
            streamedIter.map { row =>
              val result = buildRows.exists(r => boundCondition(joinedRow(row, r)))
              resultRow.setBoolean(0, result)
              joinedRow(row, resultRow)
            }
          } else {
            val resultRow = new GenericInternalRow(Array[Any](buildRows.nonEmpty))
            streamedIter.map { row =>
              joinedRow(row, resultRow)
            }
          }
        }
      case _ => // BuildLeft
        val matchedBroadcastRows = getMatchedBroadcastRowsBitSet(streamed.execute(), relation)
        val buf: CompactBuffer[InternalRow] = new CompactBuffer()
        var i = 0
        val buildRows = relation.value
        while (i < buildRows.length) {
          val result = new GenericInternalRow(Array[Any](matchedBroadcastRows.get(i)))
          buf += new JoinedRow(buildRows(i).copy(), result)
          i += 1
        }
        sparkContext.makeRDD(buf)
    }
  }

  /**
   * The implementation for these joins:
   *
   *   LeftOuter with BuildLeft
   *   RightOuter with BuildRight
   *   FullOuter
   */
  private def defaultJoin(relation: Broadcast[Array[InternalRow]]): RDD[InternalRow] = {
    val streamRdd = streamed.execute()
    def notMatchedBroadcastRows: RDD[InternalRow] = {
      getMatchedBroadcastRowsBitSetRDD(streamRdd, relation)
        .repartition(1)
        .mapPartitions(iter => Seq(iter.fold(new BitSet(relation.value.length))(_ | _)).iterator)
        .flatMap { matchedBroadcastRows =>
          val nulls = new GenericInternalRow(streamed.output.size)
          val buf: CompactBuffer[InternalRow] = new CompactBuffer()
          val joinedRow = new JoinedRow
          joinedRow.withLeft(nulls)
          var i = 0
          val buildRows = relation.value
          while (i < buildRows.length) {
            if (!matchedBroadcastRows.get(i)) {
              buf += joinedRow.withRight(buildRows(i)).copy()
            }
            i += 1
          }
          buf.iterator
        }
    }

    val matchedStreamRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val joinedRow = new JoinedRow
      val nulls = new GenericInternalRow(broadcast.output.size)

      streamedIter.flatMap { streamedRow =>
        var i = 0
        var foundMatch = false
        val matchedRows = new CompactBuffer[InternalRow]

        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matchedRows += joinedRow.copy()
            foundMatch = true
          }
          i += 1
        }

        if (!foundMatch && joinType == FullOuter) {
          matchedRows += joinedRow(streamedRow, nulls).copy()
        }
        matchedRows.iterator
      }
    }

    sparkContext.union(
      matchedStreamRows,
      notMatchedBroadcastRows
    )
  }

  /**
   * Get matched rows from broadcast side as a [[BitSet]].
   * Create a local [[BitSet]] for broadcast side on each RDD partition,
   * and merge all [[BitSet]]s together.
   */
  private def getMatchedBroadcastRowsBitSet(
      streamRdd: RDD[InternalRow],
      relation: Broadcast[Array[InternalRow]]): BitSet = {
    getMatchedBroadcastRowsBitSetRDD(streamRdd, relation)
      .fold(new BitSet(relation.value.length))(_ | _)
  }

  private def getMatchedBroadcastRowsBitSetRDD(
      streamRdd: RDD[InternalRow],
      relation: Broadcast[Array[InternalRow]]): RDD[BitSet] = {
    val matchedBuildRows = streamRdd.mapPartitionsInternal { streamedIter =>
      val buildRows = relation.value
      val matched = new BitSet(buildRows.length)
      val joinedRow = new JoinedRow

      streamedIter.foreach { streamedRow =>
        var i = 0
        while (i < buildRows.length) {
          if (boundCondition(joinedRow(streamedRow, buildRows(i)))) {
            matched.set(i)
          }
          i += 1
        }
      }
      Seq(matched).iterator
    }

    matchedBuildRows
  }

  protected override def doExecute(): RDD[InternalRow] = {
    val broadcastedRelation = broadcast.executeBroadcast[Array[InternalRow]]()

    val resultRdd = (joinType, buildSide) match {
      case (_: InnerLike, _) =>
        innerJoin(broadcastedRelation)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) =>
        outerJoin(broadcastedRelation, _ => 0)
      case (LeftSingle, BuildRight) =>
        val check: Int => Int = matches => {
          if (matches > 0) {
            throw QueryExecutionErrors.scalarSubqueryReturnsMultipleRows();
          }
          matches + 1
        }
        outerJoin(broadcastedRelation, check)
      case (LeftSemi, _) =>
        leftExistenceJoin(broadcastedRelation, exists = true)
      case (LeftAnti, _) =>
        leftExistenceJoin(broadcastedRelation, exists = false)
      case (_: ExistenceJoin, _) =>
        existenceJoin(broadcastedRelation)
      case (LeftSingle, BuildLeft) =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin should not use the left side as build when " +
            s"executing a LeftSingle join")
      case _ =>
        /**
         * LeftOuter with BuildLeft
         * RightOuter with BuildRight
         * FullOuter
         */
        defaultJoin(broadcastedRelation)
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

  override def supportCodegen: Boolean = (joinType, buildSide) match {
    case (_: InnerLike, _) | (LeftOuter, BuildRight) | (RightOuter, BuildLeft) |
         (LeftSemi | LeftAnti, BuildRight) | (LeftSingle, BuildRight) => true
    case _ => false
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    streamed.asInstanceOf[CodegenSupport].inputRDDs()
  }

  override def needCopyResult: Boolean = true

  override def doProduce(ctx: CodegenContext): String = {
    streamed.asInstanceOf[CodegenSupport].produce(ctx, this)
  }

  override def doConsume(ctx: CodegenContext, input: Seq[ExprCode], row: ExprCode): String = {
    (joinType, buildSide) match {
      case (_: InnerLike, _) => codegenInner(ctx, input)
      case (LeftOuter, BuildRight) | (RightOuter, BuildLeft) => codegenOuter(ctx, input)
      case (LeftSingle, BuildRight) => codegenOuter(ctx, input)
      case (LeftSemi, BuildRight) => codegenLeftExistence(ctx, input, exists = true)
      case (LeftAnti, BuildRight) => codegenLeftExistence(ctx, input, exists = false)
      case _ =>
        throw new IllegalArgumentException(
          s"BroadcastNestedLoopJoin code-gen should not take neither $joinType as the JoinType " +
          s"nor $buildSide as the BuildSide")
    }
  }

  /**
   * Returns a tuple of [[Broadcast]] side and the variable name for it.
   */
  private def prepareBroadcast(ctx: CodegenContext): (Array[InternalRow], String) = {
    // Create a name for broadcast side
    val broadcastArray = broadcast.executeBroadcast[Array[InternalRow]]()
    val broadcastTerm = ctx.addReferenceObj("broadcastTerm", broadcastArray)

    // Inline mutable state since not many join operations in a task
    val arrayTerm = ctx.addMutableState("InternalRow[]", "buildRowArray",
      v => s"$v = (InternalRow[]) $broadcastTerm.value();", forceInline = true)
    (broadcastArray.value, arrayTerm)
  }

  private def codegenInner(ctx: CodegenContext, input: Seq[ExprCode]): String = {
    val (_, buildRowArrayTerm) = prepareBroadcast(ctx)
    val (buildRow, checkCondition, buildVars) = getJoinCondition(ctx, input, streamed, broadcast)

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    val arrayIndex = ctx.freshName("arrayIndex")
    val numOutput = metricTerm(ctx, "numOutputRows")

    s"""
       |for (int $arrayIndex = 0; $arrayIndex < $buildRowArrayTerm.length; $arrayIndex++) {
       |  UnsafeRow $buildRow = (UnsafeRow) $buildRowArrayTerm[$arrayIndex];
       |  $checkCondition {
       |    $numOutput.add(1);
       |    ${consume(ctx, resultVars)}
       |  }
       |}
     """.stripMargin
  }

  private def codegenOuter(
      ctx: CodegenContext,
      input: Seq[ExprCode]): String = {
    val (buildRowArray, buildRowArrayTerm) = prepareBroadcast(ctx)
    val (buildRow, checkCondition, _) = getJoinCondition(ctx, input, streamed, broadcast)
    val buildVars = genOneSideJoinVars(ctx, buildRow, broadcast, setDefaultValue = true)

    val resultVars = buildSide match {
      case BuildLeft => buildVars ++ input
      case BuildRight => input ++ buildVars
    }
    val arrayIndex = ctx.freshName("arrayIndex")
    val shouldOutputRow = ctx.freshName("shouldOutputRow")
    val foundMatch = ctx.freshName("foundMatch")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val matchesFound = ctx.freshName("matchesFound")

    if (buildRowArray.isEmpty) {
      s"""
         |UnsafeRow $buildRow = null;
         |$numOutput.add(1);
         |${consume(ctx, resultVars)}
       """.stripMargin
    } else {
      val initSingleCounter = if (joinType == LeftSingle) {
        s"""
           |int $matchesFound = 0;""".stripMargin
      } else {
        ""
      }
      val evaluateSingleCheck = if (joinType == LeftSingle) {
        s"""
           |$matchesFound += 1;
           |if ($matchesFound > 1) {
           |  throw QueryExecutionErrors.scalarSubqueryReturnsMultipleRows();
           |}
           |""".stripMargin
      } else {
        ""
      }
      s"""
         |boolean $foundMatch = false;
         |${initSingleCounter}
         |for (int $arrayIndex = 0; $arrayIndex < $buildRowArrayTerm.length; $arrayIndex++) {
         |  UnsafeRow $buildRow = (UnsafeRow) $buildRowArrayTerm[$arrayIndex];
         |  boolean $shouldOutputRow = false;
         |  $checkCondition {
         |    $shouldOutputRow = true;
         |    $foundMatch = true;
         |    $evaluateSingleCheck
         |  }
         |  if ($arrayIndex == $buildRowArrayTerm.length - 1 && !$foundMatch) {
         |    $buildRow = null;
         |    $shouldOutputRow = true;
         |  }
         |  if ($shouldOutputRow) {
         |    $numOutput.add(1);
         |    ${consume(ctx, resultVars)}
         |  }
         |}
       """.stripMargin
    }
  }

  private def codegenLeftExistence(
      ctx: CodegenContext,
      input: Seq[ExprCode],
      exists: Boolean): String = {
    val (buildRowArray, buildRowArrayTerm) = prepareBroadcast(ctx)
    val numOutput = metricTerm(ctx, "numOutputRows")

    if (condition.isEmpty) {
      if (buildRowArray.nonEmpty == exists) {
        // Return streamed side if join condition is empty and
        // 1. build side is non-empty for LeftSemi join
        // or
        // 2. build side is empty for LeftAnti join.
        s"""
           |$numOutput.add(1);
           |${consume(ctx, input)}
         """.stripMargin
      } else {
        // Return nothing if join condition is empty and
        // 1. build side is empty for LeftSemi join
        // or
        // 2. build side is non-empty for LeftAnti join.
        ""
      }
    } else {
      val (buildRow, checkCondition, _) = getJoinCondition(ctx, input, streamed, broadcast)
      val foundMatch = ctx.freshName("foundMatch")
      val arrayIndex = ctx.freshName("arrayIndex")

      s"""
         |boolean $foundMatch = false;
         |for (int $arrayIndex = 0; $arrayIndex < $buildRowArrayTerm.length; $arrayIndex++) {
         |  UnsafeRow $buildRow = (UnsafeRow) $buildRowArrayTerm[$arrayIndex];
         |  $checkCondition {
         |    $foundMatch = true;
         |    break;
         |  }
         |}
         |if ($foundMatch == $exists) {
         |  $numOutput.add(1);
         |  ${consume(ctx, input)}
         |}
     """.stripMargin
    }
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): BroadcastNestedLoopJoinExec =
    copy(left = newLeft, right = newRight)
}
