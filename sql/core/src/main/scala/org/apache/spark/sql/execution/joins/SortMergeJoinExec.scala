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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.{BinaryExecNode, CodegenSupport, RowIterator, SparkPlan}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.util.collection.BitSet

/**
 * Performs a sort merge join of two child relations.
 */
case class SortMergeJoinExec(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    joinType: JoinType,
    condition: Option[Expression],
    left: SparkPlan,
    right: SparkPlan) extends BinaryExecNode with CodegenSupport {

  override lazy val metrics = Map(
    "numOutputRows" -> SQLMetrics.createMetric(sparkContext, "number of output rows"))

  override def output: Seq[Attribute] = {
    joinType match {
      case Inner =>
        left.output ++ right.output
      case LeftOuter =>
        left.output ++ right.output.map(_.withNullability(true))
      case RightOuter =>
        left.output.map(_.withNullability(true)) ++ right.output
      case FullOuter =>
        (left.output ++ right.output).map(_.withNullability(true))
      case j: ExistenceJoin =>
        left.output :+ j.exists
      case LeftExistence(_) =>
        left.output
      case x =>
        throw new IllegalArgumentException(
          s"${getClass.getSimpleName} should not take $x as the JoinType")
    }
  }

  override def outputPartitioning: Partitioning = joinType match {
    case Inner => PartitioningCollection(Seq(left.outputPartitioning, right.outputPartitioning))
    // For left and right outer joins, the output is partitioned by the streamed input's join keys.
    case LeftOuter => left.outputPartitioning
    case RightOuter => right.outputPartitioning
    case FullOuter => UnknownPartitioning(left.outputPartitioning.numPartitions)
    case LeftExistence(_) => left.outputPartitioning
    case x =>
      throw new IllegalArgumentException(
        s"${getClass.getSimpleName} should not take $x as the JoinType")
  }

  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(leftKeys) :: ClusteredDistribution(rightKeys) :: Nil

  override def outputOrdering: Seq[SortOrder] = requiredOrders(leftKeys)

  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    requiredOrders(leftKeys) :: requiredOrders(rightKeys) :: Nil

  private def requiredOrders(keys: Seq[Expression]): Seq[SortOrder] = {
    // This must be ascending in order to agree with the `keyOrdering` defined in `doExecute()`.
    keys.map(SortOrder(_, Ascending))
  }

  private def createLeftKeyGenerator(): Projection =
    UnsafeProjection.create(leftKeys, left.output)

  private def createRightKeyGenerator(): Projection =
    UnsafeProjection.create(rightKeys, right.output)

  protected override def doExecute(): RDD[InternalRow] = {
    val numOutputRows = longMetric("numOutputRows")

    left.execute().zipPartitions(right.execute()) { (leftIter, rightIter) =>
      val boundCondition: (InternalRow) => Boolean = {
        condition.map { cond =>
          newPredicate(cond, left.output ++ right.output)
        }.getOrElse {
          (r: InternalRow) => true
        }
      }

      // An ordering that can be used to compare keys from both sides.
      val keyOrdering = newNaturalAscendingOrdering(leftKeys.map(_.dataType))
      val resultProj: InternalRow => InternalRow = UnsafeProjection.create(output, output)

      joinType match {
        case Inner =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] var currentRightMatches: ArrayBuffer[InternalRow] = _
            private[this] var currentMatchIdx: Int = -1
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter)
            )
            private[this] val joinRow = new JoinedRow

            if (smjScanner.findNextInnerJoinRows()) {
              currentRightMatches = smjScanner.getBufferedMatches
              currentLeftRow = smjScanner.getStreamedRow
              currentMatchIdx = 0
            }

            override def advanceNext(): Boolean = {
              while (currentMatchIdx >= 0) {
                if (currentMatchIdx == currentRightMatches.length) {
                  if (smjScanner.findNextInnerJoinRows()) {
                    currentRightMatches = smjScanner.getBufferedMatches
                    currentLeftRow = smjScanner.getStreamedRow
                    currentMatchIdx = 0
                  } else {
                    currentRightMatches = null
                    currentLeftRow = null
                    currentMatchIdx = -1
                    return false
                  }
                }
                joinRow(currentLeftRow, currentRightMatches(currentMatchIdx))
                currentMatchIdx += 1
                if (boundCondition(joinRow)) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow)
          }.toScala

        case LeftOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createLeftKeyGenerator(),
            bufferedKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(leftIter),
            bufferedIter = RowIterator.fromScala(rightIter)
          )
          val rightNullRow = new GenericInternalRow(right.output.length)
          new LeftOuterIterator(
            smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows).toScala

        case RightOuter =>
          val smjScanner = new SortMergeJoinScanner(
            streamedKeyGenerator = createRightKeyGenerator(),
            bufferedKeyGenerator = createLeftKeyGenerator(),
            keyOrdering,
            streamedIter = RowIterator.fromScala(rightIter),
            bufferedIter = RowIterator.fromScala(leftIter)
          )
          val leftNullRow = new GenericInternalRow(left.output.length)
          new RightOuterIterator(
            smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows).toScala

        case FullOuter =>
          val leftNullRow = new GenericInternalRow(left.output.length)
          val rightNullRow = new GenericInternalRow(right.output.length)
          val smjScanner = new SortMergeFullOuterJoinScanner(
            leftKeyGenerator = createLeftKeyGenerator(),
            rightKeyGenerator = createRightKeyGenerator(),
            keyOrdering,
            leftIter = RowIterator.fromScala(leftIter),
            rightIter = RowIterator.fromScala(rightIter),
            boundCondition,
            leftNullRow,
            rightNullRow)

          new FullOuterIterator(
            smjScanner,
            resultProj,
            numOutputRows).toScala

        case LeftSemi =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter)
            )
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextInnerJoinRows()) {
                val currentRightMatches = smjScanner.getBufferedMatches
                currentLeftRow = smjScanner.getStreamedRow
                var i = 0
                while (i < currentRightMatches.length) {
                  joinRow(currentLeftRow, currentRightMatches(i))
                  if (boundCondition(joinRow)) {
                    numOutputRows += 1
                    return true
                  }
                  i += 1
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case LeftAnti =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter)
            )
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                if (currentRightMatches == null) {
                  return true
                }
                var i = 0
                var found = false
                while (!found && i < currentRightMatches.length) {
                  joinRow(currentLeftRow, currentRightMatches(i))
                  if (boundCondition(joinRow)) {
                    found = true
                  }
                  i += 1
                }
                if (!found) {
                  numOutputRows += 1
                  return true
                }
              }
              false
            }

            override def getRow: InternalRow = currentLeftRow
          }.toScala

        case j: ExistenceJoin =>
          new RowIterator {
            private[this] var currentLeftRow: InternalRow = _
            private[this] val result: MutableRow = new GenericMutableRow(Array[Any](null))
            private[this] val smjScanner = new SortMergeJoinScanner(
              createLeftKeyGenerator(),
              createRightKeyGenerator(),
              keyOrdering,
              RowIterator.fromScala(leftIter),
              RowIterator.fromScala(rightIter)
            )
            private[this] val joinRow = new JoinedRow

            override def advanceNext(): Boolean = {
              while (smjScanner.findNextOuterJoinRows()) {
                currentLeftRow = smjScanner.getStreamedRow
                val currentRightMatches = smjScanner.getBufferedMatches
                var found = false
                if (currentRightMatches != null) {
                  var i = 0
                  while (!found && i < currentRightMatches.length) {
                    joinRow(currentLeftRow, currentRightMatches(i))
                    if (boundCondition(joinRow)) {
                      found = true
                    }
                    i += 1
                  }
                }
                result.setBoolean(0, found)
                numOutputRows += 1
                return true
              }
              false
            }

            override def getRow: InternalRow = resultProj(joinRow(currentLeftRow, result))
          }.toScala

        case x =>
          throw new IllegalArgumentException(
            s"SortMergeJoin should not take $x as the JoinType")
      }

    }
  }

  override def supportCodegen: Boolean = {
    joinType == Inner
  }

  override def inputRDDs(): Seq[RDD[InternalRow]] = {
    left.execute() :: right.execute() :: Nil
  }

  private def createJoinKey(
      ctx: CodegenContext,
      row: String,
      keys: Seq[Expression],
      input: Seq[Attribute]): Seq[ExprCode] = {
    ctx.INPUT_ROW = row
    keys.map(BindReferences.bindReference(_, input).genCode(ctx))
  }

  private def copyKeys(ctx: CodegenContext, vars: Seq[ExprCode]): Seq[ExprCode] = {
    vars.zipWithIndex.map { case (ev, i) =>
      ctx.addBufferedState(leftKeys(i).dataType, "value", ev.value)
    }
  }

  private def genComparision(ctx: CodegenContext, a: Seq[ExprCode], b: Seq[ExprCode]): String = {
    val comparisons = a.zip(b).zipWithIndex.map { case ((l, r), i) =>
      s"""
         |if (comp == 0) {
         |  comp = ${ctx.genComp(leftKeys(i).dataType, l.value, r.value)};
         |}
       """.stripMargin.trim
    }
    s"""
       |comp = 0;
       |${comparisons.mkString("\n")}
     """.stripMargin
  }

  /**
   * Generate a function to scan both left and right to find a match, returns the term for
   * matched one row from left side and buffered rows from right side.
   */
  private def genScanner(ctx: CodegenContext): (String, String) = {
    // Create class member for next row from both sides.
    val leftRow = ctx.freshName("leftRow")
    ctx.addMutableState("InternalRow", leftRow, "")
    val rightRow = ctx.freshName("rightRow")
    ctx.addMutableState("InternalRow", rightRow, s"$rightRow = null;")

    // Create variables for join keys from both sides.
    val leftKeyVars = createJoinKey(ctx, leftRow, leftKeys, left.output)
    val leftAnyNull = leftKeyVars.map(_.isNull).mkString(" || ")
    val rightKeyTmpVars = createJoinKey(ctx, rightRow, rightKeys, right.output)
    val rightAnyNull = rightKeyTmpVars.map(_.isNull).mkString(" || ")
    // Copy the right key as class members so they could be used in next function call.
    val rightKeyVars = copyKeys(ctx, rightKeyTmpVars)

    // A list to hold all matched rows from right side.
    val matches = ctx.freshName("matches")
    val clsName = classOf[java.util.ArrayList[InternalRow]].getName
    ctx.addMutableState(clsName, matches, s"$matches = new $clsName();")
    // Copy the left keys as class members so they could be used in next function call.
    val matchedKeyVars = copyKeys(ctx, leftKeyVars)

    ctx.addNewFunction("findNextInnerJoinRows",
      s"""
         |private boolean findNextInnerJoinRows(
         |    scala.collection.Iterator leftIter,
         |    scala.collection.Iterator rightIter) {
         |  $leftRow = null;
         |  int comp = 0;
         |  while ($leftRow == null) {
         |    if (!leftIter.hasNext()) return false;
         |    $leftRow = (InternalRow) leftIter.next();
         |    ${leftKeyVars.map(_.code).mkString("\n")}
         |    if ($leftAnyNull) {
         |      $leftRow = null;
         |      continue;
         |    }
         |    if (!$matches.isEmpty()) {
         |      ${genComparision(ctx, leftKeyVars, matchedKeyVars)}
         |      if (comp == 0) {
         |        return true;
         |      }
         |      $matches.clear();
         |    }
         |
         |    do {
         |      if ($rightRow == null) {
         |        if (!rightIter.hasNext()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return !$matches.isEmpty();
         |        }
         |        $rightRow = (InternalRow) rightIter.next();
         |        ${rightKeyTmpVars.map(_.code).mkString("\n")}
         |        if ($rightAnyNull) {
         |          $rightRow = null;
         |          continue;
         |        }
         |        ${rightKeyVars.map(_.code).mkString("\n")}
         |      }
         |      ${genComparision(ctx, leftKeyVars, rightKeyVars)}
         |      if (comp > 0) {
         |        $rightRow = null;
         |      } else if (comp < 0) {
         |        if (!$matches.isEmpty()) {
         |          ${matchedKeyVars.map(_.code).mkString("\n")}
         |          return true;
         |        }
         |        $leftRow = null;
         |      } else {
         |        $matches.add($rightRow.copy());
         |        $rightRow = null;;
         |      }
         |    } while ($leftRow != null);
         |  }
         |  return false; // unreachable
         |}
       """.stripMargin)

    (leftRow, matches)
  }

  /**
   * Creates variables for left part of result row.
   *
   * In order to defer the access after condition and also only access once in the loop,
   * the variables should be declared separately from accessing the columns, we can't use the
   * codegen of BoundReference here.
   */
  private def createLeftVars(ctx: CodegenContext, leftRow: String): Seq[ExprCode] = {
    ctx.INPUT_ROW = leftRow
    left.output.zipWithIndex.map { case (a, i) =>
      val value = ctx.freshName("value")
      val valueCode = ctx.getValue(leftRow, a.dataType, i.toString)
      // declare it as class member, so we can access the column before or in the loop.
      ctx.addMutableState(ctx.javaType(a.dataType), value, "")
      if (a.nullable) {
        val isNull = ctx.freshName("isNull")
        ctx.addMutableState("boolean", isNull, "")
        val code =
          s"""
             |$isNull = $leftRow.isNullAt($i);
             |$value = $isNull ? ${ctx.defaultValue(a.dataType)} : ($valueCode);
           """.stripMargin
        ExprCode(code, isNull, value)
      } else {
        ExprCode(s"$value = $valueCode;", "false", value)
      }
    }
  }

  /**
   * Creates the variables for right part of result row, using BoundReference, since the right
   * part are accessed inside the loop.
   */
  private def createRightVar(ctx: CodegenContext, rightRow: String): Seq[ExprCode] = {
    ctx.INPUT_ROW = rightRow
    right.output.zipWithIndex.map { case (a, i) =>
      BoundReference(i, a.dataType, a.nullable).genCode(ctx)
    }
  }

  /**
   * Splits variables based on whether it's used by condition or not, returns the code to create
   * these variables before the condition and after the condition.
   *
   * Only a few columns are used by condition, then we can skip the accessing of those columns
   * that are not used by condition also filtered out by condition.
   */
  private def splitVarsByCondition(
      attributes: Seq[Attribute],
      variables: Seq[ExprCode]): (String, String) = {
    if (condition.isDefined) {
      val condRefs = condition.get.references
      val (used, notUsed) = attributes.zip(variables).partition{ case (a, ev) =>
        condRefs.contains(a)
      }
      val beforeCond = evaluateVariables(used.map(_._2))
      val afterCond = evaluateVariables(notUsed.map(_._2))
      (beforeCond, afterCond)
    } else {
      (evaluateVariables(variables), "")
    }
  }

  override def doProduce(ctx: CodegenContext): String = {
    ctx.copyResult = true
    val leftInput = ctx.freshName("leftInput")
    ctx.addMutableState("scala.collection.Iterator", leftInput, s"$leftInput = inputs[0];")
    val rightInput = ctx.freshName("rightInput")
    ctx.addMutableState("scala.collection.Iterator", rightInput, s"$rightInput = inputs[1];")

    val (leftRow, matches) = genScanner(ctx)

    // Create variables for row from both sides.
    val leftVars = createLeftVars(ctx, leftRow)
    val rightRow = ctx.freshName("rightRow")
    val rightVars = createRightVar(ctx, rightRow)

    val size = ctx.freshName("size")
    val i = ctx.freshName("i")
    val numOutput = metricTerm(ctx, "numOutputRows")
    val (beforeLoop, condCheck) = if (condition.isDefined) {
      // Split the code of creating variables based on whether it's used by condition or not.
      val loaded = ctx.freshName("loaded")
      val (leftBefore, leftAfter) = splitVarsByCondition(left.output, leftVars)
      val (rightBefore, rightAfter) = splitVarsByCondition(right.output, rightVars)
      // Generate code for condition
      ctx.currentVars = leftVars ++ rightVars
      val cond = BindReferences.bindReference(condition.get, output).genCode(ctx)
      // evaluate the columns those used by condition before loop
      val before = s"""
           |boolean $loaded = false;
           |$leftBefore
         """.stripMargin

      val checking = s"""
         |$rightBefore
         |${cond.code}
         |if (${cond.isNull} || !${cond.value}) continue;
         |if (!$loaded) {
         |  $loaded = true;
         |  $leftAfter
         |}
         |$rightAfter
     """.stripMargin
      (before, checking)
    } else {
      (evaluateVariables(leftVars), "")
    }

    s"""
       |while (findNextInnerJoinRows($leftInput, $rightInput)) {
       |  int $size = $matches.size();
       |  ${beforeLoop.trim}
       |  for (int $i = 0; $i < $size; $i ++) {
       |    InternalRow $rightRow = (InternalRow) $matches.get($i);
       |    ${condCheck.trim}
       |    $numOutput.add(1);
       |    ${consume(ctx, leftVars ++ rightVars)}
       |  }
       |  if (shouldStop()) return;
       |}
     """.stripMargin
  }
}

/**
 * Helper class that is used to implement [[SortMergeJoinExec]].
 *
 * To perform an inner (outer) join, users of this class call [[findNextInnerJoinRows()]]
 * ([[findNextOuterJoinRows()]]), which returns `true` if a result has been produced and `false`
 * otherwise. If a result has been produced, then the caller may call [[getStreamedRow]] to return
 * the matching row from the streamed input and may call [[getBufferedMatches]] to return the
 * sequence of matching rows from the buffered input (in the case of an outer join, this will return
 * an empty sequence if there are no matches from the buffered input). For efficiency, both of these
 * methods return mutable objects which are re-used across calls to the `findNext*JoinRows()`
 * methods.
 *
 * @param streamedKeyGenerator a projection that produces join keys from the streamed input.
 * @param bufferedKeyGenerator a projection that produces join keys from the buffered input.
 * @param keyOrdering an ordering which can be used to compare join keys.
 * @param streamedIter an input whose rows will be streamed.
 * @param bufferedIter an input whose rows will be buffered to construct sequences of rows that
 *                     have the same join key.
 */
private[joins] class SortMergeJoinScanner(
    streamedKeyGenerator: Projection,
    bufferedKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    streamedIter: RowIterator,
    bufferedIter: RowIterator) {
  private[this] var streamedRow: InternalRow = _
  private[this] var streamedRowKey: InternalRow = _
  private[this] var bufferedRow: InternalRow = _
  // Note: this is guaranteed to never have any null columns:
  private[this] var bufferedRowKey: InternalRow = _
  /**
   * The join key for the rows buffered in `bufferedMatches`, or null if `bufferedMatches` is empty
   */
  private[this] var matchJoinKey: InternalRow = _
  /** Buffered rows from the buffered side of the join. This is empty if there are no matches. */
  private[this] val bufferedMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]

  // Initialization (note: do _not_ want to advance streamed here).
  advancedBufferedToRowWithNullFreeJoinKey()

  // --- Public methods ---------------------------------------------------------------------------

  def getStreamedRow: InternalRow = streamedRow

  def getBufferedMatches: ArrayBuffer[InternalRow] = bufferedMatches

  /**
   * Advances both input iterators, stopping when we have found rows with matching join keys.
   * @return true if matching rows have been found and false otherwise. If this returns true, then
   *         [[getStreamedRow]] and [[getBufferedMatches]] can be called to construct the join
   *         results.
   */
  final def findNextInnerJoinRows(): Boolean = {
    while (advancedStreamed() && streamedRowKey.anyNull) {
      // Advance the streamed side of the join until we find the next row whose join key contains
      // no nulls or we hit the end of the streamed iterator.
    }
    if (streamedRow == null) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
      // The new streamed row has the same join key as the previous row, so return the same matches.
      true
    } else if (bufferedRow == null) {
      // The streamed row's join key does not match the current batch of buffered rows and there are
      // no more rows to read from the buffered iterator, so there can be no more matches.
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else {
      // Advance both the streamed and buffered iterators to find the next pair of matching rows.
      var comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
      do {
        if (streamedRowKey.anyNull) {
          advancedStreamed()
        } else {
          assert(!bufferedRowKey.anyNull)
          comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
          if (comp > 0) advancedBufferedToRowWithNullFreeJoinKey()
          else if (comp < 0) advancedStreamed()
        }
      } while (streamedRow != null && bufferedRow != null && comp != 0)
      if (streamedRow == null || bufferedRow == null) {
        // We have either hit the end of one of the iterators, so there can be no more matches.
        matchJoinKey = null
        bufferedMatches.clear()
        false
      } else {
        // The streamed row's join key matches the current buffered row's join, so walk through the
        // buffered iterator to buffer the rest of the matching rows.
        assert(comp == 0)
        bufferMatchingRows()
        true
      }
    }
  }

  /**
   * Advances the streamed input iterator and buffers all rows from the buffered input that
   * have matching keys.
   * @return true if the streamed iterator returned a row, false otherwise. If this returns true,
   *         then [[getStreamedRow]] and [[getBufferedMatches]] can be called to produce the outer
   *         join results.
   */
  final def findNextOuterJoinRows(): Boolean = {
    if (!advancedStreamed()) {
      // We have consumed the entire streamed iterator, so there can be no more matches.
      matchJoinKey = null
      bufferedMatches.clear()
      false
    } else {
      if (matchJoinKey != null && keyOrdering.compare(streamedRowKey, matchJoinKey) == 0) {
        // Matches the current group, so do nothing.
      } else {
        // The streamed row does not match the current group.
        matchJoinKey = null
        bufferedMatches.clear()
        if (bufferedRow != null && !streamedRowKey.anyNull) {
          // The buffered iterator could still contain matching rows, so we'll need to walk through
          // it until we either find matches or pass where they would be found.
          var comp = 1
          do {
            comp = keyOrdering.compare(streamedRowKey, bufferedRowKey)
          } while (comp > 0 && advancedBufferedToRowWithNullFreeJoinKey())
          if (comp == 0) {
            // We have found matches, so buffer them (this updates matchJoinKey)
            bufferMatchingRows()
          } else {
            // We have overshot the position where the row would be found, hence no matches.
          }
        }
      }
      // If there is a streamed input then we always return true
      true
    }
  }

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the streamed iterator and compute the new row's join key.
   * @return true if the streamed iterator returned a row and false otherwise.
   */
  private def advancedStreamed(): Boolean = {
    if (streamedIter.advanceNext()) {
      streamedRow = streamedIter.getRow
      streamedRowKey = streamedKeyGenerator(streamedRow)
      true
    } else {
      streamedRow = null
      streamedRowKey = null
      false
    }
  }

  /**
   * Advance the buffered iterator until we find a row with join key that does not contain nulls.
   * @return true if the buffered iterator returned a row and false otherwise.
   */
  private def advancedBufferedToRowWithNullFreeJoinKey(): Boolean = {
    var foundRow: Boolean = false
    while (!foundRow && bufferedIter.advanceNext()) {
      bufferedRow = bufferedIter.getRow
      bufferedRowKey = bufferedKeyGenerator(bufferedRow)
      foundRow = !bufferedRowKey.anyNull
    }
    if (!foundRow) {
      bufferedRow = null
      bufferedRowKey = null
      false
    } else {
      true
    }
  }

  /**
   * Called when the streamed and buffered join keys match in order to buffer the matching rows.
   */
  private def bufferMatchingRows(): Unit = {
    assert(streamedRowKey != null)
    assert(!streamedRowKey.anyNull)
    assert(bufferedRowKey != null)
    assert(!bufferedRowKey.anyNull)
    assert(keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
    // This join key may have been produced by a mutable projection, so we need to make a copy:
    matchJoinKey = streamedRowKey.copy()
    bufferedMatches.clear()
    do {
      bufferedMatches += bufferedRow.copy() // need to copy mutable rows before buffering them
      advancedBufferedToRowWithNullFreeJoinKey()
    } while (bufferedRow != null && keyOrdering.compare(streamedRowKey, bufferedRowKey) == 0)
  }
}

/**
 * An iterator for outputting rows in left outer join.
 */
private class LeftOuterIterator(
    smjScanner: SortMergeJoinScanner,
    rightNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: SQLMetric)
  extends OneSideOuterIterator(
    smjScanner, rightNullRow, boundCondition, resultProj, numOutputRows) {

  protected override def setStreamSideOutput(row: InternalRow): Unit = joinedRow.withLeft(row)
  protected override def setBufferedSideOutput(row: InternalRow): Unit = joinedRow.withRight(row)
}

/**
 * An iterator for outputting rows in right outer join.
 */
private class RightOuterIterator(
    smjScanner: SortMergeJoinScanner,
    leftNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: SQLMetric)
  extends OneSideOuterIterator(smjScanner, leftNullRow, boundCondition, resultProj, numOutputRows) {

  protected override def setStreamSideOutput(row: InternalRow): Unit = joinedRow.withRight(row)
  protected override def setBufferedSideOutput(row: InternalRow): Unit = joinedRow.withLeft(row)
}

/**
 * An abstract iterator for sharing code between [[LeftOuterIterator]] and [[RightOuterIterator]].
 *
 * Each [[OneSideOuterIterator]] has a streamed side and a buffered side. Each row on the
 * streamed side will output 0 or many rows, one for each matching row on the buffered side.
 * If there are no matches, then the buffered side of the joined output will be a null row.
 *
 * In left outer join, the left is the streamed side and the right is the buffered side.
 * In right outer join, the right is the streamed side and the left is the buffered side.
 *
 * @param smjScanner a scanner that streams rows and buffers any matching rows
 * @param bufferedSideNullRow the default row to return when a streamed row has no matches
 * @param boundCondition an additional filter condition for buffered rows
 * @param resultProj how the output should be projected
 * @param numOutputRows an accumulator metric for the number of rows output
 */
private abstract class OneSideOuterIterator(
    smjScanner: SortMergeJoinScanner,
    bufferedSideNullRow: InternalRow,
    boundCondition: InternalRow => Boolean,
    resultProj: InternalRow => InternalRow,
    numOutputRows: SQLMetric) extends RowIterator {

  // A row to store the joined result, reused many times
  protected[this] val joinedRow: JoinedRow = new JoinedRow()

  // Index of the buffered rows, reset to 0 whenever we advance to a new streamed row
  private[this] var bufferIndex: Int = 0

  // This iterator is initialized lazily so there should be no matches initially
  assert(smjScanner.getBufferedMatches.length == 0)

  // Set output methods to be overridden by subclasses
  protected def setStreamSideOutput(row: InternalRow): Unit
  protected def setBufferedSideOutput(row: InternalRow): Unit

  /**
   * Advance to the next row on the stream side and populate the buffer with matches.
   * @return whether there are more rows in the stream to consume.
   */
  private def advanceStream(): Boolean = {
    bufferIndex = 0
    if (smjScanner.findNextOuterJoinRows()) {
      setStreamSideOutput(smjScanner.getStreamedRow)
      if (smjScanner.getBufferedMatches.isEmpty) {
        // There are no matching rows in the buffer, so return the null row
        setBufferedSideOutput(bufferedSideNullRow)
      } else {
        // Find the next row in the buffer that satisfied the bound condition
        if (!advanceBufferUntilBoundConditionSatisfied()) {
          setBufferedSideOutput(bufferedSideNullRow)
        }
      }
      true
    } else {
      // Stream has been exhausted
      false
    }
  }

  /**
   * Advance to the next row in the buffer that satisfies the bound condition.
   * @return whether there is such a row in the current buffer.
   */
  private def advanceBufferUntilBoundConditionSatisfied(): Boolean = {
    var foundMatch: Boolean = false
    while (!foundMatch && bufferIndex < smjScanner.getBufferedMatches.length) {
      setBufferedSideOutput(smjScanner.getBufferedMatches(bufferIndex))
      foundMatch = boundCondition(joinedRow)
      bufferIndex += 1
    }
    foundMatch
  }

  override def advanceNext(): Boolean = {
    val r = advanceBufferUntilBoundConditionSatisfied() || advanceStream()
    if (r) numOutputRows += 1
    r
  }

  override def getRow: InternalRow = resultProj(joinedRow)
}

private class SortMergeFullOuterJoinScanner(
    leftKeyGenerator: Projection,
    rightKeyGenerator: Projection,
    keyOrdering: Ordering[InternalRow],
    leftIter: RowIterator,
    rightIter: RowIterator,
    boundCondition: InternalRow => Boolean,
    leftNullRow: InternalRow,
    rightNullRow: InternalRow)  {
  private[this] val joinedRow: JoinedRow = new JoinedRow()
  private[this] var leftRow: InternalRow = _
  private[this] var leftRowKey: InternalRow = _
  private[this] var rightRow: InternalRow = _
  private[this] var rightRowKey: InternalRow = _

  private[this] var leftIndex: Int = 0
  private[this] var rightIndex: Int = 0
  private[this] val leftMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]
  private[this] val rightMatches: ArrayBuffer[InternalRow] = new ArrayBuffer[InternalRow]
  private[this] var leftMatched: BitSet = new BitSet(1)
  private[this] var rightMatched: BitSet = new BitSet(1)

  advancedLeft()
  advancedRight()

  // --- Private methods --------------------------------------------------------------------------

  /**
   * Advance the left iterator and compute the new row's join key.
   * @return true if the left iterator returned a row and false otherwise.
   */
  private def advancedLeft(): Boolean = {
    if (leftIter.advanceNext()) {
      leftRow = leftIter.getRow
      leftRowKey = leftKeyGenerator(leftRow)
      true
    } else {
      leftRow = null
      leftRowKey = null
      false
    }
  }

  /**
   * Advance the right iterator and compute the new row's join key.
   * @return true if the right iterator returned a row and false otherwise.
   */
  private def advancedRight(): Boolean = {
    if (rightIter.advanceNext()) {
      rightRow = rightIter.getRow
      rightRowKey = rightKeyGenerator(rightRow)
      true
    } else {
      rightRow = null
      rightRowKey = null
      false
    }
  }

  /**
   * Populate the left and right buffers with rows matching the provided key.
   * This consumes rows from both iterators until their keys are different from the matching key.
   */
  private def findMatchingRows(matchingKey: InternalRow): Unit = {
    leftMatches.clear()
    rightMatches.clear()
    leftIndex = 0
    rightIndex = 0

    while (leftRowKey != null && keyOrdering.compare(leftRowKey, matchingKey) == 0) {
      leftMatches += leftRow.copy()
      advancedLeft()
    }
    while (rightRowKey != null && keyOrdering.compare(rightRowKey, matchingKey) == 0) {
      rightMatches += rightRow.copy()
      advancedRight()
    }

    if (leftMatches.size <= leftMatched.capacity) {
      leftMatched.clear()
    } else {
      leftMatched = new BitSet(leftMatches.size)
    }
    if (rightMatches.size <= rightMatched.capacity) {
      rightMatched.clear()
    } else {
      rightMatched = new BitSet(rightMatches.size)
    }
  }

  /**
   * Scan the left and right buffers for the next valid match.
   *
   * Note: this method mutates `joinedRow` to point to the latest matching rows in the buffers.
   * If a left row has no valid matches on the right, or a right row has no valid matches on the
   * left, then the row is joined with the null row and the result is considered a valid match.
   *
   * @return true if a valid match is found, false otherwise.
   */
  private def scanNextInBuffered(): Boolean = {
    while (leftIndex < leftMatches.size) {
      while (rightIndex < rightMatches.size) {
        joinedRow(leftMatches(leftIndex), rightMatches(rightIndex))
        if (boundCondition(joinedRow)) {
          leftMatched.set(leftIndex)
          rightMatched.set(rightIndex)
          rightIndex += 1
          return true
        }
        rightIndex += 1
      }
      rightIndex = 0
      if (!leftMatched.get(leftIndex)) {
        // the left row has never matched any right row, join it with null row
        joinedRow(leftMatches(leftIndex), rightNullRow)
        leftIndex += 1
        return true
      }
      leftIndex += 1
    }

    while (rightIndex < rightMatches.size) {
      if (!rightMatched.get(rightIndex)) {
        // the right row has never matched any left row, join it with null row
        joinedRow(leftNullRow, rightMatches(rightIndex))
        rightIndex += 1
        return true
      }
      rightIndex += 1
    }

    // There are no more valid matches in the left and right buffers
    false
  }

  // --- Public methods --------------------------------------------------------------------------

  def getJoinedRow(): JoinedRow = joinedRow

  def advanceNext(): Boolean = {
    // If we already buffered some matching rows, use them directly
    if (leftIndex <= leftMatches.size || rightIndex <= rightMatches.size) {
      if (scanNextInBuffered()) {
        return true
      }
    }

    if (leftRow != null && (leftRowKey.anyNull || rightRow == null)) {
      joinedRow(leftRow.copy(), rightNullRow)
      advancedLeft()
      true
    } else if (rightRow != null && (rightRowKey.anyNull || leftRow == null)) {
      joinedRow(leftNullRow, rightRow.copy())
      advancedRight()
      true
    } else if (leftRow != null && rightRow != null) {
      // Both rows are present and neither have null values,
      // so we populate the buffers with rows matching the next key
      val comp = keyOrdering.compare(leftRowKey, rightRowKey)
      if (comp <= 0) {
        findMatchingRows(leftRowKey.copy())
      } else {
        findMatchingRows(rightRowKey.copy())
      }
      scanNextInBuffered()
      true
    } else {
      // Both iterators have been consumed
      false
    }
  }
}

private class FullOuterIterator(
    smjScanner: SortMergeFullOuterJoinScanner,
    resultProj: InternalRow => InternalRow,
    numRows: SQLMetric) extends RowIterator {
  private[this] val joinedRow: JoinedRow = smjScanner.getJoinedRow()

  override def advanceNext(): Boolean = {
    val r = smjScanner.advanceNext()
    if (r) numRows += 1
    r
  }

  override def getRow: InternalRow = resultProj(joinedRow)
}
